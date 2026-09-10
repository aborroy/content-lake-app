package org.hyland.contentlake.service;

import lombok.extern.slf4j.Slf4j;
import org.hyland.contentlake.client.HxprService;
import org.hyland.contentlake.model.Chunk;
import org.hyland.contentlake.model.ContentLakeIngestProperties;
import org.hyland.contentlake.model.HxprDocument;
import org.hyland.contentlake.model.HxprEmbedding;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Re-embeds an already-indexed corpus into the currently configured embedding type, at a controlled
 * rate, while the previous type keeps answering queries (#121).
 *
 * <h3>What this is for</h3>
 * <p>Changing the embedding model invalidates every vector. Without this, an upgrade means a full
 * re-ingest with search degraded throughout, which is what makes model upgrades indefinitely
 * deferrable. Here the new type is written document by document and the old one is left in place, so
 * multi-type retrieval serves both and the corpus is never in a state where a document has no usable
 * vectors.</p>
 *
 * <h3>Where the chunk text comes from</h3>
 * <p>The extracted-text mirror stored on each document, not the source system. That keeps the job
 * inside the index, with no source credentials, no re-extraction and no dependency on the source still
 * holding the document. The cost is that the mirror is markup-stripped and capped, so re-chunking it
 * does not reproduce the original chunk boundaries exactly, and a document whose extraction exceeded
 * the mirror cap is backfilled without its tail.</p>
 *
 * <p>For that reason the job deliberately does <strong>not</strong> write the content fingerprint. The
 * fingerprint stays as the retired model left it, so it no longer matches, and the next content sync
 * reprocesses that document properly from its source. The backfill is a bridge that keeps search
 * working through a migration, not a replacement for a sync.</p>
 *
 * <h3>Resume</h3>
 * <p>Resuming re-scans from the start and skips any document that already carries a child for the
 * target type. That makes the job idempotent, so an interrupted run, a restart and a deliberate pause
 * all recover the same way, with no cursor to persist and no risk of a stored offset pointing into a
 * corpus that has since changed. Because the pass starts over, so do the counters -- see
 * {@link Progress}.</p>
 *
 * <h3>Rate</h3>
 * <p>{@code docsPerMinute} bounds the documents this job re-embeds per minute, so an operator can leave
 * the ingesters enough embedding throughput to keep up. Two things are deliberately outside it: a
 * document skipped because it already carries the target type, which consumes no capacity, and the
 * index wait, which is paid once per pass rather than once per document.</p>
 */
@Slf4j
public class EmbeddingBackfillService {

    private static final String P_EXTRACTED_TEXT = ContentLakeIngestProperties.CONTENT_LAKE_EXTRACTED_TEXT;
    private static final String P_SOURCE_NAME = ContentLakeIngestProperties.SOURCE_NAME;
    private static final String P_SOURCE_PATH = ContentLakeIngestProperties.SOURCE_PATH;

    private static final String FILE_QUERY = "SELECT * FROM SysContent";
    private static final String FILE_CLAUSE = "sys_primaryType = 'SysFile'";

    private static final int PAGE_SIZE = 100;

    /** Bound on the scan, so a non-advancing page cannot spin forever. */
    private static final int MAX_PAGES = 10_000;

    private static final String DOCUMENT_CONTEXT_PREFIX = "Document: ";

    /** Where the job is in its lifecycle. */
    public enum State {
        /** Never started, or the previous run has been read and forgotten. */
        IDLE,
        /** Working through the corpus. */
        RUNNING,
        /** Stopped between documents at an operator's request; {@link #resume()} continues. */
        PAUSED,
        /** Every document was visited. */
        COMPLETED,
        /** The scan itself failed. Per-document failures are counted and do not end the run. */
        FAILED
    }

    /**
     * What the job has done so far, in the <em>current scan pass</em>.
     *
     * <p>A resume re-scans the corpus from the start, so the counters are reset when it does and describe
     * that pass alone. Reporting them cumulatively made {@code scanned} count the documents visited before
     * the pause twice, so a completed run over 41 documents reported 44 (#127); it also left the counters
     * unable to reconcile, since a document backfilled in an earlier pass is counted as already present in
     * this one. Per-pass, {@code scanned} equals the corpus size and
     * {@code scanned = backfilled + skippedAlreadyPresent + skippedNoText + skippedNotIngested + failed}.
     * The numbers from the pass being resumed are logged before they are cleared.</p>
     *
     * @param state                 lifecycle position
     * @param targetType            the embedding type being written
     * @param scanned               documents visited
     * @param backfilled            documents given a new embedding child
     * @param skippedAlreadyPresent documents that already carried the target type
     * @param skippedNoText         documents with no extracted-text mirror, which need a full re-sync
     *                              rather than a backfill
     * @param skippedNotIngested    rows that are not ingested source documents, so there is nothing to
     *                              re-embed
     * @param failed                documents whose re-embedding threw
     * @param error                 why the scan stopped, when it did
     */
    public record Progress(State state,
                           String targetType,
                           int scanned,
                           int backfilled,
                           int skippedAlreadyPresent,
                           int skippedNoText,
                           int skippedNotIngested,
                           int failed,
                           String error) {
    }

    /**
     * What visiting one document did.
     *
     * <p>{@code consumesThroughput} is what the rate limit applies to. Sleeping after a skip spends the
     * whole budget on documents that used no embedding capacity, so a resume over a mostly-complete
     * corpus paid {@code 60000/docsPerMinute} per no-op (#127). A failure counts as consuming: it may
     * have thrown after the embedding call, and pausing between failures is the right behaviour anyway.</p>
     */
    private enum Outcome {

        BACKFILLED(true),
        FAILED(true),
        SKIPPED_ALREADY_PRESENT(false),
        SKIPPED_NO_TEXT(false),
        SKIPPED_NOT_INGESTED(false);

        private final boolean consumesThroughput;

        Outcome(boolean consumesThroughput) {
            this.consumesThroughput = consumesThroughput;
        }

        boolean consumesThroughput() {
            return consumesThroughput;
        }
    }

    /** Pauses between documents to honour the configured rate. Injected so tests need no real delay. */
    public interface Sleeper {

        void sleep(long millis) throws InterruptedException;
    }

    private final HxprService hxprService;
    private final EmbeddingService embeddingService;
    private final SimpleChunkingServiceAccess chunkingService;
    private final Executor executor;
    private final Sleeper sleeper;

    private final AtomicBoolean running = new AtomicBoolean();
    private final AtomicBoolean pauseRequested = new AtomicBoolean();

    private final AtomicInteger scanned = new AtomicInteger();
    private final AtomicInteger backfilled = new AtomicInteger();
    private final AtomicInteger skippedAlreadyPresent = new AtomicInteger();
    private final AtomicInteger skippedNoText = new AtomicInteger();
    private final AtomicInteger skippedNotIngested = new AtomicInteger();
    private final AtomicInteger failed = new AtomicInteger();

    private volatile State state = State.IDLE;
    private volatile String error;
    private volatile int docsPerMinute;

    /**
     * Chunking, narrowed to what this service needs.
     *
     * <p>An interface rather than the concrete service so a test can supply chunking without standing
     * up noise reduction and a strategy, and so this class states exactly what it uses.</p>
     */
    public interface SimpleChunkingServiceAccess {

        List<Chunk> chunk(String text, String nodeId, String mimeType);
    }

    public EmbeddingBackfillService(HxprService hxprService,
                                    EmbeddingService embeddingService,
                                    SimpleChunkingServiceAccess chunkingService,
                                    Executor executor,
                                    Sleeper sleeper) {
        this.hxprService = hxprService;
        this.embeddingService = embeddingService;
        this.chunkingService = chunkingService;
        this.executor = executor;
        this.sleeper = sleeper;
    }

    /** The embedding type the job writes: the configured one, which is the migration target. */
    public String targetType() {
        return hxprService.getEmbeddingType();
    }

    /**
     * Starts a run, resetting the counters.
     *
     * @param docsPerMinute documents per minute, or zero or less for no limit
     * @return the progress at the moment of starting
     * @throws IllegalStateException when a run is already in progress
     */
    public synchronized Progress start(int docsPerMinute) {
        if (state == State.RUNNING) {
            throw new IllegalStateException("A backfill is already running");
        }
        return launch(docsPerMinute);
    }

    /**
     * Asks the job to stop after the document it is on. The run does not hold a thread while paused.
     *
     * @return the progress at the moment of the request
     */
    public synchronized Progress pause() {
        if (state == State.RUNNING) {
            pauseRequested.set(true);
        }
        return status();
    }

    /**
     * Continues a paused run at the configured rate.
     *
     * <p>The scan restarts from the beginning: documents already carrying the target type are skipped,
     * which is cheaper than persisting a cursor and correct even if the corpus changed while paused. The
     * counters restart with it, because they count what this pass visited and a re-scanned document would
     * otherwise be counted twice ({@link Progress}); the paused pass's numbers are logged first.</p>
     *
     * @throws IllegalStateException when there is no paused run to continue
     */
    public synchronized Progress resume() {
        if (state != State.PAUSED) {
            throw new IllegalStateException("No paused backfill to resume (state is " + state + ")");
        }
        log.info("Resuming the embedding backfill; the paused pass ended at {}", status());
        return launch(docsPerMinute);
    }

    private Progress launch(int ratePerMinute) {
        scanned.set(0);
        backfilled.set(0);
        skippedAlreadyPresent.set(0);
        skippedNoText.set(0);
        skippedNotIngested.set(0);
        failed.set(0);
        error = null;
        this.docsPerMinute = ratePerMinute;
        pauseRequested.set(false);
        state = State.RUNNING;
        running.set(true);
        executor.execute(this::run);
        return status();
    }

    /** What the job has done so far. Safe to call at any time. */
    public Progress status() {
        return new Progress(state, targetType(), scanned.get(), backfilled.get(),
                skippedAlreadyPresent.get(), skippedNoText.get(), skippedNotIngested.get(),
                failed.get(), error);
    }

    // ------------------------------------------------------------------
    // The run
    // ------------------------------------------------------------------

    private void run() {
        String target = targetType();
        long delayMillis = docsPerMinute > 0 ? 60_000L / docsPerMinute : 0L;
        log.info("Embedding backfill started for type '{}' at {} docs/minute", target,
                docsPerMinute > 0 ? docsPerMinute : "unlimited");

        // Once, not per document: the per-call wait is up to 30 seconds and would dominate the run. Each
        // document then lists its children without waiting and hands that list to the write, which is
        // what keeps the wait out of the per-document path entirely (#127).
        hxprService.awaitIndex("embedding backfill");

        try {
            for (int page = 0; page < MAX_PAGES; page++) {
                HxprDocument.QueryResult result = hxprService.advancedQuery(
                        FILE_QUERY, List.of(FILE_CLAUSE), PAGE_SIZE, page * PAGE_SIZE);
                List<HxprDocument> documents = result == null ? null : result.getDocuments();
                if (documents == null || documents.isEmpty()) {
                    break;
                }

                for (HxprDocument document : documents) {
                    if (pauseRequested.get()) {
                        state = State.PAUSED;
                        running.set(false);
                        log.info("Embedding backfill paused after {} documents", scanned.get());
                        return;
                    }
                    Outcome outcome = processDocument(document, target);
                    // Only work that spent embedding capacity is rate-limited. Throttling skips made a
                    // resume over a mostly-done corpus pay the full delay per no-op (#127).
                    if (delayMillis > 0 && outcome.consumesThroughput()) {
                        sleeper.sleep(delayMillis);
                    }
                }

                if (documents.size() < PAGE_SIZE) {
                    break;
                }
            }

            state = State.COMPLETED;
            log.info("Embedding backfill completed for type '{}': {}", target, status());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            state = State.PAUSED;
            log.info("Embedding backfill interrupted after {} documents; resume to continue", scanned.get());
        } catch (Exception e) {
            error = e.getMessage();
            state = State.FAILED;
            log.error("Embedding backfill failed after {} documents: {}", scanned.get(), e.getMessage(), e);
        } finally {
            running.set(false);
        }
    }

    private Outcome processDocument(HxprDocument document, String targetType) {
        scanned.incrementAndGet();
        String documentId = document.getSysId();
        if (documentId == null || document.getCinId() == null) {
            // Not an ingested source document; SysEmbeddings children carry no cin_id either.
            skippedNotIngested.incrementAndGet();
            return Outcome.SKIPPED_NOT_INGESTED;
        }

        try {
            // Listed once, without waiting for the index (the job waits once before it starts), and
            // handed to the write below so it does not list them again and pay the wait per document.
            List<HxprService.EmbeddingChild> children =
                    hxprService.listEmbeddingChildren(documentId, false);
            boolean alreadyPresent = children.stream()
                    .anyMatch(child -> targetType.equals(child.embeddingType()));
            if (alreadyPresent) {
                skippedAlreadyPresent.incrementAndGet();
                return Outcome.SKIPPED_ALREADY_PRESENT;
            }

            String text = mirrorText(document);
            if (text == null) {
                skippedNoText.incrementAndGet();
                log.debug("Backfill skipped document {}: no extracted-text mirror, needs a full re-sync",
                        documentId);
                return Outcome.SKIPPED_NO_TEXT;
            }

            List<Chunk> chunks = chunkingService.chunk(text, document.getCinId(), "text/plain");
            if (chunks.isEmpty()) {
                skippedNoText.incrementAndGet();
                return Outcome.SKIPPED_NO_TEXT;
            }

            List<EmbeddingService.ChunkWithEmbedding> embedded =
                    embeddingService.embedChunks(chunks, documentContext(document));
            hxprService.updateEmbeddings(documentId, toHxprEmbeddings(embedded, targetType), children);
            backfilled.incrementAndGet();
            log.debug("Backfilled document {} with {} embeddings under type '{}'",
                    documentId, embedded.size(), targetType);
            return Outcome.BACKFILLED;
        } catch (Exception e) {
            failed.incrementAndGet();
            log.warn("Backfill failed for document {}: {}", documentId, e.getMessage());
            return Outcome.FAILED;
        }
    }

    private static List<HxprEmbedding> toHxprEmbeddings(
            List<EmbeddingService.ChunkWithEmbedding> embedded, String targetType) {
        List<HxprEmbedding> embeddings = new java.util.ArrayList<>(embedded.size());
        for (EmbeddingService.ChunkWithEmbedding cwe : embedded) {
            HxprEmbedding embedding = new HxprEmbedding();
            embedding.setText(cwe.chunk().getText());
            embedding.setVector(cwe.embedding());
            embedding.setType(targetType);
            embedding.setChunkId(cwe.chunk().getId());

            HxprEmbedding.EmbeddingLocation location = new HxprEmbedding.EmbeddingLocation();
            HxprEmbedding.EmbeddingLocation.TextLocation text =
                    new HxprEmbedding.EmbeddingLocation.TextLocation();
            text.setParagraph(cwe.chunk().getIndex());
            location.setText(text);
            embedding.setLocation(location);

            embeddings.add(embedding);
        }
        return embeddings;
    }

    /**
     * The stored mirror, with the keyword-leg document-context prefix removed when it is present.
     *
     * <p>The prefix is metadata the sync prepends for term matching, not document body, so re-chunking
     * it would put it in a chunk of its own and embed it as content.</p>
     */
    private static String mirrorText(HxprDocument document) {
        Map<String, Object> props = document.getCinIngestProperties();
        if (props == null) {
            return null;
        }
        Object stored = props.get(P_EXTRACTED_TEXT);
        if (stored == null) {
            return null;
        }
        String text = stored.toString();
        if (text.isBlank()) {
            return null;
        }
        if (text.startsWith(DOCUMENT_CONTEXT_PREFIX)) {
            int bodyStart = text.indexOf("\n\n");
            if (bodyStart > 0) {
                text = text.substring(bodyStart + 2);
            }
        }
        return text.isBlank() ? null : text;
    }

    /**
     * The document-context prefix the original ingest embedded each chunk with, rebuilt from the
     * document's own metadata so the backfilled vectors sit in the same place in the space.
     */
    private static String documentContext(HxprDocument document) {
        Map<String, Object> props = document.getCinIngestProperties();
        if (props == null) {
            return null;
        }
        StringBuilder context = new StringBuilder();
        Object name = props.get(P_SOURCE_NAME);
        if (name != null && !name.toString().isBlank()) {
            context.append(DOCUMENT_CONTEXT_PREFIX).append(name);
        }
        Object path = props.get(P_SOURCE_PATH);
        if (path != null && !path.toString().isBlank()) {
            if (!context.isEmpty()) {
                context.append(" | ");
            }
            context.append("Path: ").append(path);
        }
        return context.isEmpty() ? null : context.toString();
    }
}
