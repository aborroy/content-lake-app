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
 * corpus that has since changed.</p>
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
     * What the job has done so far.
     *
     * @param state                 lifecycle position
     * @param targetType            the embedding type being written
     * @param scanned               documents visited
     * @param backfilled            documents given a new embedding child
     * @param skippedAlreadyPresent documents that already carried the target type
     * @param skippedNoText         documents with no extracted-text mirror, which need a full re-sync
     *                              rather than a backfill
     * @param failed                documents whose re-embedding threw
     * @param error                 why the scan stopped, when it did
     */
    public record Progress(State state,
                           String targetType,
                           int scanned,
                           int backfilled,
                           int skippedAlreadyPresent,
                           int skippedNoText,
                           int failed,
                           String error) {
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
        scanned.set(0);
        backfilled.set(0);
        skippedAlreadyPresent.set(0);
        skippedNoText.set(0);
        failed.set(0);
        error = null;
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
     * Continues a paused run, keeping its counters.
     *
     * <p>The scan restarts from the beginning: documents already carrying the target type are skipped,
     * which is cheaper than persisting a cursor and correct even if the corpus changed while paused.</p>
     *
     * @throws IllegalStateException when there is no paused run to continue
     */
    public synchronized Progress resume() {
        if (state != State.PAUSED) {
            throw new IllegalStateException("No paused backfill to resume (state is " + state + ")");
        }
        return launch(docsPerMinute);
    }

    private Progress launch(int ratePerMinute) {
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
                skippedAlreadyPresent.get(), skippedNoText.get(), failed.get(), error);
    }

    // ------------------------------------------------------------------
    // The run
    // ------------------------------------------------------------------

    private void run() {
        String target = targetType();
        long delayMillis = docsPerMinute > 0 ? 60_000L / docsPerMinute : 0L;
        log.info("Embedding backfill started for type '{}' at {} docs/minute", target,
                docsPerMinute > 0 ? docsPerMinute : "unlimited");

        // Once, not per document: the per-call wait is up to 30 seconds and would dominate the run.
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
                    processDocument(document, target);
                    if (delayMillis > 0) {
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

    private void processDocument(HxprDocument document, String targetType) {
        scanned.incrementAndGet();
        String documentId = document.getSysId();
        if (documentId == null || document.getCinId() == null) {
            // Not an ingested source document; SysEmbeddings children carry no cin_id either.
            return;
        }

        try {
            boolean alreadyPresent = hxprService.listEmbeddingChildren(documentId, false).stream()
                    .anyMatch(child -> targetType.equals(child.embeddingType()));
            if (alreadyPresent) {
                skippedAlreadyPresent.incrementAndGet();
                return;
            }

            String text = mirrorText(document);
            if (text == null) {
                skippedNoText.incrementAndGet();
                log.debug("Backfill skipped document {}: no extracted-text mirror, needs a full re-sync",
                        documentId);
                return;
            }

            List<Chunk> chunks = chunkingService.chunk(text, document.getCinId(), "text/plain");
            if (chunks.isEmpty()) {
                skippedNoText.incrementAndGet();
                return;
            }

            List<EmbeddingService.ChunkWithEmbedding> embedded =
                    embeddingService.embedChunks(chunks, documentContext(document));
            hxprService.updateEmbeddings(documentId, toHxprEmbeddings(embedded, targetType));
            backfilled.incrementAndGet();
            log.debug("Backfilled document {} with {} embeddings under type '{}'",
                    documentId, embedded.size(), targetType);
        } catch (Exception e) {
            failed.incrementAndGet();
            log.warn("Backfill failed for document {}: {}", documentId, e.getMessage());
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
