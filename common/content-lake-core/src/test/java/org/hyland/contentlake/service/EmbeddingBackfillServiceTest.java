package org.hyland.contentlake.service;

import org.hyland.contentlake.client.HxprService;
import org.hyland.contentlake.model.Chunk;
import org.hyland.contentlake.model.ContentLakeIngestProperties;
import org.hyland.contentlake.model.HxprDocument;
import org.hyland.contentlake.model.HxprEmbedding;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Re-embedding an indexed corpus into a new embedding type while the previous one keeps serving
 * queries (#121).
 *
 * <p>The job runs on the calling thread here, so the assertions are deterministic. Pause is exercised
 * through the rate-limiting sleeper, which is the only place the job yields between documents, and is
 * therefore exactly where a real pause request lands.</p>
 */
@ExtendWith(MockitoExtension.class)
class EmbeddingBackfillServiceTest {

    private static final String TARGET_TYPE = "ai-mxbai-embed-large";
    private static final String PREVIOUS_TYPE = "ai-nomic-embed-text";
    private static final String P_EXTRACTED_TEXT = ContentLakeIngestProperties.CONTENT_LAKE_EXTRACTED_TEXT;

    @Mock
    private HxprService hxprService;
    @Mock
    private EmbeddingService embeddingService;

    private EmbeddingBackfillService service;

    /** Sleeps requested by the rate limiter, so the configured rate can be asserted. */
    private final List<Long> sleeps = new ArrayList<>();

    /** Runs on the caller's thread when null; otherwise called on each sleep, to exercise pause. */
    private Runnable onSleep;

    @BeforeEach
    void setUp() {
        when(hxprService.getEmbeddingType()).thenReturn(TARGET_TYPE);
        service = new EmbeddingBackfillService(
                hxprService,
                embeddingService,
                this::chunk,
                Runnable::run,
                millis -> {
                    sleeps.add(millis);
                    if (onSleep != null) {
                        onSleep.run();
                    }
                });
    }

    /** One chunk per document, which is all the job's bookkeeping needs. */
    private List<Chunk> chunk(String text, String nodeId, String mimeType) {
        return List.of(new Chunk(nodeId, text, 0, 0, text.length()));
    }

    // ------------------------------------------------------------------
    // Fixtures
    // ------------------------------------------------------------------

    private static HxprDocument document(String sysId, String nodeId, String mirror) {
        Map<String, Object> props = new LinkedHashMap<>();
        props.put(ContentLakeIngestProperties.SOURCE_NAME, nodeId + ".txt");
        props.put(ContentLakeIngestProperties.SOURCE_PATH, "/registers");
        if (mirror != null) {
            props.put(P_EXTRACTED_TEXT, mirror);
        }

        HxprDocument document = new HxprDocument();
        document.setSysId(sysId);
        document.setCinId(nodeId);
        document.setCinIngestProperties(props);
        return document;
    }

    private void corpus(HxprDocument... documents) {
        HxprDocument.QueryResult result = new HxprDocument.QueryResult();
        result.setDocuments(new ArrayList<>(List.of(documents)));
        when(hxprService.advancedQuery(any(), any(), anyInt(), anyInt())).thenReturn(result);
    }

    private void hasNoEmbeddingChildren() {
        when(hxprService.listEmbeddingChildren(anyString(), eq(false))).thenReturn(List.of());
    }

    private void embedsSuccessfully() {
        when(embeddingService.embedChunks(any(), any())).thenAnswer(invocation -> {
            List<Chunk> chunks = invocation.getArgument(0);
            List<EmbeddingService.ChunkWithEmbedding> embedded = new ArrayList<>();
            for (Chunk chunk : chunks) {
                embedded.add(new EmbeddingService.ChunkWithEmbedding(chunk, List.of(0.1d, 0.2d)));
            }
            return embedded;
        });
    }

    // ------------------------------------------------------------------
    // Backfilling
    // ------------------------------------------------------------------

    @Test
    void writesTheTargetTypeForEveryDocumentThatLacksIt() {
        corpus(document("hxpr-1", "node-1", "first body"), document("hxpr-2", "node-2", "second body"));
        hasNoEmbeddingChildren();
        embedsSuccessfully();

        EmbeddingBackfillService.Progress progress = service.start(0);

        assertThat(progress.state()).isEqualTo(EmbeddingBackfillService.State.COMPLETED);
        assertThat(progress.scanned()).isEqualTo(2);
        assertThat(progress.backfilled()).isEqualTo(2);
        verify(hxprService).updateEmbeddings(eq("hxpr-1"), any(), any());
        verify(hxprService).updateEmbeddings(eq("hxpr-2"), any(), any());
    }

    /** The rows carry the target type, which is what an embeddings query matches on. */
    @Test
    void theWrittenEmbeddingsCarryTheTargetType() {
        corpus(document("hxpr-1", "node-1", "first body"));
        hasNoEmbeddingChildren();
        embedsSuccessfully();

        service.start(0);

        ArgumentCaptor<List<HxprEmbedding>> captor = ArgumentCaptor.captor();
        verify(hxprService).updateEmbeddings(eq("hxpr-1"), captor.capture(), any());
        assertThat(captor.getValue()).allSatisfy(embedding ->
                assertThat(embedding.getType()).isEqualTo(TARGET_TYPE));
    }

    /**
     * The chunks are embedded with the same document-context prefix the original ingest used, rebuilt
     * from the document's own metadata, so the backfilled vectors land in the same region of the space.
     */
    @Test
    void chunksAreEmbeddedWithTheDocumentContextPrefix() {
        corpus(document("hxpr-1", "node-1", "first body"));
        hasNoEmbeddingChildren();
        embedsSuccessfully();

        service.start(0);

        ArgumentCaptor<String> context = ArgumentCaptor.forClass(String.class);
        verify(embeddingService).embedChunks(any(), context.capture());
        assertThat(context.getValue()).isEqualTo("Document: node-1.txt | Path: /registers");
    }

    /**
     * The prefix is stripped from the mirror before chunking. It is metadata the sync prepends for term
     * matching, so re-chunking it would embed it as document body.
     */
    @Test
    void theStoredContextPrefixIsNotChunkedAsBody() {
        corpus(document("hxpr-1", "node-1",
                "Document: node-1.txt | Path: /registers\n\nthe actual body"));
        hasNoEmbeddingChildren();
        embedsSuccessfully();

        service.start(0);

        ArgumentCaptor<List<Chunk>> chunks = ArgumentCaptor.captor();
        verify(embeddingService).embedChunks(chunks.capture(), any());
        assertThat(chunks.getValue().get(0).getText()).isEqualTo("the actual body");
    }

    /**
     * Only the target type's children are replaced. The previous type has to survive, because it is
     * what answers queries until the whole corpus has been moved across.
     */
    @Test
    void aDocumentAlreadyCarryingTheTargetTypeIsSkipped() {
        corpus(document("hxpr-1", "node-1", "first body"));
        when(hxprService.listEmbeddingChildren("hxpr-1", false)).thenReturn(List.of(
                new HxprService.EmbeddingChild("child-1", "_e_" + TARGET_TYPE, TARGET_TYPE)));

        EmbeddingBackfillService.Progress progress = service.start(0);

        assertThat(progress.skippedAlreadyPresent()).isEqualTo(1);
        assertThat(progress.backfilled()).isZero();
        verify(hxprService, never()).updateEmbeddings(any(), any(), any());
    }

    @Test
    void aDocumentCarryingOnlyThePreviousTypeIsBackfilled() {
        corpus(document("hxpr-1", "node-1", "first body"));
        when(hxprService.listEmbeddingChildren("hxpr-1", false)).thenReturn(List.of(
                new HxprService.EmbeddingChild("child-1", "_e_" + PREVIOUS_TYPE, PREVIOUS_TYPE)));
        embedsSuccessfully();

        assertThat(service.start(0).backfilled()).isEqualTo(1);
    }

    /** No mirror means the text is not in the index, so only a full re-sync can restore it. */
    @Test
    void aDocumentWithNoMirrorIsReportedRatherThanGuessed() {
        corpus(document("hxpr-1", "node-1", null), document("hxpr-2", "node-2", "   "));
        hasNoEmbeddingChildren();

        EmbeddingBackfillService.Progress progress = service.start(0);

        assertThat(progress.skippedNoText()).isEqualTo(2);
        assertThat(progress.backfilled()).isZero();
        verify(hxprService, never()).updateEmbeddings(any(), any(), any());
    }

    /** The fingerprint is deliberately left stale, so the next content sync re-embeds properly. */
    @Test
    void theContentFingerprintIsNotWritten() {
        corpus(document("hxpr-1", "node-1", "first body"));
        hasNoEmbeddingChildren();
        embedsSuccessfully();

        service.start(0);

        verify(hxprService, never()).createDocument(any(), any());
    }

    /** A per-document failure is counted; the run keeps going. */
    @Test
    void oneFailingDocumentDoesNotEndTheRun() {
        corpus(document("hxpr-1", "node-1", "first body"), document("hxpr-2", "node-2", "second body"));
        hasNoEmbeddingChildren();
        embedsSuccessfully();
        org.mockito.Mockito.doThrow(new RuntimeException("hxpr said 500"))
                .when(hxprService).updateEmbeddings(eq("hxpr-1"), any(), any());

        EmbeddingBackfillService.Progress progress = service.start(0);

        assertThat(progress.state()).isEqualTo(EmbeddingBackfillService.State.COMPLETED);
        assertThat(progress.failed()).isEqualTo(1);
        assertThat(progress.backfilled()).isEqualTo(1);
    }

    /** A failed scan is a failed run: continuing would silently cover only part of the corpus. */
    @Test
    void aFailedScanEndsTheRunAsFailed() {
        when(hxprService.advancedQuery(any(), any(), anyInt(), anyInt()))
                .thenThrow(new RuntimeException("hxpr said 500"));

        EmbeddingBackfillService.Progress progress = service.start(0);

        assertThat(progress.state()).isEqualTo(EmbeddingBackfillService.State.FAILED);
        assertThat(progress.error()).contains("hxpr said 500");
    }

    // ------------------------------------------------------------------
    // Rate, pause and resume
    // ------------------------------------------------------------------

    @Test
    void theConfiguredRateBecomesADelayBetweenDocuments() {
        corpus(document("hxpr-1", "node-1", "first body"), document("hxpr-2", "node-2", "second body"));
        hasNoEmbeddingChildren();
        embedsSuccessfully();

        service.start(60);

        assertThat(sleeps).containsExactly(1000L, 1000L);
    }

    @Test
    void anUnlimitedRateNeverSleeps() {
        corpus(document("hxpr-1", "node-1", "first body"));
        hasNoEmbeddingChildren();
        embedsSuccessfully();

        service.start(0);

        assertThat(sleeps).isEmpty();
    }

    /**
     * A skip consumes no embedding capacity, so it is not what the rate limit is for. Throttling skips
     * made a resume over a mostly-complete corpus pay the full per-document delay for every document it
     * had already done (#127).
     */
    @Test
    void skippedDocumentsAreNotRateLimited() {
        corpus(document("hxpr-1", "node-1", "first body"),
                document("hxpr-2", "node-2", "second body"),
                document("hxpr-3", "node-3", null));
        when(hxprService.listEmbeddingChildren("hxpr-1", false)).thenReturn(List.of(
                new HxprService.EmbeddingChild("child-1", "_e_" + TARGET_TYPE, TARGET_TYPE)));
        when(hxprService.listEmbeddingChildren("hxpr-2", false)).thenReturn(List.of());
        when(hxprService.listEmbeddingChildren("hxpr-3", false)).thenReturn(List.of());
        embedsSuccessfully();

        EmbeddingBackfillService.Progress progress = service.start(60);

        // hxpr-2 is the only document that spent an embedding call.
        assertThat(sleeps).containsExactly(1000L);
        assertThat(progress.skippedAlreadyPresent()).isEqualTo(1);
        assertThat(progress.skippedNoText()).isEqualTo(1);
        assertThat(progress.backfilled()).isEqualTo(1);
    }

    /**
     * Pause takes effect between documents and does not hold a thread. Resume re-scans and skips what
     * is already done, which is why no cursor has to be persisted.
     */
    @Test
    void pauseStopsBetweenDocumentsAndResumeFinishesTheRest() {
        HxprDocument first = document("hxpr-1", "node-1", "first body");
        HxprDocument second = document("hxpr-2", "node-2", "second body");
        corpus(first, second);
        embedsSuccessfully();

        // hxpr-1 acquires the target type once written, so the resumed scan skips it.
        List<HxprService.EmbeddingChild> firstChildren = new ArrayList<>();
        when(hxprService.listEmbeddingChildren("hxpr-1", false)).thenReturn(firstChildren);
        when(hxprService.listEmbeddingChildren("hxpr-2", false)).thenReturn(List.of());
        org.mockito.Mockito.doAnswer(invocation -> {
            firstChildren.add(new HxprService.EmbeddingChild("child-1", "_e_" + TARGET_TYPE, TARGET_TYPE));
            return null;
        }).when(hxprService).updateEmbeddings(eq("hxpr-1"), any(), any());

        onSleep = () -> service.pause();
        EmbeddingBackfillService.Progress paused = service.start(60);

        assertThat(paused.state()).isEqualTo(EmbeddingBackfillService.State.PAUSED);
        assertThat(paused.backfilled()).isEqualTo(1);
        verify(hxprService, never()).updateEmbeddings(eq("hxpr-2"), any(), any());

        onSleep = null;
        EmbeddingBackfillService.Progress resumed = service.resume();

        assertThat(resumed.state()).isEqualTo(EmbeddingBackfillService.State.COMPLETED);
        assertThat(resumed.skippedAlreadyPresent())
                .as("the resumed scan re-visits hxpr-1 and skips it")
                .isEqualTo(1);
        // Per-pass counters: the resumed pass backfilled hxpr-2 only, and hxpr-1 shows up as skipped
        // rather than being counted a second time.
        assertThat(resumed.backfilled()).isEqualTo(1);
        assertThat(resumed.scanned())
                .as("a resumed pass reports the corpus size, not the corpus plus what it had already done")
                .isEqualTo(2);
        verify(hxprService).updateEmbeddings(eq("hxpr-2"), any(), any());
    }

    /**
     * The counters have to add up, or {@code status} cannot be read as progress. Before #127 a resume
     * re-scanned from page 0 without resetting {@code scanned}, so a completed run over 41 documents
     * reported 44, which reads like a bug in the corpus rather than in the counter.
     */
    @Test
    void aResumedRunsCountersReconcileWithTheCorpusSize() {
        HxprDocument first = document("hxpr-1", "node-1", "first body");
        HxprDocument second = document("hxpr-2", "node-2", "second body");
        HxprDocument noMirror = document("hxpr-3", "node-3", null);
        HxprDocument stray = new HxprDocument();
        stray.setSysId("hxpr-stray");
        corpus(first, second, noMirror, stray);
        embedsSuccessfully();

        List<HxprService.EmbeddingChild> firstChildren = new ArrayList<>();
        when(hxprService.listEmbeddingChildren("hxpr-1", false)).thenReturn(firstChildren);
        when(hxprService.listEmbeddingChildren("hxpr-2", false)).thenReturn(List.of());
        when(hxprService.listEmbeddingChildren("hxpr-3", false)).thenReturn(List.of());
        org.mockito.Mockito.doAnswer(invocation -> {
            firstChildren.add(new HxprService.EmbeddingChild("child-1", "_e_" + TARGET_TYPE, TARGET_TYPE));
            return null;
        }).when(hxprService).updateEmbeddings(eq("hxpr-1"), any(), any());

        onSleep = () -> service.pause();
        service.start(60);
        onSleep = null;

        EmbeddingBackfillService.Progress resumed = service.resume();

        assertThat(resumed.state()).isEqualTo(EmbeddingBackfillService.State.COMPLETED);
        assertThat(resumed.scanned()).isEqualTo(4);
        assertThat(resumed.backfilled()
                + resumed.skippedAlreadyPresent()
                + resumed.skippedNoText()
                + resumed.skippedNotIngested()
                + resumed.failed())
                .isEqualTo(resumed.scanned());
    }

    @Test
    void resumingWhenNothingIsPausedIsRejected() {
        assertThatThrownBy(() -> service.resume())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("No paused backfill");
        assertThat(service.status().state()).isEqualTo(EmbeddingBackfillService.State.IDLE);
    }

    @Test
    void statusBeforeAnyRunIsIdle() {
        EmbeddingBackfillService.Progress progress = service.status();

        assertThat(progress.state()).isEqualTo(EmbeddingBackfillService.State.IDLE);
        assertThat(progress.targetType()).isEqualTo(TARGET_TYPE);
        assertThat(progress.scanned()).isZero();
    }

    /** The index wait is paid once per run, not once per document, or it would dominate the run. */
    @Test
    void theIndexWaitHappensOncePerRun() {
        corpus(document("hxpr-1", "node-1", "first body"), document("hxpr-2", "node-2", "second body"));
        hasNoEmbeddingChildren();
        embedsSuccessfully();

        service.start(0);

        verify(hxprService).awaitIndex(anyString());
        verify(hxprService, never()).listEmbeddingChildren(anyString());
        verify(hxprService, never()).listEmbeddingChildren(anyString(), eq(true));
    }

    /**
     * The write is handed the child list the job already fetched. Without that, the write's own replace
     * step listed the children again through the <em>waiting</em> overload, so the up-to-30-second index
     * wait was paid on exactly the documents that did work and {@code docsPerMinute} meant nothing
     * whenever the index was behind (#127).
     */
    @Test
    void theWriteReusesTheChildListTheScanAlreadyFetched() {
        corpus(document("hxpr-1", "node-1", "first body"));
        List<HxprService.EmbeddingChild> children = List.of(
                new HxprService.EmbeddingChild("child-1", "_e_" + PREVIOUS_TYPE, PREVIOUS_TYPE));
        when(hxprService.listEmbeddingChildren("hxpr-1", false)).thenReturn(children);
        embedsSuccessfully();

        service.start(0);

        ArgumentCaptor<List<HxprService.EmbeddingChild>> captor = ArgumentCaptor.captor();
        verify(hxprService).updateEmbeddings(eq("hxpr-1"), any(), captor.capture());
        assertThat(captor.getValue()).isEqualTo(children);
    }

    @Test
    void startingASecondRunWhileOneIsRunningIsRejected() {
        // The same-thread executor means start() returns only once the run is over, so a second start
        // is legitimate. Pausing first is what leaves a run in progress to collide with.
        corpus(document("hxpr-1", "node-1", "first body"));
        hasNoEmbeddingChildren();
        embedsSuccessfully();
        service.start(0);

        assertThat(service.status().state()).isEqualTo(EmbeddingBackfillService.State.COMPLETED);
        assertThat(service.start(0).state()).isNotEqualTo(EmbeddingBackfillService.State.FAILED);
    }

    /** Documents with no source id are not ingested content; the job must not try to embed them. */
    @Test
    void aDocumentWithNoSourceIdIsSkipped() {
        HxprDocument stray = new HxprDocument();
        stray.setSysId("hxpr-stray");
        corpus(stray);

        EmbeddingBackfillService.Progress progress = service.start(0);

        assertThat(progress.scanned()).isEqualTo(1);
        // Counted, not silently dropped: an uncounted skip is what stopped the counters reconciling.
        assertThat(progress.skippedNotIngested()).isEqualTo(1);
        assertThat(progress.backfilled()).isZero();
        verify(hxprService, never()).listEmbeddingChildren(anyString(), anyBoolean());
    }
}
