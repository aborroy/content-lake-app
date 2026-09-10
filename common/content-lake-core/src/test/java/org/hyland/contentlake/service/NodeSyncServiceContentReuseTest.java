package org.hyland.contentlake.service;

import org.hyland.contentlake.client.HxprDocumentApi;
import org.hyland.contentlake.client.HxprService;
import org.hyland.contentlake.model.Chunk;
import org.hyland.contentlake.model.ContentLakeIngestProperties;
import org.hyland.contentlake.model.HxprDocument;
import org.hyland.contentlake.service.chunking.SimpleChunkingService;
import org.hyland.contentlake.service.chunking.strategy.ChunkingStrategy.ChunkingConfig;
import org.hyland.contentlake.spi.ContentSourceClient;
import org.hyland.contentlake.spi.SourceNode;
import org.hyland.contentlake.spi.TextExtractor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.nio.charset.StandardCharsets;
import java.time.OffsetDateTime;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Content reuse (#120): a document whose bytes did not change must not be chunked or embedded again.
 *
 * <p>The staleness guard cannot cover this. It compares {@code source_modifiedAt}, and a permission
 * change, a property edit or a folder move all produce a newer one, so every such operation used to
 * re-run extraction, chunking and embedding over identical content. Embedding throughput is the
 * pipeline bottleneck, so on an actively used repository that was the largest avoidable consumer of
 * it.</p>
 */
@ExtendWith(MockitoExtension.class)
class NodeSyncServiceContentReuseTest {

    private static final String NODE_ID = "node-1";
    private static final String SOURCE_ID = "default";
    private static final String FORMATTED_SOURCE_ID = "alfresco:default";
    private static final String HXPR_DOC_ID = "hxpr-1";
    private static final String TARGET_PATH = "/alfresco-sync";

    private static final String TEXT = "Change CHG-105402 was reverted after the rollback failed.";
    private static final String EMBEDDING_TYPE = "ai-mxbai-embed-large";
    private static final ChunkingConfig CONFIG = new ChunkingConfig(200, 1024, 256, 0.75);
    private static final String SECTION_MAP = "{\"chunkSections\":[0],\"sections\":[]}";

    private static final String P_FINGERPRINT = ContentLakeIngestProperties.CONTENT_LAKE_CONTENT_FINGERPRINT;
    private static final String P_EXTRACTED_TEXT = ContentLakeIngestProperties.CONTENT_LAKE_EXTRACTED_TEXT;
    private static final String P_SECTION_MAP = ContentLakeIngestProperties.CONTENT_LAKE_SECTION_MAP;
    private static final String P_MODIFIED_AT = ContentLakeIngestProperties.SOURCE_MODIFIED_AT;

    private static final String STORED_MODIFIED_AT = "2026-01-01T00:00:00Z";
    private static final String INCOMING_MODIFIED_AT = "2026-02-01T00:00:00Z";

    @Mock
    private ContentSourceClient sourceClient;
    @Mock
    private HxprDocumentApi documentApi;
    @Mock
    private HxprService hxprService;
    @Mock
    private TextExtractor textExtractor;
    @Mock
    private EmbeddingService embeddingService;
    @Mock
    private SimpleChunkingService chunkingService;

    private NodeSyncService service;

    @BeforeEach
    void setUp() {
        service = newService(true);
    }

    private NodeSyncService newService(boolean contentReuseEnabled) {
        return new NodeSyncService(
                sourceClient,
                documentApi,
                hxprService,
                textExtractor,
                embeddingService,
                chunkingService,
                TARGET_PATH,
                null,
                false,
                contentReuseEnabled
        );
    }

    // ------------------------------------------------------------------
    // Fixtures
    // ------------------------------------------------------------------

    private static String fingerprintOf(String text) {
        return ContentFingerprint.of(text, EMBEDDING_TYPE, CONFIG, false);
    }

    private static SourceNode node(Set<String> readPrincipals, Set<String> denyPrincipals) {
        return new SourceNode(
                NODE_ID,
                SOURCE_ID,
                "alfresco",
                "change-register-extract.txt",
                "/registers",
                "text/plain",
                OffsetDateTime.parse(INCOMING_MODIFIED_AT),
                false,
                readPrincipals,
                denyPrincipals,
                Map.of(
                        "source_nodeId", NODE_ID,
                        "source_type", "alfresco",
                        P_MODIFIED_AT, INCOMING_MODIFIED_AT
                )
        );
    }

    private static SourceNode node() {
        return node(Set.of("GROUP_EVERYONE"), Set.of());
    }

    /** A document already indexed from {@code storedText}, as a completed content pass leaves it. */
    private HxprDocument indexedDocument(String storedFingerprint, String storedMirror) {
        Map<String, Object> props = new LinkedHashMap<>();
        props.put("source_nodeId", NODE_ID);
        props.put(P_MODIFIED_AT, STORED_MODIFIED_AT);
        props.put(ContentLakeIngestProperties.CONTENT_LAKE_SYNC_STATUS, "INDEXED");
        if (storedFingerprint != null) {
            props.put(P_FINGERPRINT, storedFingerprint);
        }
        if (storedMirror != null) {
            props.put(P_EXTRACTED_TEXT, storedMirror);
            props.put(P_SECTION_MAP, SECTION_MAP);
        }

        HxprDocument existing = new HxprDocument();
        existing.setSysId(HXPR_DOC_ID);
        existing.setCinIngestProperties(props);
        return existing;
    }

    /**
     * Wires the mocks for a sync of {@code sourceText} against a stored document, and returns every
     * payload passed to {@code documentApi.updateById} in order: the metadata write first, then
     * whatever content processing decided to write.
     */
    private List<HxprDocument> sync(NodeSyncService target, SourceNode node,
                                    HxprDocument existing, String sourceText) {
        HxprDocument updated = new HxprDocument();
        updated.setSysId(HXPR_DOC_ID);

        when(hxprService.findByNodeId(NODE_ID, FORMATTED_SOURCE_ID)).thenReturn(existing);
        when(documentApi.updateById(eq(HXPR_DOC_ID), any(HxprDocument.class))).thenReturn(updated);
        when(sourceClient.getContent(NODE_ID)).thenReturn(sourceText.getBytes(StandardCharsets.UTF_8));
        when(hxprService.getEmbeddingType()).thenReturn(EMBEDDING_TYPE);
        when(chunkingService.getConfig()).thenReturn(CONFIG);

        target.syncNode(node);

        ArgumentCaptor<HxprDocument> captor = ArgumentCaptor.forClass(HxprDocument.class);
        verify(documentApi, org.mockito.Mockito.atLeastOnce())
                .updateById(eq(HXPR_DOC_ID), captor.capture());
        return captor.getAllValues();
    }

    /** Stubs the chunk-and-embed collaborators a full reprocess needs. */
    private void expectFullReprocess(String text) {
        Chunk chunk = new Chunk(NODE_ID, text, 0, 0, text.length());
        when(chunkingService.chunk(eq(text), eq(NODE_ID), eq("text/plain"))).thenReturn(List.of(chunk));
        when(embeddingService.embedChunks(eq(List.of(chunk)), any()))
                .thenReturn(List.of(new EmbeddingService.ChunkWithEmbedding(chunk, List.of(0.1d, 0.2d))));
    }

    // ------------------------------------------------------------------
    // Short circuit
    // ------------------------------------------------------------------

    @Test
    void whenContentIsUnchanged_skipsChunkingAndEmbedding() {
        List<HxprDocument> writes = sync(service, node(), indexedDocument(fingerprintOf(TEXT), TEXT), TEXT);

        verify(chunkingService, never()).chunk(any(), any(), any());
        verify(embeddingService, never()).embedChunks(any(), any());
        verify(hxprService, never()).updateEmbeddings(any(), any());

        HxprDocument statusWrite = writes.get(writes.size() - 1);
        assertThat(statusWrite.getSyncStatus()).isEqualTo(HxprDocument.SyncStatus.INDEXED);
        assertThat(service.getContentReuseStats())
                .isEqualTo(new NodeSyncService.ContentReuseStats(1, 0));
    }

    @Test
    void aMetadataOnlyChangeStillAdvancesSourceModifiedAt() {
        List<HxprDocument> writes = sync(service, node(), indexedDocument(fingerprintOf(TEXT), TEXT), TEXT);

        verify(embeddingService, never()).embedChunks(any(), any());
        assertThat(writes).allSatisfy(write ->
                assertThat(write.getCinIngestProperties()).containsEntry(P_MODIFIED_AT, INCOMING_MODIFIED_AT));
    }

    /**
     * The short circuit writes the content-derived properties back rather than relying on hxpr to
     * merge a map-valued field, so the keyword mirror and the section map survive a metadata write
     * that rebuilt the property map from source metadata alone.
     */
    @Test
    void aShortCircuitPreservesTheKeywordMirrorAndTheSectionMap() {
        List<HxprDocument> writes = sync(service, node(), indexedDocument(fingerprintOf(TEXT), TEXT), TEXT);

        assertThat(writes).allSatisfy(write -> {
            assertThat(write.getCinIngestProperties())
                    .containsEntry(P_EXTRACTED_TEXT, TEXT)
                    .containsEntry(P_SECTION_MAP, SECTION_MAP)
                    .containsEntry(P_FINGERPRINT, fingerprintOf(TEXT));
            assertThat(write.getCinIngestPropertyNames())
                    .containsExactlyInAnyOrderElementsOf(write.getCinIngestProperties().keySet());
        });
    }

    @Test
    void anAclOnlyChangeIsWrittenAndContentIsStillSkipped() {
        SourceNode node = node(Set.of("bob", "GROUP_sales"), Set.of("GROUP_secret"));

        List<HxprDocument> writes = sync(service, node, indexedDocument(fingerprintOf(TEXT), TEXT), TEXT);

        verify(embeddingService, never()).embedChunks(any(), any());

        HxprDocument metadataWrite = writes.get(0);
        assertThat(metadataWrite.getCinRead()).containsExactly("GROUP_sales", "bob");
        assertThat(metadataWrite.getCinDeny()).containsExactly("GROUP_secret");
        assertThat(metadataWrite.getSysAcl()).hasSize(2);
    }

    // ------------------------------------------------------------------
    // Full reprocess
    // ------------------------------------------------------------------

    @Test
    void aContentEditTriggersAFullReprocessAndStoresTheNewFingerprint() {
        String edited = TEXT + " Root cause: a missing index.";
        expectFullReprocess(edited);

        List<HxprDocument> writes = sync(service, node(), indexedDocument(fingerprintOf(TEXT), TEXT), edited);

        verify(hxprService).updateEmbeddings(eq(HXPR_DOC_ID), any());
        assertThat(writes.get(writes.size() - 1).getCinIngestProperties())
                .containsEntry(P_FINGERPRINT, fingerprintOf(edited));
        assertThat(service.getContentReuseStats())
                .isEqualTo(new NodeSyncService.ContentReuseStats(0, 1));
    }

    /**
     * The defect a text-only hash would introduce. Changing the embedding model must re-embed
     * identical text, or the corpus keeps serving the retired model's vectors while the type-aware
     * clear path never runs (#113).
     */
    @Test
    void aChangedEmbeddingTypeReprocessesIdenticalText() {
        expectFullReprocess(TEXT);
        String fingerprintUnderPreviousModel =
                ContentFingerprint.of(TEXT, "ai-nomic-embed-text", CONFIG, false);

        sync(service, node(), indexedDocument(fingerprintUnderPreviousModel, TEXT), TEXT);

        verify(hxprService).updateEmbeddings(eq(HXPR_DOC_ID), any());
    }

    /**
     * A fingerprint with no keyword mirror is not evidence of a completed content pass. Trusting it
     * would make the "indexed with zero embeddings" state permanent, since the document would then
     * short-circuit on every subsequent sync.
     */
    @Test
    void aFingerprintWithNoKeywordMirrorReprocesses() {
        expectFullReprocess(TEXT);

        sync(service, node(), indexedDocument(fingerprintOf(TEXT), null), TEXT);

        verify(hxprService).updateEmbeddings(eq(HXPR_DOC_ID), any());
    }

    @Test
    void aDocumentWithNoStoredFingerprintReprocessesToAcquireOne() {
        expectFullReprocess(TEXT);

        List<HxprDocument> writes = sync(service, node(), indexedDocument(null, TEXT), TEXT);

        verify(hxprService).updateEmbeddings(eq(HXPR_DOC_ID), any());
        assertThat(writes.get(writes.size() - 1).getCinIngestProperties())
                .containsEntry(P_FINGERPRINT, fingerprintOf(TEXT));
    }

    @Test
    void withContentReuseDisabled_unchangedContentIsStillReprocessed() {
        NodeSyncService disabled = newService(false);
        expectFullReprocess(TEXT);

        List<HxprDocument> writes = sync(disabled, node(), indexedDocument(fingerprintOf(TEXT), TEXT), TEXT);

        verify(hxprService).updateEmbeddings(eq(HXPR_DOC_ID), any());
        assertThat(writes.get(writes.size() - 1).getCinIngestProperties())
                .as("the fingerprint is still recorded, so re-enabling the flag works immediately")
                .containsEntry(P_FINGERPRINT, fingerprintOf(TEXT));
        assertThat(disabled.getContentReuseStats())
                .isEqualTo(new NodeSyncService.ContentReuseStats(0, 1));
    }
}
