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
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.nio.charset.StandardCharsets;
import java.time.OffsetDateTime;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * The staleness guard must not skip content for a document that holds none (#100).
 *
 * <p>Comparing {@code source_modifiedAt} alone was enough to conclude "already current", and a document
 * can carry the node's timestamp while holding no content at all: {@code updatePermissions} creates
 * exactly that when a permission event arrives for a node with no document yet. Every later sync of an
 * unmodified node then took the skip branch, so the document stayed unretrievable until the node was next
 * modified -- and because the skip refreshes permissions, the log looked like a permission update rather
 * than a lost content pass. It cost 8 of the 10 failures of the 2026-09-14 gate run, across both the
 * Alfresco and the Nuxeo phases.</p>
 */
@ExtendWith(MockitoExtension.class)
class NodeSyncServiceStalenessTest {

    private static final String NODE_ID = "node-1";
    private static final String SOURCE_ID = "default";
    private static final String FORMATTED_SOURCE_ID = "alfresco:default";
    private static final String HXPR_DOC_ID = "hxpr-1";
    private static final String TARGET_PATH = "/alfresco-sync";

    private static final String TEXT = "Available to all staff. Public company announcement.";
    private static final String EMBEDDING_TYPE = "ai-mxbai-embed-large";
    private static final ChunkingConfig CONFIG = new ChunkingConfig(200, 1024, 256, 0.75);

    private static final String P_EXTRACTED_TEXT = ContentLakeIngestProperties.CONTENT_LAKE_EXTRACTED_TEXT;
    private static final String P_SYNC_STATUS = ContentLakeIngestProperties.CONTENT_LAKE_SYNC_STATUS;
    private static final String P_MODIFIED_AT = ContentLakeIngestProperties.SOURCE_MODIFIED_AT;

    /** The node has not been touched since the stored copy was written, so timestamps alone say "skip". */
    private static final String UNCHANGED_MODIFIED_AT = "2026-01-01T00:00:00Z";

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
        service = new NodeSyncService(
                sourceClient, documentApi, hxprService, textExtractor, embeddingService,
                chunkingService, TARGET_PATH, null, false, true);
    }

    // ------------------------------------------------------------------
    // Skipped: the copy really is current
    // ------------------------------------------------------------------

    /** The cheap timestamp skip has to survive the fix, or every sweep re-extracts the whole corpus. */
    @Test
    void anUnmodifiedNodeWithAnIndexedDocumentSkipsContent() {
        stored("INDEXED", TEXT);

        service.syncNode(node());

        verifyContentWasNotProcessed();
    }

    /**
     * {@code FAILED} stays skippable. Its commonest cause is a binary with no extractable text at all,
     * which is a stable outcome, and retrying it every sweep would pay extraction for every such file in
     * the corpus forever.
     */
    @Test
    void anUnmodifiedNodeWhoseLastPassFailedStillSkipsContent() {
        stored("FAILED", null);

        service.syncNode(node());

        verifyContentWasNotProcessed();
    }

    // ------------------------------------------------------------------
    // Processed: the document holds no content
    // ------------------------------------------------------------------

    /**
     * The defect. This is the state {@code updatePermissions} leaves behind, and the node's timestamp is
     * unchanged because the permission update copied it from the same node.
     */
    @Test
    void anUnmodifiedNodeWhosePendingDocumentHasNoContentIsProcessed() {
        stored("PENDING", null);
        expectContentPass();

        service.syncNode(node());

        verify(hxprService).updateEmbeddings(eq(HXPR_DOC_ID), any());
    }

    /**
     * Indexed while holding no content: invisible to search, finished to monitoring. Skipping it on the
     * timestamp would make that state permanent, which is the same reason {@code canReuseContent} requires
     * the mirror.
     */
    @Test
    void anUnmodifiedNodeIndexedWithNoExtractedTextIsProcessed() {
        stored("INDEXED", null);
        expectContentPass();

        service.syncNode(node());

        verify(hxprService).updateEmbeddings(eq(HXPR_DOC_ID), any());
    }

    /** A pending document that does hold content is mid-pipeline, not abandoned, but repeating the pass
     * is the safe reading: the batch pipeline writes PENDING before handing content to a worker, so
     * "pending with a mirror" is a pass that has not reported completion. */
    @Test
    void anUnmodifiedNodeStillPendingIsProcessedEvenWithAMirror() {
        stored("PENDING", TEXT);
        expectContentPass();

        service.syncNode(node());

        verify(hxprService).updateEmbeddings(eq(HXPR_DOC_ID), any());
    }

    // ------------------------------------------------------------------
    // Fixtures
    // ------------------------------------------------------------------

    private static SourceNode node() {
        return new SourceNode(
                NODE_ID, SOURCE_ID, "alfresco",
                "public-announcement.txt", "/content-lake-test", "text/plain",
                OffsetDateTime.parse(UNCHANGED_MODIFIED_AT),
                false,
                Set.of("GROUP_EVERYONE"), Set.of(),
                Map.of("source_nodeId", NODE_ID, "source_type", "alfresco",
                        P_MODIFIED_AT, UNCHANGED_MODIFIED_AT));
    }

    /** Stores a document for the node with the given sync status and extracted-text mirror. */
    private void stored(String syncStatus, String mirror) {
        Map<String, Object> props = new LinkedHashMap<>();
        props.put("source_nodeId", NODE_ID);
        props.put(P_MODIFIED_AT, UNCHANGED_MODIFIED_AT);
        props.put(P_SYNC_STATUS, syncStatus);
        if (mirror != null) {
            props.put(P_EXTRACTED_TEXT, mirror);
        }

        HxprDocument existing = new HxprDocument();
        existing.setSysId(HXPR_DOC_ID);
        existing.setCinIngestProperties(props);
        when(hxprService.findByNodeId(NODE_ID, FORMATTED_SOURCE_ID)).thenReturn(existing);
    }

    /** Stubs what a full extract-chunk-embed pass needs. */
    private void expectContentPass() {
        HxprDocument updated = new HxprDocument();
        updated.setSysId(HXPR_DOC_ID);
        when(documentApi.updateById(eq(HXPR_DOC_ID), any(HxprDocument.class))).thenReturn(updated);
        when(sourceClient.getContent(NODE_ID)).thenReturn(TEXT.getBytes(StandardCharsets.UTF_8));
        when(hxprService.getEmbeddingType()).thenReturn(EMBEDDING_TYPE);
        when(chunkingService.getConfig()).thenReturn(CONFIG);

        Chunk chunk = new Chunk(NODE_ID, TEXT, 0, 0, TEXT.length());
        when(chunkingService.chunk(eq(TEXT), eq(NODE_ID), eq("text/plain"))).thenReturn(List.of(chunk));
        when(embeddingService.embedChunks(eq(List.of(chunk)), any()))
                .thenReturn(List.of(new EmbeddingService.ChunkWithEmbedding(chunk, List.of(0.1d, 0.2d))));
    }

    /** The skip branch refreshes permissions and reads no content at all. */
    private void verifyContentWasNotProcessed() {
        verify(sourceClient, never()).getContent(any());
        verify(chunkingService, never()).chunk(any(), any(), any());
        verify(embeddingService, never()).embedChunks(any(), any());
        verify(hxprService, never()).updateEmbeddings(any(), any());
        verify(documentApi).updateById(eq(HXPR_DOC_ID), any(HxprDocument.class));
    }
}
