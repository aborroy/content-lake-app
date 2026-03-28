package org.hyland.contentlake.service;

import org.hyland.contentlake.client.HxprDocumentApi;
import org.hyland.contentlake.client.HxprService;
import org.hyland.contentlake.model.Chunk;
import org.hyland.contentlake.model.HxprDocument;
import org.hyland.contentlake.service.chunking.SimpleChunkingService;
import org.hyland.contentlake.spi.ContentSourceClient;
import org.hyland.contentlake.spi.TextExtractor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.core.io.Resource;

import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class NodeSyncServiceTextExtractionTest {

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
                sourceClient,
                documentApi,
                hxprService,
                textExtractor,
                embeddingService,
                chunkingService,
                "/nuxeo-sync",
                null
        );
    }

    @Test
    void processContent_usesSourceReferenceExtractionWithoutDownloadingTempFile() {
        Chunk chunk = new Chunk("node-1", "Converted by Nuxeo", 0, 0, 18);
        HxprDocument updatedDocument = new HxprDocument();
        updatedDocument.setSysId("hxpr-doc-1");

        when(textExtractor.supportsSourceReference("application/pdf")).thenReturn(true);
        when(textExtractor.extractText("node-1", "application/pdf")).thenReturn("Converted by Nuxeo");
        when(chunkingService.chunk("Converted by Nuxeo", "node-1", "application/pdf")).thenReturn(List.of(chunk));
        when(embeddingService.embedChunks(eq(List.of(chunk)), any()))
                .thenReturn(List.of(new EmbeddingService.ChunkWithEmbedding(chunk, List.of(0.1d, 0.2d))));
        when(embeddingService.getModelName()).thenReturn("test-embedding-model");
        when(documentApi.updateById(eq("hxpr-doc-1"), any())).thenReturn(updatedDocument);

        service.processContent(
                "hxpr-doc-1",
                Map.of("source_nodeId", "node-1"),
                "node-1",
                "application/pdf",
                "report.pdf",
                "/default-domain/workspaces/report.pdf"
        );

        verify(textExtractor).extractText("node-1", "application/pdf");
        verify(textExtractor, never()).extractText(any(Resource.class), eq("application/pdf"));
        verify(sourceClient, never()).downloadContent(any(), any());
        verify(hxprService).updateEmbeddings(eq("hxpr-doc-1"), any());
    }
}
