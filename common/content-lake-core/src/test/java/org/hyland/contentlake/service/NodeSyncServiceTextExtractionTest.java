package org.hyland.contentlake.service;

import org.hyland.contentlake.client.HxprDocumentApi;
import org.hyland.contentlake.client.HxprService;
import org.hyland.contentlake.model.Chunk;
import org.hyland.contentlake.model.ContentLakeIngestProperties;
import org.hyland.contentlake.model.HxprDocument;
import org.hyland.contentlake.service.chunking.SimpleChunkingService;
import org.hyland.contentlake.spi.ContentSourceClient;
import org.hyland.contentlake.spi.ExtractedText;
import org.hyland.contentlake.spi.TextExtractor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.core.io.Resource;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
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
                null,
                false,
                true
        );
    }

    @Test
    void processContent_usesSourceReferenceExtractionWithoutDownloadingTempFile() {
        Chunk chunk = new Chunk("node-1", "Converted by Nuxeo", 0, 0, 18);
        HxprDocument updatedDocument = new HxprDocument();
        updatedDocument.setSysId("hxpr-doc-1");

        when(textExtractor.supportsSourceReference("application/pdf")).thenReturn(true);
        when(textExtractor.extract("node-1", "application/pdf"))
                .thenReturn(ExtractedText.plain("Converted by Nuxeo"));
        when(chunkingService.chunk("Converted by Nuxeo", "node-1", "application/pdf")).thenReturn(List.of(chunk));
        when(embeddingService.embedChunks(eq(List.of(chunk)), any()))
                .thenReturn(List.of(new EmbeddingService.ChunkWithEmbedding(chunk, List.of(0.1d, 0.2d))));
        when(documentApi.updateById(eq("hxpr-doc-1"), any())).thenReturn(updatedDocument);

        service.processContent(
                "hxpr-doc-1",
                Map.of("source_nodeId", "node-1"),
                "node-1",
                "application/pdf",
                "report.pdf",
                "/default-domain/workspaces/report.pdf"
        );

        verify(textExtractor).extract("node-1", "application/pdf");
        verify(textExtractor, never()).extract(any(Resource.class), eq("application/pdf"));
        verify(textExtractor, never()).extractText(any(Resource.class), eq("application/pdf"));
        verify(sourceClient, never()).downloadContent(any(), any());
        verify(hxprService).updateEmbeddings(eq("hxpr-doc-1"), any());
    }

    /**
     * Markdown reaches the chunker verbatim, so heading and table boundaries survive segmentation,
     * while both fulltext mirrors get the markup stripped: they feed an analysed keyword index where
     * pipes and dashes are noise rather than terms.
     */
    @Test
    void markdownExtractionChunksTheMarkdownButMirrorsStrippedText() {
        String markdown = "## Regions\n\n| Region | FTE |\n| --- | --- |\n| Iberia | 227 |";
        Chunk chunk = new Chunk("node-2", markdown, 0, 0, markdown.length());
        HxprDocument updatedDocument = new HxprDocument();
        updatedDocument.setSysId("hxpr-doc-2");

        when(textExtractor.supportsSourceReference("application/pdf")).thenReturn(true);
        when(textExtractor.extract("node-2", "application/pdf"))
                .thenReturn(ExtractedText.markdown(markdown));
        when(chunkingService.chunk(markdown, "node-2", "application/pdf")).thenReturn(List.of(chunk));
        when(embeddingService.embedChunks(eq(List.of(chunk)), any()))
                .thenReturn(List.of(new EmbeddingService.ChunkWithEmbedding(chunk, List.of(0.1d))));
        when(documentApi.updateById(eq("hxpr-doc-2"), any())).thenReturn(updatedDocument);

        service.processContent(
                "hxpr-doc-2",
                Map.of("source_nodeId", "node-2"),
                "node-2",
                "application/pdf",
                "regions.pdf",
                "/default-domain/workspaces/regions.pdf"
        );

        // The chunker saw the markdown verbatim.
        verify(chunkingService).chunk(markdown, "node-2", "application/pdf");

        ArgumentCaptor<HxprDocument> captor = ArgumentCaptor.forClass(HxprDocument.class);
        verify(documentApi).updateById(eq("hxpr-doc-2"), captor.capture());
        HxprDocument update = captor.getValue();

        assertThat(update.getSysFulltextBinary())
                .doesNotContain("|")
                .doesNotContain("---")
                .doesNotContain("##")
                .contains("Regions")
                .contains("Iberia  227");

        Object mirrored = update.getCinIngestProperties()
                .get(ContentLakeIngestProperties.CONTENT_LAKE_EXTRACTED_TEXT);
        assertThat(mirrored).asString().doesNotContain("|").contains("Iberia  227");
    }
}
