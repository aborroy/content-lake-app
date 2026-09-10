package org.hyland.contentlake.service;

import org.hyland.contentlake.client.HxprDocumentApi;
import org.hyland.contentlake.client.HxprService;
import org.hyland.contentlake.model.Chunk;
import org.hyland.contentlake.model.ContentLakeIngestProperties;
import org.hyland.contentlake.model.HxprDocument;
import org.hyland.contentlake.service.chunking.SimpleChunkingService;
import org.hyland.contentlake.spi.ContentSourceClient;
import org.hyland.contentlake.spi.TextExtractor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Extracted text must reach {@code cin_ingestProperties} for the keyword leg of hybrid search.
 *
 * <p>The sync also writes {@code sys_fulltextBinary}, but hxpr does not expose that field to HXQL:
 * a query against it matches nothing, verified against a live index. hxpr does fold
 * {@code cin_ingestProperties} into its analysed {@code sys_fulltext} index, so this property is the
 * only thing that makes document body text matchable by term. Without it
 * {@code keyword_leg_hit_rate} measures 0.0000 and hybrid search silently degenerates to its vector
 * leg alone.</p>
 */
@ExtendWith(MockitoExtension.class)
class NodeSyncServiceExtractedTextTest {

    private static final String EXTRACTED_TEXT = ContentLakeIngestProperties.CONTENT_LAKE_EXTRACTED_TEXT;

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
                true,  // keyword-leg context enrichment on: these tests assert the prefix
                true
        );
    }

    private HxprDocument syncAndCaptureUpdate(String text) {
        Chunk chunk = new Chunk("node-1", text, 0, 0, text.length());
        HxprDocument updated = new HxprDocument();
        updated.setSysId("hxpr-doc-1");

        // text/plain is read straight from the source client rather than through the extractor.
        when(sourceClient.getContent("node-1")).thenReturn(text.getBytes(StandardCharsets.UTF_8));
        when(chunkingService.chunk(text, "node-1", "text/plain")).thenReturn(List.of(chunk));
        when(embeddingService.embedChunks(eq(List.of(chunk)), any()))
                .thenReturn(List.of(new EmbeddingService.ChunkWithEmbedding(chunk, List.of(0.1d, 0.2d))));
        when(documentApi.updateById(eq("hxpr-doc-1"), any())).thenReturn(updated);

        service.processContent(
                "hxpr-doc-1",
                Map.of("source_nodeId", "node-1"),
                "node-1",
                "text/plain",
                "doc.txt",
                "/default-domain/workspaces/doc.txt"
        );

        ArgumentCaptor<HxprDocument> captor = ArgumentCaptor.forClass(HxprDocument.class);
        verify(documentApi).updateById(eq("hxpr-doc-1"), captor.capture());
        return captor.getValue();
    }

    @Test
    void extractedTextIsMirroredIntoIngestPropertiesForKeywordSearch() {
        HxprDocument update = syncAndCaptureUpdate("Collect evidence in order of volatility.");

        assertThat(update.getCinIngestProperties())
                .as("the keyword leg reads this property; sys_fulltextBinary is not queryable")
                .containsKey(EXTRACTED_TEXT);
        assertThat((String) update.getCinIngestProperties().get(EXTRACTED_TEXT))
                .contains("volatility");
    }

    @Test
    void extractedBodyIsStoredVerbatimAfterContextPrefix() {
        // No case folding: hxpr's sys_fulltext index analyses this text and matches it
        // case-insensitively, so folding here would lose information for no benefit. The document
        // body is preserved verbatim; only a context prefix is prepended (see next test).
        HxprDocument update = syncAndCaptureUpdate("Severity 1 RESPONSE within 15 minutes");

        assertThat((String) update.getCinIngestProperties().get(EXTRACTED_TEXT))
                .endsWith("Severity 1 RESPONSE within 15 minutes");
    }

    @Test
    void keywordLegTextIsEnrichedWithDocumentContext() {
        // #66: the keyword leg gets the same document-context prefix the vector leg already gets, so
        // the document name/path are matchable by term, not just the raw body.
        HxprDocument update = syncAndCaptureUpdate("Collect evidence in order of volatility.");

        String indexed = (String) update.getCinIngestProperties().get(EXTRACTED_TEXT);
        assertThat(indexed).contains("Document: doc.txt");
        assertThat(indexed).contains("Path: /default-domain/workspaces/doc.txt");
        assertThat(indexed).contains("volatility");
    }

    @Test
    void sysFulltextBinaryStaysRawWithoutContextPrefix() {
        // The enrichment is applied only to the queryable ingest property; the binary fulltext field
        // stays the pure extracted body.
        HxprDocument update = syncAndCaptureUpdate("Collect evidence in order of volatility.");

        assertThat(update.getSysFulltextBinary()).doesNotContain("Document:");
    }

    @Test
    void propertyNamesMirrorPropertyKeys() {
        // cin_ingestPropertyNames must always mirror cin_ingestProperties.keySet(); adding a
        // property without the name leaves hxpr unable to see it.
        HxprDocument update = syncAndCaptureUpdate("some extracted body text");

        assertThat(update.getCinIngestPropertyNames())
                .containsExactlyInAnyOrderElementsOf(update.getCinIngestProperties().keySet());
        assertThat(update.getCinIngestPropertyNames()).contains(EXTRACTED_TEXT);
    }

    @Test
    void sysFulltextBinaryIsStillWritten() {
        // Keep writing it: it is the semantically correct home for extracted text and costs nothing,
        // even though it cannot currently be queried.
        HxprDocument update = syncAndCaptureUpdate("Collect evidence in order of volatility.");

        assertThat(update.getSysFulltextBinary()).isEqualTo("Collect evidence in order of volatility.");
    }
}
