package org.hyland.contentlake.service;

import org.hyland.contentlake.client.HxprDocumentApi;
import org.hyland.contentlake.client.HxprService;
import org.hyland.contentlake.model.ContentLakeIngestProperties;
import org.hyland.contentlake.model.ContentLakeNodeStatus;
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

/**
 * Content that cannot contain text is skipped before it is downloaded (#154).
 *
 * <p>The assertion that matters is the negative one: not that no text was stored, which was already true, but
 * that the source was never asked for the bytes. On a metered source that download is the charge, and it is the
 * only part of this the policy can save.</p>
 */
@ExtendWith(MockitoExtension.class)
class NodeSyncServiceNonTextSkipTest {

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
                sourceClient, documentApi, hxprService, textExtractor, embeddingService, chunkingService,
                "/sharepoint-sync", null, false, true);
    }

    @Test
    void neverDownloadsContentWhoseExtensionCannotContainText() {
        service.processContent(
                "hxpr-doc-1",
                Map.of("source_nodeId", "node-1"),
                "node-1",
                "application/octet-stream",
                "Multilanguage.docx_signed.csig",
                "/samples/Multilanguage.docx_signed.csig");

        // The whole point: no bytes were fetched, by either route.
        verify(sourceClient, never()).downloadContent(any(), any());
        verify(sourceClient, never()).getContent(any());

        // And the extractor was never consulted either, so no parser could throw on it. That is what ended a
        // real crawl at 51 of 52 documents.
        verify(textExtractor, never()).supports(any());
        verify(textExtractor, never()).supportsSourceReference(any());
        verify(textExtractor, never()).extract(any(Resource.class), any());
        verify(textExtractor, never()).extract(any(String.class), any());

        verify(chunkingService, never()).chunk(any(), any(), any());
        verify(hxprService, never()).updateEmbeddings(any(), any());
    }

    @Test
    void countsTheSkipSoASilentSavingIsStillVisible() {
        assertThat(service.getNonTextSkipCount()).isZero();

        service.processContent("hxpr-doc-1", Map.of(), "node-1",
                "application/octet-stream", "a.csig", "/a.csig");
        service.processContent("hxpr-doc-2", Map.of(), "node-2",
                "application/zip", "bundle", "/bundle");

        assertThat(service.getNonTextSkipCount()).isEqualTo(2);
    }

    @Test
    void recordsItsOwnReasonRatherThanTheEmptyExtractionOne() {
        service.processContent("hxpr-doc-1", Map.of("source_nodeId", "node-1"), "node-1",
                "application/octet-stream", "detached.p7s", "/detached.p7s");

        ArgumentCaptor<HxprDocument> captor = ArgumentCaptor.forClass(HxprDocument.class);
        verify(documentApi).updateById(eq("hxpr-doc-1"), captor.capture());
        Object error = captor.getValue().getCinIngestProperties()
                .get(ContentLakeIngestProperties.CONTENT_LAKE_SYNC_ERROR);

        // "We did not try" and "we tried and got nothing" are different facts, and only the second is worth an
        // operator's attention. So this must not read as the empty-extraction message.
        assertThat(String.valueOf(error))
                .isEqualTo("Content not read, because its extension .p7s cannot contain text")
                .doesNotContain("No extractable text");
    }

    @Test
    void recordsSkippedRatherThanFailed() {
        service.processContent("hxpr-doc-1", Map.of("source_nodeId", "node-1"), "node-1",
                "application/octet-stream", "detached.p7s", "/detached.p7s");

        ArgumentCaptor<HxprDocument> captor = ArgumentCaptor.forClass(HxprDocument.class);
        verify(documentApi).updateById(eq("hxpr-doc-1"), captor.capture());
        HxprDocument patched = captor.getValue();

        // The status an operator or a query can filter on, not just the error string. FAILED would claim a
        // pass ran and produced nothing; nothing was attempted.
        assertThat(patched.getCinIngestProperties().get(ContentLakeIngestProperties.CONTENT_LAKE_SYNC_STATUS))
                .isEqualTo(ContentLakeNodeStatus.Status.SKIPPED.name());
        assertThat(patched.getSyncStatus()).isEqualTo(HxprDocument.SyncStatus.SKIPPED);
    }

    @Test
    void stillExtractsATypeThePolicyDoesNotDeny() {
        when(textExtractor.supportsSourceReference("application/pdf")).thenReturn(true);
        when(textExtractor.extract("node-9", "application/pdf")).thenReturn(ExtractedText.plain("Quarterly"));
        when(chunkingService.chunk(any(), any(), any())).thenReturn(List.of());

        service.processContent("hxpr-doc-9", Map.of(), "node-9",
                "application/pdf", "quarterly-report.pdf", "/quarterly-report.pdf");

        // A PDF is exactly what the extractor chain is for, so the policy must be invisible to it.
        verify(textExtractor).extract("node-9", "application/pdf");
        assertThat(service.getNonTextSkipCount()).isZero();
    }

    @Test
    void aPolicyThatDeniesNothingRestoresThePreviousBehaviour() {
        NodeSyncService permissive = new NodeSyncService(
                sourceClient, documentApi, hxprService, textExtractor, embeddingService, chunkingService,
                "/sharepoint-sync", null, false, true, NonTextContentPolicy.allowEverything());

        permissive.processContent("hxpr-doc-1", Map.of(), "node-1",
                "application/octet-stream", "a.csig", "/a.csig");

        // It reaches the extractor chain, which declines it, exactly as before this policy existed.
        verify(textExtractor).supportsSourceReference("application/octet-stream");
        assertThat(permissive.getNonTextSkipCount()).isZero();
    }
}
