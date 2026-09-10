package org.hyland.contentlake.service;

import org.hyland.contentlake.client.HxprDocumentApi;
import org.hyland.contentlake.client.HxprService;
import org.hyland.contentlake.model.ContentLakeIngestProperties;
import org.hyland.contentlake.model.HxprDocument;
import org.hyland.contentlake.service.chunking.SimpleChunkingService;
import org.hyland.contentlake.spi.ContentSourceClient;
import org.hyland.contentlake.spi.SourceTombstone;
import org.hyland.contentlake.spi.TextExtractor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.time.OffsetDateTime;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Covers the single delete implementation #115 introduces, and the boolean delegates the 11 existing
 * call sites still use.
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class NodeSyncServiceDeleteTest {

    private static final String NODE_ID = "node-1";
    private static final String DOC_ID = "hxpr-doc-1";
    private static final String SOURCE_ID = "alfresco:repo-uuid";

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
        service = new NodeSyncService(sourceClient, documentApi, hxprService, textExtractor,
                embeddingService, chunkingService, "/alfresco-sync", null, false, true);
        when(sourceClient.getSourceType()).thenReturn("alfresco");
        when(sourceClient.getSourceId()).thenReturn("repo-uuid");
    }

    @Test
    void clearsEmbeddingChildrenBeforeDeletingTheDocument() {
        // An embedding child left behind is still searchable, because the read path substitutes the
        // '*' embedding-type wildcard. hxpr's cascade on document delete is not contractual.
        when(hxprService.findByNodeId(NODE_ID, SOURCE_ID)).thenReturn(document(null));

        NodeSyncService.DeleteOutcome outcome = service.delete(SourceTombstone.deleted(NODE_ID));

        assertThat(outcome).isEqualTo(NodeSyncService.DeleteOutcome.DELETED);
        InOrder inOrder = inOrder(hxprService, documentApi);
        inOrder.verify(hxprService).deleteEmbeddings(DOC_ID);
        inOrder.verify(documentApi).deleteById(DOC_ID);
    }

    @Test
    void stillDeletesTheDocumentWhenClearingItsEmbeddingsFails() {
        // A surviving parent is the worse outcome: it is what search returns as a phantom result.
        when(hxprService.findByNodeId(NODE_ID, SOURCE_ID)).thenReturn(document(null));
        doThrow(new RuntimeException("hxpr unavailable")).when(hxprService).deleteEmbeddings(DOC_ID);

        NodeSyncService.DeleteOutcome outcome = service.delete(SourceTombstone.deleted(NODE_ID));

        assertThat(outcome).isEqualTo(NodeSyncService.DeleteOutcome.DELETED);
        verify(documentApi).deleteById(DOC_ID);
    }

    @Test
    void reportsNotFoundWhenNoDocumentExists() {
        when(hxprService.findByNodeId(NODE_ID, SOURCE_ID)).thenReturn(null);

        assertThat(service.delete(SourceTombstone.deleted(NODE_ID)))
                .isEqualTo(NodeSyncService.DeleteOutcome.NOT_FOUND);
        verify(documentApi, never()).deleteById(org.mockito.ArgumentMatchers.anyString());
    }

    @Test
    void preservesTheStalenessGuard() {
        // A delete event that arrives after a newer sync must not undo it.
        when(hxprService.findByNodeId(NODE_ID, SOURCE_ID))
                .thenReturn(document("2026-09-02T10:00:00Z"));

        NodeSyncService.DeleteOutcome outcome = service.delete(new SourceTombstone(
                NODE_ID, OffsetDateTime.parse("2026-09-01T10:00:00Z"), SourceTombstone.Reason.DELETED));

        assertThat(outcome).isEqualTo(NodeSyncService.DeleteOutcome.SKIPPED_NEWER);
        verify(documentApi, never()).deleteById(org.mockito.ArgumentMatchers.anyString());
    }

    @Test
    void deletesWhenTheEventIsNewerThanTheStoredVersion() {
        when(hxprService.findByNodeId(NODE_ID, SOURCE_ID))
                .thenReturn(document("2026-09-01T10:00:00Z"));

        NodeSyncService.DeleteOutcome outcome = service.delete(new SourceTombstone(
                NODE_ID, OffsetDateTime.parse("2026-09-02T10:00:00Z"), SourceTombstone.Reason.DELETED));

        assertThat(outcome).isEqualTo(NodeSyncService.DeleteOutcome.DELETED);
    }

    @Test
    void aReconcileTombstoneHasNoTimestamp_soTheStalenessGuardDoesNotApply() {
        // A sweep is not reacting to an event, so there is no event time to compare against. The
        // guard's purpose is ordering events, not vetoing a sweep that observed the source directly.
        when(hxprService.findByNodeId(NODE_ID, SOURCE_ID))
                .thenReturn(document("2099-01-01T00:00:00Z"));

        assertThat(service.delete(SourceTombstone.missingAtReconcile(NODE_ID)))
                .isEqualTo(NodeSyncService.DeleteOutcome.DELETED);
    }

    @Test
    void reportsFailedRatherThanThrowing_soASweepCanContinue() {
        when(hxprService.findByNodeId(NODE_ID, SOURCE_ID)).thenReturn(document(null));
        doThrow(new RuntimeException("boom")).when(documentApi).deleteById(DOC_ID);

        assertThat(service.delete(SourceTombstone.deleted(NODE_ID)))
                .isEqualTo(NodeSyncService.DeleteOutcome.FAILED);
    }

    @Test
    void theBooleanDelegateStillReturnsWhatTheExistingCallSitesExpect() {
        when(hxprService.findByNodeId(NODE_ID, SOURCE_ID)).thenReturn(document(null));
        assertThat(service.deleteNode(NODE_ID)).isTrue();

        when(hxprService.findByNodeId(NODE_ID, SOURCE_ID)).thenReturn(null);
        assertThat(service.deleteNode(NODE_ID)).isFalse();
    }

    @Test
    void theBooleanDelegateReportsFalseForAStaleEvent() {
        when(hxprService.findByNodeId(NODE_ID, SOURCE_ID))
                .thenReturn(document("2026-09-02T10:00:00Z"));

        assertThat(service.deleteNode(NODE_ID, OffsetDateTime.parse("2026-09-01T10:00:00Z"))).isFalse();
    }

    @Test
    void contentLakePathPrefix_mapsASourcePathToTheIndexedPath() {
        // cin_paths holds <hxprTargetPath>/<repositoryId><sourcePath>, not the raw source path. A
        // scope predicate written against the source path matches nothing and deletes nothing.
        assertThat(service.contentLakePathPrefix("repo-uuid", "/Sites/marketing"))
                .isEqualTo("/alfresco-sync/repo-uuid/Sites/marketing");
    }

    @Test
    void contentLakePathPrefix_collapsesToTheRepositoryRootForABlankPath() {
        assertThat(service.contentLakePathPrefix("repo-uuid", null))
                .isEqualTo("/alfresco-sync/repo-uuid");
        assertThat(service.contentLakePathPrefix("repo-uuid", "  "))
                .isEqualTo("/alfresco-sync/repo-uuid");
    }

    @Test
    void contentLakePathPrefix_normalizesARelativeOrTrailingSlashPath() {
        assertThat(service.contentLakePathPrefix("repo-uuid", "Sites/marketing/"))
                .isEqualTo("/alfresco-sync/repo-uuid/Sites/marketing");
    }

    private static HxprDocument document(String sourceModifiedAt) {
        HxprDocument doc = new HxprDocument();
        doc.setSysId(DOC_ID);
        if (sourceModifiedAt != null) {
            doc.setCinIngestProperties(Map.of(
                    ContentLakeIngestProperties.SOURCE_MODIFIED_AT, sourceModifiedAt));
        }
        return doc;
    }
}
