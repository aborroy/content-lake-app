package org.hyland.nuxeo.contentlake.batch.service;

import org.hyland.nuxeo.contentlake.batch.model.IngestionJob;
import org.hyland.nuxeo.contentlake.batch.model.NuxeoSyncRequest;
import org.hyland.contentlake.service.DiscoveryOutcome;
import org.hyland.contentlake.service.IndexReconciliationService;
import org.hyland.contentlake.service.NodeSyncService;
import org.hyland.nuxeo.contentlake.batch.config.NuxeoBatchProperties;
import org.hyland.nuxeo.contentlake.client.NuxeoClient;
import org.hyland.nuxeo.contentlake.config.NuxeoProperties;
import org.hyland.contentlake.spi.SourceNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class NuxeoBatchIngestionServiceTest {

    @Mock
    private NuxeoDiscoveryService discoveryService;

    @Mock
    private NodeSyncService nodeSyncService;

    @Mock
    private IndexReconciliationService reconciliationService;

    @Mock
    private NuxeoClient nuxeoClient;

    private NuxeoBatchIngestionService service;

    // Synchronous executor so tests can assert on state after the call returns.
    private final Executor syncExecutor = Runnable::run;

    @BeforeEach
    void setUp() {
        // Reconciliation is off by default (NuxeoBatchProperties.reconcile.enabled), so the sweep
        // never runs in these cases and its collaborators are unused.
        service = new NuxeoBatchIngestionService(discoveryService, nodeSyncService, syncExecutor,
                reconciliationService, nuxeoClient, new NuxeoProperties(), new NuxeoBatchProperties());
    }

    @Test
    void startConfiguredSync_completesWithCorrectCounts() throws Exception {
        SourceNode node = fileNode("doc-1");
        when(discoveryService.discoverFromConfigTallied()).thenReturn(discovery(node));
        NodeSyncService.SyncResult syncResult = new NodeSyncService.SyncResult(
                "hxpr-doc-1", "doc-1", "application/pdf", "Doc 1", "/nuxeo-sync", false, Map.of());
        when(nodeSyncService.ingestMetadata(node)).thenReturn(syncResult);

        IngestionJob job = service.startConfiguredSync();

        assertThat(job.getStatus()).isEqualTo(IngestionJob.JobStatus.COMPLETED);
        assertThat(job.getDiscoveredCountValue()).isEqualTo(1);
        assertThat(job.getSyncedCountValue()).isEqualTo(1);
        assertThat(job.getSkippedCountValue()).isEqualTo(0);
        assertThat(job.getFailedCountValue()).isEqualTo(0);
        assertThat(job.getCompletedAt()).isNotNull();
    }

    @Test
    void startConfiguredSync_skippedNodeIncrementSkipCount() throws Exception {
        SourceNode node = fileNode("doc-2");
        when(discoveryService.discoverFromConfigTallied()).thenReturn(discovery(node));
        NodeSyncService.SyncResult skipped = new NodeSyncService.SyncResult(
                null, null, null, null, null, true, null);
        when(nodeSyncService.ingestMetadata(node)).thenReturn(skipped);

        IngestionJob job = service.startConfiguredSync();

        assertThat(job.getStatus()).isEqualTo(IngestionJob.JobStatus.COMPLETED);
        assertThat(job.getSkippedCountValue()).isEqualTo(1);
        assertThat(job.getSyncedCountValue()).isEqualTo(0);
        verify(nodeSyncService, never()).processContent(any(), any(), any(), any(), any(), any());
    }

    @Test
    void startConfiguredSync_failedNodeIncrementFailedCount() throws Exception {
        SourceNode node = fileNode("doc-3");
        when(discoveryService.discoverFromConfigTallied()).thenReturn(discovery(node));
        when(nodeSyncService.ingestMetadata(node)).thenThrow(new RuntimeException("HXPR unreachable"));

        IngestionJob job = service.startConfiguredSync();

        assertThat(job.getStatus()).isEqualTo(IngestionJob.JobStatus.COMPLETED);
        assertThat(job.getFailedCountValue()).isEqualTo(1);
        assertThat(job.getSyncedCountValue()).isEqualTo(0);
    }

    @Test
    void startConfiguredSync_discoveryFailureSetsJobFailed() {
        when(discoveryService.discoverFromConfigTallied()).thenThrow(new RuntimeException("Nuxeo unavailable"));

        IngestionJob job = service.startConfiguredSync();

        assertThat(job.getStatus()).isEqualTo(IngestionJob.JobStatus.FAILED);
        assertThat(job.getCompletedAt()).isNotNull();
    }

    @Test
    void getJob_returnsNullForUnknownId() {
        assertThat(service.getJob("nonexistent")).isNull();
    }

    @Test
    void getJob_returnsJobAfterStart() {
        when(discoveryService.discoverFromConfigTallied()).thenReturn(discovery());

        IngestionJob job = service.startConfiguredSync();

        assertThat(service.getJob(job.getJobId())).isSameAs(job);
    }

    @Test
    void getAllJobs_returnsUnmodifiableSnapshot() {
        when(discoveryService.discoverFromConfigTallied()).thenReturn(discovery());

        IngestionJob firstJob = service.startConfiguredSync();
        Map<String, IngestionJob> snapshot = service.getAllJobs();
        IngestionJob secondJob = service.startConfiguredSync();

        assertThat(snapshot).containsOnlyKeys(firstJob.getJobId());
        assertThat(snapshot).doesNotContainKey(secondJob.getJobId());
        assertThatThrownBy(snapshot::clear).isInstanceOf(UnsupportedOperationException.class);
        assertThat(service.getAllJobs()).containsKeys(firstJob.getJobId(), secondJob.getJobId());
    }

    @Test
    void startBatchSync_usesRequestParameters() throws Exception {
        NuxeoSyncRequest request = new NuxeoSyncRequest();
        SourceNode node = fileNode("doc-4");
        when(discoveryService.discoverTallied(request)).thenReturn(discovery(node));
        NodeSyncService.SyncResult syncResult = new NodeSyncService.SyncResult(
                "hxpr-doc-4", "doc-4", "text/plain", "Doc 4", "/nuxeo-sync", false, Map.of());
        when(nodeSyncService.ingestMetadata(node)).thenReturn(syncResult);

        IngestionJob job = service.startBatchSync(request);

        assertThat(job.getStatus()).isEqualTo(IngestionJob.JobStatus.COMPLETED);
        assertThat(job.getSyncedCountValue()).isEqualTo(1);
        verify(discoveryService).discoverTallied(request);
    }

    /**
     * A job whose task throws an {@code Error} ends {@code FAILED}, not {@code RUNNING} for ever (#153).
     *
     * <p>{@code catch (Exception)} does not catch an {@code Error}, and the future the task ran in was
     * discarded, so such a throwable was captured by nobody: the job kept {@code status: RUNNING} and
     * {@code completedAt: null} indefinitely and printed nothing at all. A caller polling for a terminal status
     * waited for ever. That is what stopped a real crawl at 51 of 52 documents.</p>
     */
    @Test
    void startConfiguredSync_marksTheJobFailedWhenTheTaskThrowsAnError() {
        when(discoveryService.discoverFromConfigTallied())
                .thenThrow(new StackOverflowError("thrown from the extraction chain"));

        IngestionJob job = service.startConfiguredSync();

        assertThat(job.getStatus()).isEqualTo(IngestionJob.JobStatus.FAILED);
        assertThat(job.getCompletedAt()).isNotNull();
    }

    /** The same for a {@code Throwable} that is neither an {@code Exception} nor an {@code Error}. */
    @Test
    void startBatchSync_marksTheJobFailedWhenTheTaskThrowsAThrowableThatIsNeither() {
        NuxeoSyncRequest request = new NuxeoSyncRequest();
        when(discoveryService.discoverTallied(request)).thenAnswer(invocation -> {
            throw new NeitherExceptionNorError();
        });

        IngestionJob job = service.startBatchSync(request);

        assertThat(job.getStatus()).isEqualTo(IngestionJob.JobStatus.FAILED);
        assertThat(job.getCompletedAt()).isNotNull();
    }

    /** Extends Throwable directly, which is the case neither {@code catch} clause used to reach. */
    private static final class NeitherExceptionNorError extends Throwable {
    }

    private static NuxeoDiscoveryService.NuxeoDiscovery discovery(SourceNode... nodes) {
        return new NuxeoDiscoveryService.NuxeoDiscovery(List.of(nodes),
                DiscoveryOutcome.complete(List.of("/default-domain/workspaces/finance")));
    }

    private static SourceNode fileNode(String nodeId) {
        return new SourceNode(
                nodeId, "nuxeo-dev", "nuxeo", "Document",
                "/default-domain/workspaces/finance",
                "application/pdf",
                OffsetDateTime.parse("2026-03-24T10:00:00Z"),
                false, Set.of(), Set.of(),
                Map.of("nuxeo_path", "/default-domain/workspaces/finance/" + nodeId + ".pdf",
                        "nuxeo_documentType", "File",
                        "nuxeo_lifecycleState", "project")
        );
    }
}
