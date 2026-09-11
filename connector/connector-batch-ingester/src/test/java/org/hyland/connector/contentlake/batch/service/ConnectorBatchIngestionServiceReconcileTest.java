package org.hyland.connector.contentlake.batch.service;

import org.hyland.connector.contentlake.batch.config.ConnectorBatchProperties;
import org.hyland.connector.contentlake.batch.config.SelectedConnector;
import org.hyland.connector.contentlake.batch.model.IngestionJob;
import org.hyland.contentlake.model.HxprDocument;
import org.hyland.contentlake.service.DiscoveryOutcome;
import org.hyland.contentlake.service.IndexReconciliationService;
import org.hyland.contentlake.service.NodeSyncService;
import org.hyland.contentlake.service.SeenSet;
import org.hyland.contentlake.spi.ContentSourceClient;
import org.hyland.contentlake.spi.ScopeResolver;
import org.hyland.contentlake.spi.SourceNode;
import org.hyland.contentlake.spi.TextExtractor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.function.Predicate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Reconciliation over a plugin connector (#132).
 *
 * <p>Same guards as the other batch ingesters, plus the one this host adds: an incomplete pass must not
 * delete. That matters more here than elsewhere, because a container this walk cannot list is contained
 * rather than fatal, so "some of the source was not enumerated" is a routine outcome rather than an
 * exceptional one.</p>
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class ConnectorBatchIngestionServiceReconcileTest {

    private static final String ROOT_PATH = "/Sites/marketing";
    private static final String INDEXED_PREFIX = "/connector-sync/cmis-instance" + ROOT_PATH;

    @Mock
    private ConnectorDiscoveryService discoveryService;
    @Mock
    private NodeSyncService nodeSyncService;
    @Mock
    private IndexReconciliationService reconciliationService;
    @Mock
    private ContentSourceClient sourceClient;
    @Mock
    private ScopeResolver scopeResolver;
    @Mock
    private TextExtractor textExtractor;

    private ConnectorBatchProperties props;
    private ConnectorBatchIngestionService service;

    /** Synchronous, so a test can assert on job state once the call returns. */
    private final Executor syncExecutor = Runnable::run;

    @BeforeEach
    void setUp() {
        props = new ConnectorBatchProperties();
        SelectedConnector connector = new SelectedConnector("cmis", "cmis connector", "cmis.jar",
                sourceClient, scopeResolver, textExtractor, false);
        service = new ConnectorBatchIngestionService(discoveryService, nodeSyncService, syncExecutor,
                reconciliationService, connector, props);

        when(sourceClient.getSourceId()).thenReturn("cmis-instance");
        when(nodeSyncService.contentLakePathPrefix(eq("cmis-instance"), any())).thenReturn(INDEXED_PREFIX);
        when(nodeSyncService.ingestMetadata(any())).thenReturn(new NodeSyncService.SyncResult(
                "hxpr-1", "node-1", "text/plain", "a.txt", ROOT_PATH, false, Map.of()));
    }

    /**
     * The application default, and set more cautiously than for the filesystem source: the sweep's scope
     * comes from paths a connector reported, which the host cannot sanity-check.
     */
    @Test
    void doesNotSweepWhenReconciliationIsDisabled() {
        assertThat(props.getReconcile().isEnabled()).isFalse();
        when(discoveryService.discoverTallied()).thenReturn(complete(node("node-1")));

        IngestionJob job = service.startConfiguredSync();

        assertThat(job.getStatus()).isEqualTo(IngestionJob.JobStatus.COMPLETED);
        verify(reconciliationService, never()).reconcile(any(), any(), anyInt(), any(), any());
        assertThat(job.getReconciliation()).isNull();
    }

    @Test
    void sweepsWithTheDiscoveredIdsWhenEnabled() {
        props.getReconcile().setEnabled(true);
        when(discoveryService.discoverTallied()).thenReturn(complete(node("node-1"), node("node-2")));
        when(reconciliationService.reconcile(any(), any(), anyInt(), any(), any())).thenReturn(report(1));

        IngestionJob job = service.startConfiguredSync();

        ArgumentCaptor<SeenSet> seenCaptor = ArgumentCaptor.forClass(SeenSet.class);
        verify(reconciliationService).reconcile(seenCaptor.capture(), any(), anyInt(), any(), any());

        SeenSet seen = seenCaptor.getValue();
        assertThat(seen.contains("node-1")).isTrue();
        assertThat(seen.contains("node-2")).isTrue();
        assertThat(seen.contains("node-never-existed")).isFalse();
        assertThat(job.getReconciliation().deleted()).isEqualTo(1);
    }

    /**
     * The outcome, not just the ids, reaches the sweep. A container this walk could not list produces an
     * incomplete pass, and deleting on one would remove documents that are still in the source.
     */
    @Test
    void passesAnIncompleteOutcomeThroughSoTheSweepCanRefuse() {
        props.getReconcile().setEnabled(true);
        when(discoveryService.discoverTallied()).thenReturn(
                new ConnectorDiscoveryService.ConnectorDiscovery(
                        List.of(node("node-1")),
                        DiscoveryOutcome.incomplete(List.of(ROOT_PATH),
                                List.of("Listing container 'locked' failed: denied"))));
        when(reconciliationService.reconcile(any(), any(), anyInt(), any(), any())).thenReturn(report(0));

        service.startConfiguredSync();

        ArgumentCaptor<DiscoveryOutcome> outcomeCaptor = ArgumentCaptor.forClass(DiscoveryOutcome.class);
        verify(reconciliationService).reconcile(any(), outcomeCaptor.capture(), anyInt(), any(), any());
        assertThat(outcomeCaptor.getValue().complete()).isFalse();
    }

    @Test
    void passesTheNodeFailureCountThrough_soAFailedNodeBlocksTheSweep() {
        props.getReconcile().setEnabled(true);
        when(discoveryService.discoverTallied()).thenReturn(complete(node("node-1")));
        when(nodeSyncService.ingestMetadata(any())).thenThrow(new RuntimeException("read failed"));
        when(reconciliationService.reconcile(any(), any(), anyInt(), any(), any())).thenReturn(report(0));

        service.startConfiguredSync();

        verify(reconciliationService).reconcile(any(), any(), eq(1), any(), any());
    }

    /** cin_paths holds the hxpr path, so a predicate built from the raw source path would match nothing. */
    @Test
    void scopesTheSweepToTheIndexedPathPrefix() {
        props.getReconcile().setEnabled(true);
        when(discoveryService.discoverTallied()).thenReturn(complete(node("node-1")));
        when(reconciliationService.reconcile(any(), any(), anyInt(), any(), any())).thenReturn(report(0));

        service.startConfiguredSync();

        verify(nodeSyncService).contentLakePathPrefix("cmis-instance", ROOT_PATH);
        @SuppressWarnings("unchecked")
        ArgumentCaptor<Predicate<HxprDocument>> predicateCaptor = ArgumentCaptor.forClass(Predicate.class);
        verify(reconciliationService).reconcile(any(), any(), anyInt(), predicateCaptor.capture(), any());

        HxprDocument indexed = new HxprDocument();
        indexed.setCinPaths(List.of(INDEXED_PREFIX + "/a.txt"));
        assertThat(predicateCaptor.getValue().test(indexed)).isTrue();

        HxprDocument elsewhere = new HxprDocument();
        elsewhere.setCinPaths(List.of("/alfresco-sync/repo/Sites/x.txt"));
        assertThat(predicateCaptor.getValue().test(elsewhere)).isFalse();
    }

    /**
     * A pass with no resolved root would produce an empty predicate list, which the sweep would report as
     * "index matches the source": a no-op dressed as success.
     */
    @Test
    void skipsTheSweepWhenNoRootPathResolved() {
        props.getReconcile().setEnabled(true);
        when(discoveryService.discoverTallied()).thenReturn(
                new ConnectorDiscoveryService.ConnectorDiscovery(
                        List.of(node("node-1")), DiscoveryOutcome.complete(List.of())));

        IngestionJob job = service.startConfiguredSync();

        assertThat(job.getStatus()).isEqualTo(IngestionJob.JobStatus.COMPLETED);
        verify(reconciliationService, never()).reconcile(any(), any(), anyInt(), any(), any());
    }

    @Test
    void aSweepFailureDoesNotFailTheIngestion() {
        props.getReconcile().setEnabled(true);
        when(discoveryService.discoverTallied()).thenReturn(complete(node("node-1")));
        when(reconciliationService.reconcile(any(), any(), anyInt(), any(), any()))
                .thenThrow(new RuntimeException("hxpr unavailable"));

        IngestionJob job = service.startConfiguredSync();

        assertThat(job.getStatus()).isEqualTo(IngestionJob.JobStatus.COMPLETED);
        assertThat(job.getReconciliation()).isNull();
    }

    @Test
    void aDiscoveryFailureFailsTheJobAndSkipsTheSweepEntirely() {
        props.getReconcile().setEnabled(true);
        when(discoveryService.discoverTallied()).thenThrow(new RuntimeException("root unreadable"));

        IngestionJob job = service.startConfiguredSync();

        assertThat(job.getStatus()).isEqualTo(IngestionJob.JobStatus.FAILED);
        verify(reconciliationService, never()).reconcile(any(), any(), anyInt(), any(), any());
    }

    /** The job reports the connector's source type, since this host has none of its own. */
    @Test
    void theJobCarriesTheConnectorSourceType() {
        when(discoveryService.discoverTallied()).thenReturn(complete(node("node-1")));

        assertThat(service.startConfiguredSync().getSourceType()).isEqualTo("cmis");
    }

    @Test
    void sizesTheSeenSetFromConfiguration() {
        props.getReconcile().setEnabled(true);
        props.getReconcile().setMaxSeenIds(1);
        when(discoveryService.discoverTallied()).thenReturn(complete(node("node-1"), node("node-2")));
        when(reconciliationService.reconcile(any(), any(), anyInt(), any(), any())).thenReturn(report(0));

        service.startConfiguredSync();

        ArgumentCaptor<SeenSet> seenCaptor = ArgumentCaptor.forClass(SeenSet.class);
        verify(reconciliationService).reconcile(seenCaptor.capture(), any(), anyInt(), any(), any());
        assertThat(seenCaptor.getValue().overflowed()).isTrue();
    }

    private static IndexReconciliationService.Report report(int deleted) {
        return new IndexReconciliationService.Report(
                IndexReconciliationService.Status.COMPLETED, 2, 2, deleted, deleted, 0, 0.5, "ok");
    }

    private static ConnectorDiscoveryService.ConnectorDiscovery complete(SourceNode... nodes) {
        return new ConnectorDiscoveryService.ConnectorDiscovery(
                List.of(nodes), DiscoveryOutcome.complete(List.of(ROOT_PATH)));
    }

    private static SourceNode node(String nodeId) {
        return new SourceNode(nodeId, "cmis-instance", "cmis", nodeId + ".txt", ROOT_PATH, "text/plain",
                OffsetDateTime.parse("2026-09-01T10:00:00Z"), false,
                Set.of("__Everyone__"), Set.of(), Map.of());
    }
}
