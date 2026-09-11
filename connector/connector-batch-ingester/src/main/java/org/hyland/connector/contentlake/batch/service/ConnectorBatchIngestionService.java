package org.hyland.connector.contentlake.batch.service;

import lombok.extern.slf4j.Slf4j;
import org.hyland.connector.contentlake.batch.config.ConnectorBatchProperties;
import org.hyland.connector.contentlake.batch.config.SelectedConnector;
import org.hyland.connector.contentlake.batch.model.IngestionJob;
import org.hyland.contentlake.service.DiscoveryOutcome;
import org.hyland.contentlake.service.IndexReconciliationService;
import org.hyland.contentlake.service.NodeSyncService;
import org.hyland.contentlake.service.ReconcileConfig;
import org.hyland.contentlake.service.SeenSet;
import org.hyland.contentlake.spi.SourceNode;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * Runs and tracks batch jobs over the connector this host loaded.
 *
 * <p>Identical in shape to the other batch ingestion services: discover, sync each node through
 * {@code NodeSyncService}, then sweep. Everything source-specific it needs comes from
 * {@link SelectedConnector}.</p>
 */
@Slf4j
public class ConnectorBatchIngestionService {

    private static final int MAX_RETAINED_JOBS = 100;

    private final ConnectorDiscoveryService discoveryService;
    private final NodeSyncService nodeSyncService;
    private final Executor batchExecutor;
    private final IndexReconciliationService reconciliationService;
    private final SelectedConnector connector;
    private final ConnectorBatchProperties props;
    private final Map<String, IngestionJob> jobsById = Collections.synchronizedMap(
            new LinkedHashMap<>(MAX_RETAINED_JOBS, 0.75f, false) {
                @Override
                protected boolean removeEldestEntry(Map.Entry<String, IngestionJob> eldest) {
                    return size() > MAX_RETAINED_JOBS;
                }
            });

    public ConnectorBatchIngestionService(ConnectorDiscoveryService discoveryService,
                                         NodeSyncService nodeSyncService,
                                         Executor batchExecutor,
                                         IndexReconciliationService reconciliationService,
                                         SelectedConnector connector,
                                         ConnectorBatchProperties props) {
        this.discoveryService = discoveryService;
        this.nodeSyncService = nodeSyncService;
        this.batchExecutor = batchExecutor;
        this.reconciliationService = reconciliationService;
        this.connector = connector;
        this.props = props;
    }

    public IngestionJob startConfiguredSync() {
        IngestionJob job = createJob();
        CompletableFuture.runAsync(() -> runJob(job), batchExecutor);
        return job;
    }

    public IngestionJob getJob(String jobId) {
        return jobsById.get(jobId);
    }

    public Map<String, IngestionJob> getAllJobs() {
        synchronized (jobsById) {
            return Collections.unmodifiableMap(new LinkedHashMap<>(jobsById));
        }
    }

    private IngestionJob createJob() {
        String jobId = UUID.randomUUID().toString();
        IngestionJob job = new IngestionJob(jobId, connector.sourceType());
        jobsById.put(jobId, job);
        log.info("Starting connector batch job {} for '{}' (from {})",
                jobId, connector.sourceType(), connector.origin());
        return job;
    }

    private void runJob(IngestionJob job) {
        try {
            ReconcileConfig config = props.getReconcile().toConfig();
            SeenSet seen = new SeenSet(config.maxSeenIds());

            ConnectorDiscoveryService.ConnectorDiscovery discovered = discoveryService.discoverTallied();
            discovered.nodes().forEach(node -> syncNode(node, job, seen));

            job.complete();
            log.info("Connector sync job {} completed. Discovered: {}, Synced: {}, Skipped: {}, Failed: {}",
                    job.getJobId(),
                    job.getDiscoveredCountValue(),
                    job.getSyncedCountValue(),
                    job.getSkippedCountValue(),
                    job.getFailedCountValue());

            sweep(job, discovered.outcome(), seen, config);
        } catch (Exception e) {
            job.fail();
            log.error("Connector batch job {} failed", job.getJobId(), e);
        }
    }

    /**
     * Deletes indexed documents this sync's discovery did not see.
     *
     * <p>Runs after {@code job.complete()}: the sweep is a distinct optional phase and a sweep failure
     * must not turn a successful ingestion into a failed job.</p>
     */
    private void sweep(IngestionJob job, DiscoveryOutcome outcome, SeenSet seen, ReconcileConfig config) {
        if (!config.enabled()) {
            return;
        }
        try {
            List<String> prefixes = outcome.resolvedRootPaths().stream()
                    .map(path -> nodeSyncService.contentLakePathPrefix(
                            connector.client().getSourceId(), path))
                    .toList();

            if (prefixes.isEmpty()) {
                // No resolved scope means the predicate would match nothing, which reconcile() would
                // then report as "index matches the source" -- a silent no-op dressed as success.
                log.warn("Reconciliation skipped for job {}: no scope path could be resolved from "
                        + "discovery, so the sweep has nothing it can safely own.", job.getJobId());
                return;
            }

            job.recordReconciliation(reconciliationService.reconcile(
                    seen, outcome, job.getFailedCountValue(),
                    IndexReconciliationService.underAnyPath(prefixes), config));
        } catch (Exception e) {
            log.error("Reconciliation sweep for job {} failed; ingestion is unaffected", job.getJobId(), e);
        }
    }

    private void syncNode(SourceNode node, IngestionJob job, SeenSet seen) {
        job.incrementDiscovered();
        // Recorded before the sync attempt: the set means "the source has this node", which holds
        // whether or not syncing it succeeded. A failed node blocks the sweep separately.
        seen.add(node.nodeId());
        try {
            NodeSyncService.SyncResult metadata = nodeSyncService.ingestMetadata(node);
            if (metadata.skipped()) {
                job.incrementSkipped();
                return;
            }
            nodeSyncService.processContent(
                    metadata.hxprDocId(),
                    metadata.ingestProperties(),
                    metadata.nodeId(),
                    metadata.mimeType(),
                    metadata.documentName(),
                    metadata.documentPath());
            job.incrementSynced();
        } catch (Exception e) {
            job.incrementFailed();
            log.error("Failed to sync {} node {}", connector.sourceType(), node.nodeId(), e);
        }
    }
}
