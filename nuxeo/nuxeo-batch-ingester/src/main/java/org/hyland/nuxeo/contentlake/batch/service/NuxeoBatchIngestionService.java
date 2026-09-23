package org.hyland.nuxeo.contentlake.batch.service;

import lombok.extern.slf4j.Slf4j;
import org.hyland.nuxeo.contentlake.batch.config.NuxeoBatchProperties;
import org.hyland.nuxeo.contentlake.batch.model.IngestionJob;
import org.hyland.nuxeo.contentlake.batch.model.NuxeoSyncRequest;
import org.hyland.nuxeo.contentlake.client.NuxeoClient;
import org.hyland.nuxeo.contentlake.config.NuxeoProperties;
import org.hyland.contentlake.service.DiscoveryOutcome;
import org.hyland.contentlake.service.IndexReconciliationService;
import org.hyland.contentlake.service.NodeSyncService;
import org.hyland.contentlake.service.ReconcileConfig;
import org.hyland.contentlake.service.SeenSet;
import org.hyland.contentlake.spi.SourceNode;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Supplier;

@Slf4j
@Service
public class NuxeoBatchIngestionService {

    private static final int MAX_RETAINED_JOBS = 100;

    private final NuxeoDiscoveryService discoveryService;
    private final NodeSyncService nodeSyncService;
    private final Executor batchExecutor;
    private final IndexReconciliationService reconciliationService;
    private final NuxeoClient nuxeoClient;
    private final NuxeoProperties nuxeoProps;
    private final NuxeoBatchProperties batchProps;
    private final Map<String, IngestionJob> jobsById = Collections.synchronizedMap(
            new LinkedHashMap<>(MAX_RETAINED_JOBS, 0.75f, false) {
                @Override
                protected boolean removeEldestEntry(Map.Entry<String, IngestionJob> eldest) {
                    return size() > MAX_RETAINED_JOBS;
                }
            }
    );

    public NuxeoBatchIngestionService(NuxeoDiscoveryService discoveryService,
                                      NodeSyncService nodeSyncService,
                                      @Qualifier("nuxeoBatchIngestionExecutor") Executor batchExecutor,
                                      IndexReconciliationService reconciliationService,
                                      NuxeoClient nuxeoClient,
                                      NuxeoProperties nuxeoProps,
                                      NuxeoBatchProperties batchProps) {
        this.discoveryService = discoveryService;
        this.nodeSyncService = nodeSyncService;
        this.batchExecutor = batchExecutor;
        this.reconciliationService = reconciliationService;
        this.nuxeoClient = nuxeoClient;
        this.nuxeoProps = nuxeoProps;
        this.batchProps = batchProps;
    }

    public IngestionJob startConfiguredSync() {
        IngestionJob job = createJob("configured sync");
        CompletableFuture.runAsync(
                () -> runJob(job, "configured sync", discoveryService::discoverFromConfigTallied), batchExecutor)
                .whenComplete((ignored, thrown) -> failIfEscaped(job, thrown));
        return job;
    }

    public IngestionJob startBatchSync(NuxeoSyncRequest request) {
        IngestionJob job = createJob("batch sync");
        CompletableFuture.runAsync(
                () -> runJob(job, "batch sync", () -> discoveryService.discoverTallied(request)), batchExecutor)
                .whenComplete((ignored, thrown) -> failIfEscaped(job, thrown));
        return job;
    }

    /**
     * The last line of defence for a job whose task did not finish it.
     *
     * <p>A future nobody observes captures whatever escaped its task and discards it, so a throwable that
     * missed the catch in {@link #runJob} left the job {@code RUNNING} with {@code completedAt} null, for ever,
     * and printed nothing at all. A caller polling for a terminal status waited for ever, and a stalled job was
     * indistinguishable from a slow one. This is the only place that sees such a throwable.</p>
     *
     * <p>{@code thrown} is non-null only when something escaped the task, so this cannot overwrite the status
     * of a job that finished. It can run twice for one throwable, because {@link #runJob} marks an
     * {@code Error} and rethrows it; both calls write the same terminal status.</p>
     */
    private void failIfEscaped(IngestionJob job, Throwable thrown) {
        if (thrown == null) {
            return;
        }
        job.fail();
        log.error("Nuxeo job {} ended on a throwable its task did not handle", job.getJobId(), thrown);
    }

    public IngestionJob getJob(String jobId) {
        return jobsById.get(jobId);
    }

    public Map<String, IngestionJob> getAllJobs() {
        synchronized (jobsById) {
            return Collections.unmodifiableMap(new LinkedHashMap<>(jobsById));
        }
    }

    private IngestionJob createJob(String label) {
        String jobId = UUID.randomUUID().toString();
        IngestionJob job = new IngestionJob(jobId);
        jobsById.put(jobId, job);
        log.info("Starting Nuxeo {} job {}", label, jobId);
        return job;
    }

    private void runJob(IngestionJob job, String label,
                        Supplier<NuxeoDiscoveryService.NuxeoDiscovery> discovery) {
        try {
            ReconcileConfig config = batchProps.getReconcile().toConfig();
            SeenSet seen = new SeenSet(config.maxSeenIds());

            NuxeoDiscoveryService.NuxeoDiscovery discovered = discovery.get();
            discovered.nodes().forEach(node -> syncNode(node, job, seen));

            job.complete();
            log.info("Nuxeo sync job {} completed. Discovered: {}, Synced: {}, Skipped: {}, Failed: {}",
                    job.getJobId(),
                    job.getDiscoveredCountValue(),
                    job.getSyncedCountValue(),
                    job.getSkippedCountValue(),
                    job.getFailedCountValue());

            sweep(job, discovered.outcome(), seen, config);
        } catch (Throwable t) {
            // Throwable rather than Exception: one unreadable file threw something outside Exception from the
            // extraction chain, and the job stayed RUNNING for ever because nothing here caught it. Marking the
            // job here is what keeps the job id and the label in the log line; an Error is rethrown afterwards,
            // because swallowing one would hide a JVM-level problem from the executor. The sweep catches its own
            // throwables, so this cannot turn a completed ingestion into a failed job.
            job.fail();
            log.error("Nuxeo {} job {} failed", label, job.getJobId(), t);
            if (t instanceof Error error) {
                throw error;
            }
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
                    .map(path -> nodeSyncService.contentLakePathPrefix(nuxeoClient.getSourceId(), path))
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
        } catch (Throwable t) {
            // Throwable, and deliberately not rethrown, so the invariant above survives the widening of the
            // caller's catch: the job is already COMPLETED with correct ingestion counters, and letting a sweep
            // throwable reach the caller would flip it to FAILED. Logged rather than swallowed, which is the
            // whole point -- nothing here may end without a log line.
            log.error("Reconciliation sweep for job {} failed; ingestion is unaffected", job.getJobId(), t);
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
                    metadata.documentPath()
            );
            job.incrementSynced();
        } catch (Exception e) {
            job.incrementFailed();
            log.error("Failed to sync Nuxeo node {}", node.nodeId(), e);
        }
    }
}
