package org.hyland.alfresco.contentlake.batch.service;

import lombok.extern.slf4j.Slf4j;
import org.hyland.alfresco.contentlake.batch.config.IngestionProperties;
import org.hyland.alfresco.contentlake.batch.model.BatchSyncRequest;
import org.hyland.alfresco.contentlake.batch.model.IngestionJob;
import org.hyland.alfresco.contentlake.batch.model.TransformationTask;
import org.hyland.alfresco.contentlake.client.AlfrescoClient;
import org.hyland.contentlake.service.DiscoveryOutcome;
import org.hyland.contentlake.service.IndexReconciliationService;
import org.hyland.contentlake.service.NodeSyncService;
import org.hyland.contentlake.service.ReconcileConfig;
import org.hyland.contentlake.service.SeenSet;
import org.alfresco.core.model.Node;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;

/**
 * Orchestrates asynchronous batch ingestion jobs and tracks their execution state.
 */
@Slf4j
@Service
public class BatchIngestionService {

    private final NodeDiscoveryService discoveryService;
    private final MetadataIngester metadataIngester;
    private final TransformationQueue transformationQueue;
    private final Executor batchIngestionExecutor;
    private final IndexReconciliationService reconciliationService;
    private final NodeSyncService nodeSyncService;
    private final AlfrescoClient alfrescoClient;
    private final IngestionProperties props;

    private final Map<String, IngestionJob> jobsById = new ConcurrentHashMap<>();

    /**
     * Coordinates batch ingestion: discover nodes, ingest metadata, and enqueue transformations.
     *
     * @param discoveryService service to discover nodes to ingest
     * @param metadataIngester component that ingests node metadata into hxpr
     * @param transformationQueue queue for transformation tasks
     * @param batchIngestionExecutor executor for asynchronous ingestion jobs
     * @param reconciliationService post-discovery sweep that removes documents the source no longer has
     * @param nodeSyncService used to map a source path to its indexed {@code cin_paths} prefix
     * @param alfrescoClient supplies the source id the sweep scans under
     * @param props reconciliation tunables
     */
    public BatchIngestionService(
            NodeDiscoveryService discoveryService,
            MetadataIngester metadataIngester,
            TransformationQueue transformationQueue,
            @Qualifier("batchIngestionExecutor") Executor batchIngestionExecutor,
            IndexReconciliationService reconciliationService,
            NodeSyncService nodeSyncService,
            AlfrescoClient alfrescoClient,
            IngestionProperties props
    ) {
        this.discoveryService = discoveryService;
        this.metadataIngester = metadataIngester;
        this.transformationQueue = transformationQueue;
        this.batchIngestionExecutor = batchIngestionExecutor;
        this.reconciliationService = reconciliationService;
        this.nodeSyncService = nodeSyncService;
        this.alfrescoClient = alfrescoClient;
        this.props = props;
    }

    /**
     * Starts an asynchronous batch synchronization job based on the given request.
     *
     * @param request discovery and filtering parameters
     * @return the created {@link IngestionJob}
     */
    public IngestionJob startBatchSync(BatchSyncRequest request) {
        IngestionJob job = createJob("batch sync");

        CompletableFuture.runAsync(() -> runBatchSync(job, request), batchIngestionExecutor)
                .whenComplete((ignored, thrown) -> failIfEscaped(job, thrown));

        return job;
    }

    /**
     * Starts an asynchronous synchronization job based on the application configuration.
     *
     * @return the created {@link IngestionJob}
     */
    public IngestionJob startConfiguredSync() {
        IngestionJob job = createJob("configured sync");

        CompletableFuture.runAsync(() -> runConfiguredSync(job), batchIngestionExecutor)
                .whenComplete((ignored, thrown) -> failIfEscaped(job, thrown));

        return job;
    }

    /**
     * The last line of defence for a job whose task did not finish it.
     *
     * <p>A future nobody observes captures whatever escaped its task and discards it, so a throwable that
     * missed the catch in the run method left the job {@code RUNNING} with {@code completedAt} null, for ever,
     * and printed nothing at all. A caller polling for a terminal status waited for ever, and a stalled job was
     * indistinguishable from a slow one. This is the only place that sees such a throwable.</p>
     *
     * <p>{@code thrown} is non-null only when something escaped the task, so this cannot overwrite the status
     * of a job that finished. It can run twice for one throwable, because the run methods mark an {@code Error}
     * and rethrow it; both calls write the same terminal status.</p>
     */
    private void failIfEscaped(IngestionJob job, Throwable thrown) {
        if (thrown == null) {
            return;
        }
        job.fail();
        log.error("Job {} ended on a throwable its task did not handle", job.getJobId(), thrown);
    }

    /**
     * Returns the job for the given identifier.
     *
     * @param jobId job identifier
     * @return the job, or {@code null} if not found
     */
    public IngestionJob getJob(String jobId) {
        return jobsById.get(jobId);
    }

    /**
     * Returns all known jobs keyed by identifier.
     *
     * @return map of jobs
     */
    public Map<String, IngestionJob> getAllJobs() {
        return jobsById;
    }

    private IngestionJob createJob(String label) {
        String jobId = UUID.randomUUID().toString();
        IngestionJob job = new IngestionJob(jobId);
        jobsById.put(jobId, job);

        log.info("Starting {} job: {}", label, jobId);
        return job;
    }

    private void runBatchSync(IngestionJob job, BatchSyncRequest request) {
        String jobId = job.getJobId();
        try {
            SeenSet seen = new SeenSet(reconcileConfig().maxSeenIds());
            NodeDiscoveryService.Discovery discovery = discoveryService.discoverNodesTallied(request);
            discovery.nodes().forEach(node -> ingestNode(node, job, seen));

            job.complete();
            log.info(
                    "Batch sync job {} completed. Discovered: {}, Ingested: {}, Failed: {}",
                    jobId,
                    job.getDiscoveredCount(),
                    job.getMetadataIngestedCount(),
                    job.getFailedCount()
            );

            sweep(job, discovery.tally(), seen);
        } catch (Throwable t) {
            // Throwable rather than Exception: one unreadable file threw something outside Exception from the
            // extraction chain, and the job stayed RUNNING for ever because nothing here caught it. Marking the
            // job here is what keeps the job id and the source context in the log line; an Error is rethrown
            // afterwards, because swallowing one would hide a JVM-level problem from the executor. The sweep
            // catches its own throwables, so this cannot turn a completed ingestion into a failed job.
            log.error("Batch sync job {} failed", jobId, t);
            job.fail();
            if (t instanceof Error error) {
                throw error;
            }
        }
    }

    private void runConfiguredSync(IngestionJob job) {
        String jobId = job.getJobId();
        try {
            SeenSet seen = new SeenSet(reconcileConfig().maxSeenIds());
            NodeDiscoveryService.Discovery discovery = discoveryService.discoverFromConfigTallied();
            discovery.nodes().forEach(node -> ingestNode(node, job, seen));

            job.complete();
            log.info("Configured sync job {} completed", jobId);

            sweep(job, discovery.tally(), seen);
        } catch (Throwable t) {
            // Throwable rather than Exception, for the reason given in runBatchSync.
            log.error("Configured sync job {} failed", jobId, t);
            job.fail();
            if (t instanceof Error error) {
                throw error;
            }
        }
    }

    /**
     * Deletes indexed documents this sync's discovery did not see.
     *
     * <p>Runs after {@code job.complete()} deliberately: the sweep is a distinct optional phase, and a
     * sweep failure must not turn a successful ingestion into a failed job. Its outcome lands on the
     * job so it is visible through the sync status API, and in the log.</p>
     *
     * <p>The tally is read here, once the lazy discovery stream has been drained, because its
     * silent-partial cases only become known as it is consumed.</p>
     */
    private void sweep(IngestionJob job, NodeDiscoveryService.DiscoveryTally tally, SeenSet seen) {
        ReconcileConfig config = reconcileConfig();
        if (!config.enabled()) {
            return;
        }
        try {
            DiscoveryOutcome outcome = tally.toOutcome();
            List<String> prefixes = tally.resolvedRootPaths().stream()
                    .map(path -> nodeSyncService.contentLakePathPrefix(alfrescoClient.getSourceId(), path))
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

    private ReconcileConfig reconcileConfig() {
        return props.getReconcile().toConfig();
    }

    private void ingestNode(Node node, IngestionJob job, SeenSet seen) {
        job.incrementDiscovered();
        // Recorded before the ingest attempt: the set means "the source has this node", which is true
        // whether or not ingesting it succeeded. A failed node blocks the sweep separately.
        seen.add(node.getId());
        try {
            TransformationTask task = metadataIngester.ingestMetadata(node);
            job.incrementMetadataIngested();
            if (task != null) {
                transformationQueue.enqueue(task);
            }
        } catch (Exception e) {
            job.incrementFailed();
            log.error("Failed to ingest metadata for node: {}", node.getId(), e);
        }
    }
}
