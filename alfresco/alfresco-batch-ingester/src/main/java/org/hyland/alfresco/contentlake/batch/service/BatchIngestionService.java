package org.hyland.alfresco.contentlake.batch.service;

import lombok.extern.slf4j.Slf4j;
import org.hyland.alfresco.contentlake.batch.model.BatchSyncRequest;
import org.hyland.alfresco.contentlake.batch.model.IngestionJob;
import org.hyland.alfresco.contentlake.batch.model.TransformationTask;
import org.alfresco.core.model.Node;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

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

    private final Map<String, IngestionJob> jobsById = new ConcurrentHashMap<>();

    /**
     * Coordinates batch ingestion: discover nodes, ingest metadata, and enqueue transformations.
     *
     * @param discoveryService service to discover nodes to ingest
     * @param metadataIngester component that ingests node metadata into hxpr
     * @param transformationQueue queue for transformation tasks
     * @param batchIngestionExecutor executor for asynchronous ingestion jobs
     */
    public BatchIngestionService(
            NodeDiscoveryService discoveryService,
            MetadataIngester metadataIngester,
            TransformationQueue transformationQueue,
            @Qualifier("batchIngestionExecutor") Executor batchIngestionExecutor
    ) {
        this.discoveryService = discoveryService;
        this.metadataIngester = metadataIngester;
        this.transformationQueue = transformationQueue;
        this.batchIngestionExecutor = batchIngestionExecutor;
    }

    /**
     * Starts an asynchronous batch synchronization job based on the given request.
     *
     * @param request discovery and filtering parameters
     * @return the created {@link IngestionJob}
     */
    public IngestionJob startBatchSync(BatchSyncRequest request) {
        IngestionJob job = createJob("batch sync");

        CompletableFuture.runAsync(() -> runBatchSync(job, request), batchIngestionExecutor);

        return job;
    }

    /**
     * Starts an asynchronous synchronization job based on the application configuration.
     *
     * @return the created {@link IngestionJob}
     */
    public IngestionJob startConfiguredSync() {
        IngestionJob job = createJob("configured sync");

        CompletableFuture.runAsync(() -> runConfiguredSync(job), batchIngestionExecutor);

        return job;
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
            discoveryService.discoverNodes(request).forEach(node -> ingestNode(node, job));

            job.complete();
            log.info(
                    "Batch sync job {} completed. Discovered: {}, Ingested: {}, Failed: {}",
                    jobId,
                    job.getDiscoveredCount(),
                    job.getMetadataIngestedCount(),
                    job.getFailedCount()
            );
        } catch (Exception e) {
            log.error("Batch sync job {} failed", jobId, e);
            job.fail();
        }
    }

    private void runConfiguredSync(IngestionJob job) {
        String jobId = job.getJobId();
        try {
            discoveryService.discoverFromConfig().forEach(node -> ingestNode(node, job));

            job.complete();
            log.info("Configured sync job {} completed", jobId);
        } catch (Exception e) {
            log.error("Configured sync job {} failed", jobId, e);
            job.fail();
        }
    }

    private void ingestNode(Node node, IngestionJob job) {
        job.incrementDiscovered();
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
