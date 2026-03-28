package org.hyland.alfresco.contentlake.batch.controller;

import lombok.RequiredArgsConstructor;
import org.hyland.alfresco.contentlake.batch.model.BatchSyncRequest;
import org.hyland.alfresco.contentlake.batch.model.IngestionJob;
import org.hyland.alfresco.contentlake.batch.service.BatchIngestionService;
import org.hyland.alfresco.contentlake.batch.service.TransformationQueue;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

/**
 * REST controller exposing batch synchronization endpoints.
 *
 * <p>Provides operations to start ingestion jobs, query their status,
 * inspect the transformation queue, and perform basic queue management.</p>
 */
@RestController
@RequestMapping("/api/sync")
@RequiredArgsConstructor
public class SyncController {

    private final BatchIngestionService batchIngestionService;
    private final TransformationQueue transformationQueue;

    /**
     * Starts a batch synchronization job using the provided request parameters.
     *
     * @param request batch synchronization configuration
     * @return the created {@link IngestionJob}
     */
    @PostMapping("/batch")
    public IngestionJob startBatchSync(@RequestBody BatchSyncRequest request) {
        return batchIngestionService.startBatchSync(request);
    }

    /**
     * Starts a batch synchronization job using the default configuration.
     *
     * @return the created {@link IngestionJob}
     */
    @PostMapping("/configured")
    public IngestionJob startConfiguredSync() {
        return batchIngestionService.startConfiguredSync();
    }

    /**
     * Retrieves the status of a specific ingestion job.
     *
     * @param jobId the job identifier
     * @return the corresponding {@link IngestionJob}
     */
    @GetMapping("/status/{jobId}")
    public IngestionJob getJobStatus(@PathVariable String jobId) {
        return batchIngestionService.getJob(jobId);
    }

    /**
     * Returns an aggregated view of all jobs and queue statistics.
     *
     * @return a map containing job list and queue metrics
     */
    @GetMapping("/status")
    public Map<String, Object> getOverallStatus() {
        return Map.of(
                "jobs", batchIngestionService.getAllJobs(),
                "queue", Map.of(
                        "pending", transformationQueue.getPendingCount(),
                        "completed", transformationQueue.getCompletedCount(),
                        "failed", transformationQueue.getFailedCount(),
                        "queueSize", transformationQueue.getQueueSize()
                )
        );
    }

    /**
     * Clears all pending items from the transformation queue.
     *
     * @return a simple status response
     */
    @DeleteMapping("/queue")
    public Map<String, String> clearQueue() {
        transformationQueue.clear();
        return Map.of("status", "cleared");
    }
}
