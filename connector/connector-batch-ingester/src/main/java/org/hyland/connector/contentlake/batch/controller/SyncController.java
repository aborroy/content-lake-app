package org.hyland.connector.contentlake.batch.controller;

import lombok.RequiredArgsConstructor;
import org.hyland.connector.contentlake.batch.model.IngestionJob;
import org.hyland.connector.contentlake.batch.service.ConnectorBatchIngestionService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

/**
 * Batch synchronization endpoints, the same three every batch ingester exposes.
 *
 * <p>Which connector a sync runs against is startup configuration, not a request parameter: this host runs
 * exactly one, and choosing per request would let a caller ingest from a source the deployment did not
 * intend.</p>
 */
@RestController
@RequestMapping("/api/sync")
@RequiredArgsConstructor
public class SyncController {

    private final ConnectorBatchIngestionService batchIngestionService;

    /** Starts a batch sync over the configured roots of the loaded connector. */
    @PostMapping("/configured")
    public IngestionJob startConfiguredSync() {
        return batchIngestionService.startConfiguredSync();
    }

    /** Retrieves the status of a specific ingestion job. */
    @GetMapping("/status/{jobId}")
    public IngestionJob getJobStatus(@PathVariable String jobId) {
        return batchIngestionService.getJob(jobId);
    }

    /** Returns all known jobs keyed by identifier. */
    @GetMapping("/status")
    public Map<String, IngestionJob> getOverallStatus() {
        return batchIngestionService.getAllJobs();
    }
}
