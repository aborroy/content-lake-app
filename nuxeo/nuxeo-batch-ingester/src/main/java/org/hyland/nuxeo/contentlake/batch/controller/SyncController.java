package org.hyland.nuxeo.contentlake.batch.controller;

import lombok.RequiredArgsConstructor;
import org.hyland.nuxeo.contentlake.batch.model.IngestionJob;
import org.hyland.nuxeo.contentlake.batch.model.NuxeoSyncRequest;
import org.hyland.nuxeo.contentlake.batch.service.NuxeoBatchIngestionService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
@RequestMapping("/api/sync")
@RequiredArgsConstructor
public class SyncController {

    private final NuxeoBatchIngestionService batchIngestionService;

    @PostMapping("/batch")
    public IngestionJob startBatchSync(@RequestBody(required = false) NuxeoSyncRequest request) {
        return batchIngestionService.startBatchSync(request != null ? request : new NuxeoSyncRequest());
    }

    @PostMapping("/configured")
    public IngestionJob startConfiguredSync() {
        return batchIngestionService.startConfiguredSync();
    }

    @GetMapping("/status/{jobId}")
    public ResponseEntity<IngestionJob> getJobStatus(@PathVariable String jobId) {
        IngestionJob job = batchIngestionService.getJob(jobId);
        return job != null ? ResponseEntity.ok(job) : ResponseEntity.notFound().build();
    }

    @GetMapping("/status")
    public Map<String, Object> getOverallStatus() {
        return Map.of(
                "sourceType", "nuxeo",
                "jobs", batchIngestionService.getAllJobs()
        );
    }
}
