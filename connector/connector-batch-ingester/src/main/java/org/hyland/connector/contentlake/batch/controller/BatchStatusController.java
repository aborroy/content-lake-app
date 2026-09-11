package org.hyland.connector.contentlake.batch.controller;

import lombok.RequiredArgsConstructor;
import org.hyland.connector.contentlake.batch.config.SelectedConnector;
import org.hyland.connector.contentlake.batch.model.IngestionJob;
import org.hyland.connector.contentlake.batch.service.ConnectorBatchIngestionService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.Comparator;

/**
 * Compact operational status: {@code GET /api/status}.
 *
 * <p>Surfaces the most recent run alongside which connector is loaded and which jar it came from. The
 * origin is here rather than only in {@code /api/connectors} because the first question about an
 * unexpected corpus is which jar produced it.</p>
 */
@RestController
@RequestMapping("/api/status")
@RequiredArgsConstructor
public class BatchStatusController {

    private final ConnectorBatchIngestionService batchIngestionService;
    private final SelectedConnector connector;

    @GetMapping
    public BatchStatus status() {
        IngestionJob latest = batchIngestionService.getAllJobs().values().stream()
                .max(Comparator.comparing(IngestionJob::getStartedAt))
                .orElse(null);

        if (latest == null) {
            return new BatchStatus(connector.sourceType(), connector.displayName(), connector.origin(),
                    "IDLE", null, null, null, 0, 0, 0, 0);
        }
        return new BatchStatus(
                connector.sourceType(),
                connector.displayName(),
                connector.origin(),
                latest.getStatus().name(),
                latest.getJobId(),
                latest.getStartedAt(),
                latest.getCompletedAt(),
                latest.getDiscoveredCountValue(),
                latest.getSyncedCountValue(),
                latest.getSkippedCountValue(),
                latest.getFailedCountValue());
    }

    /** Last-run summary. {@code state} is {@code IDLE} when no run has occurred. */
    public record BatchStatus(
            String sourceType,
            String connectorName,
            String connectorOrigin,
            String state,
            String jobId,
            Instant startedAt,
            Instant completedAt,
            int nodesDiscovered,
            int nodesIndexed,
            int nodesSkipped,
            int nodesFailed) {
    }
}
