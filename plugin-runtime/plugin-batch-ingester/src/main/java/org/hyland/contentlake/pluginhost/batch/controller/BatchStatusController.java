package org.hyland.contentlake.pluginhost.batch.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.hyland.contentlake.pluginhost.batch.config.SelectedConnector;
import org.hyland.contentlake.pluginhost.batch.model.IngestionJob;
import org.hyland.contentlake.pluginhost.batch.service.ConnectorBatchIngestionService;
import org.hyland.contentlake.spi.SourceAuthState;
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
@Slf4j
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
                    "IDLE", null, null, null, 0, 0, 0, 0, authState());
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
                latest.getFailedCountValue(),
                authState());
    }

    /**
     * The connector's authentication state, or {@code null} when it reports none.
     *
     * <p>A field on this response rather than an endpoint of its own: a screen showing it is already polling
     * status, and a second endpoint would double the calls for a line of text.</p>
     *
     * <p>The connector's contract is that answering is cheap and makes no network call, but a connector is a
     * third-party jar and this is a status endpoint. So a throwing implementation degrades to "nothing to
     * report" rather than taking down the one response an operator uses to work out what is wrong. Logged at
     * debug, because a connector that has no auth state and a connector that fails to describe it are equally
     * uninteresting until someone is looking.</p>
     */
    private SourceAuthState authState() {
        try {
            return connector.client().authState();
        } catch (RuntimeException e) {
            log.debug("Connector '{}' could not report its authentication state: {}",
                    connector.sourceType(), e.getMessage());
            return null;
        }
    }

    /**
     * Last-run summary. {@code state} is {@code IDLE} when no run has occurred.
     *
     * @param auth how the connector is authenticating, or {@code null} when it has nothing to report, which is
     *             the case for every source whose credential is deployment configuration. Present for one whose
     *             credential can lapse while the rest of the deployment stays healthy
     */
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
            int nodesFailed,
            SourceAuthState auth) {
    }
}
