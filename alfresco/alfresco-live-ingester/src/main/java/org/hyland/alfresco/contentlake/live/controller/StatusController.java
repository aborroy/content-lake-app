package org.hyland.alfresco.contentlake.live.controller;

import lombok.RequiredArgsConstructor;
import org.hyland.alfresco.contentlake.live.service.LiveIngesterMetrics;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

/**
 * Exposes live-ingester event processing metrics.
 */
@RestController
@RequestMapping("/api/live")
@RequiredArgsConstructor
public class StatusController {

    private final LiveIngesterMetrics metrics;

    @GetMapping("/status")
    public Map<String, Object> getStatus() {
        return Map.of(
                "received", metrics.getReceivedCount(),
                "processed", metrics.getProcessedCount(),
                "filtered", metrics.getFilteredCount(),
                "deduplicated", metrics.getDeduplicatedCount(),
                "errors", metrics.getErrorCount()
        );
    }
}
