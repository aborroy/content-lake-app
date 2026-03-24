package org.alfresco.contentlake.live.service;

import lombok.RequiredArgsConstructor;
import org.alfresco.contentlake.live.config.LiveIngesterProperties;
import org.alfresco.repo.event.v1.model.DataAttributes;
import org.alfresco.repo.event.v1.model.NodeResource;
import org.alfresco.repo.event.v1.model.RepoEvent;
import org.alfresco.repo.event.v1.model.Resource;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

@Service
@RequiredArgsConstructor
public class RecentEventDeduplicator {

    private final LiveIngesterProperties props;

    private final Map<String, Instant> recentKeys = new ConcurrentHashMap<>();
    private final AtomicInteger cleanupCounter = new AtomicInteger();

    public boolean shouldSkip(RepoEvent<DataAttributes<Resource>> event, String nodeId) {
        Duration window = props.getDedup().getWindow();
        if (window == null || window.isZero() || window.isNegative()) {
            return false;
        }

        String key = buildKey(event, nodeId);
        if (key == null) {
            return false;
        }

        Instant now = Instant.now();
        maybeCleanup(now);

        Instant expiresAt = now.plus(window);
        Instant existing = recentKeys.putIfAbsent(key, expiresAt);
        while (existing != null) {
            if (existing.isAfter(now)) {
                return true;
            }
            if (recentKeys.replace(key, existing, expiresAt)) {
                return false;
            }
            existing = recentKeys.get(key);
        }

        return false;
    }

    private String buildKey(RepoEvent<DataAttributes<Resource>> event, String nodeId) {
        if (nodeId == null || nodeId.isBlank()) {
            return null;
        }

        String type = event.getType() != null ? event.getType() : "unknown";
        Resource resource = event.getData() != null ? event.getData().getResource() : null;
        if (resource instanceof NodeResource nodeResource && nodeResource.getModifiedAt() != null) {
            return type + "|" + nodeId + "|" + nodeResource.getModifiedAt().toInstant();
        }
        if (event.getId() != null && !event.getId().isBlank()) {
            return type + "|" + nodeId + "|" + event.getId();
        }
        if (event.getTime() != null) {
            return type + "|" + nodeId + "|" + event.getTime().toInstant();
        }
        return type + "|" + nodeId;
    }

    private void maybeCleanup(Instant now) {
        if (cleanupCounter.incrementAndGet() % 128 != 0 && recentKeys.size() <= props.getDedup().getMaxEntries()) {
            return;
        }

        recentKeys.entrySet().removeIf(entry -> !entry.getValue().isAfter(now));
    }
}
