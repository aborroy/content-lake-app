package org.alfresco.contentlake.nuxeo.live.service;

import io.micrometer.core.instrument.Timer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.alfresco.contentlake.client.NuxeoClient;
import org.alfresco.contentlake.nuxeo.live.client.NuxeoAuditClient;
import org.alfresco.contentlake.nuxeo.live.config.NuxeoLiveProperties;
import org.alfresco.contentlake.nuxeo.live.model.AuditCursor;
import org.alfresco.contentlake.nuxeo.live.model.NuxeoAuditEntry;
import org.alfresco.contentlake.nuxeo.live.model.NuxeoAuditPage;
import org.alfresco.contentlake.service.NuxeoScopeResolver;
import org.alfresco.contentlake.service.NodeSyncService;
import org.alfresco.contentlake.spi.SourceNode;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.time.OffsetDateTime;
import java.util.List;

/**
 * Consumes the Nuxeo audit stream for near-live sync.
 *
 * <p>The delivery mechanism is scheduled polling, but the processed unit is the
 * audit event stream rather than a repository scan. This keeps the service
 * stateless on the Nuxeo side while preserving live-ingester semantics.</p>
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class NuxeoAuditListener {

    private final NuxeoAuditClient auditClient;
    private final AuditCursorStore cursorStore;
    private final NodeSyncService nodeSyncService;
    private final NuxeoClient nuxeoClient;
    private final NuxeoScopeResolver scopeResolver;
    private final NuxeoLiveProperties liveProperties;
    private final NuxeoAuditMetrics metrics;
    private final Clock clock;

    @Scheduled(
            initialDelayString = "#{@nuxeoLiveRuntime.audit.initialDelay.toMillis()}",
            fixedDelayString = "#{@nuxeoLiveRuntime.audit.fixedDelay.toMillis()}"
    )
    public void listen() {
        if (!liveProperties.getAudit().isEnabled()) {
            return;
        }

        String repositoryKey = repositoryKey();
        Timer.Sample sample = metrics.startPoll();
        try {
            AuditCursor persistedCursor = resolveCursor(repositoryKey);
            metrics.updateCursor(repositoryKey, persistedCursor.lastLogDate());

            OffsetDateTime windowEnd = OffsetDateTime.now(clock);
            AuditCursor cycleCursor = persistedCursor;
            int processed = 0;

            while (true) {
                NuxeoAuditPage page = auditClient.fetchPage(
                        cycleCursor,
                        windowEnd,
                        liveProperties.getAudit().getPageSize()
                );
                List<NuxeoAuditEntry> entries = page.getEntries();
                if (entries.isEmpty()) {
                    break;
                }

                for (NuxeoAuditEntry entry : entries) {
                    processEntry(repositoryKey, entry);
                    cycleCursor = AuditCursor.from(entry);
                    processed++;
                }

                if (!page.hasMore()) {
                    break;
                }
            }

            if (processed > 0) {
                cursorStore.save(repositoryKey, cycleCursor);
                metrics.updateCursor(repositoryKey, cycleCursor.lastLogDate());
            }

            metrics.recordPoll(repositoryKey, "success", sample);
            log.info("Completed Nuxeo audit cycle for {} with {} processed entries", repositoryKey, processed);
        } catch (Exception e) {
            metrics.recordPoll(repositoryKey, "error", sample);
            log.error("Nuxeo audit cycle failed for {}", repositoryKey, e);
        }
    }

    private AuditCursor resolveCursor(String repositoryKey) {
        return cursorStore.load(repositoryKey)
                .orElseGet(() -> AuditCursor.initial(
                        OffsetDateTime.now(clock).minus(liveProperties.getAudit().getInitialLookback())
                ));
    }

    private void processEntry(String repositoryKey, NuxeoAuditEntry entry) {
        String eventId = entry.eventId();
        if (entry.docUUID() == null || entry.docUUID().isBlank()) {
            metrics.recordEvent(repositoryKey, eventId, "skipped");
            log.debug("Skipping audit event {} because docUUID is missing (entry id {})", eventId, entry.id());
            return;
        }

        try {
            switch (eventId) {
                case "documentCreated", "documentModified" -> processUpsert(repositoryKey, entry);
                case "documentSecurityUpdated" -> processPermissionUpdate(repositoryKey, entry);
                case "documentTrashed", "documentRemoved" -> processDelete(repositoryKey, entry);
                default -> {
                    metrics.recordEvent(repositoryKey, eventId, "skipped");
                    log.debug("Ignoring unsupported audit event {} (entry id {})", eventId, entry.id());
                }
            }
        } catch (Exception e) {
            metrics.recordEvent(repositoryKey, eventId, "error");
            log.warn(
                    "Failed to process audit event {} (entry id {}, docUUID {}): {}",
                    eventId,
                    entry.id(),
                    entry.docUUID(),
                    e.getMessage(),
                    e
            );
        }
    }

    private void processUpsert(String repositoryKey, NuxeoAuditEntry entry) {
        SourceNode node = nuxeoClient.getNode(entry.docUUID());
        if (node == null) {
            boolean deleted = nodeSyncService.deleteNode(entry.docUUID(), effectiveEventTime(entry));
            metrics.recordEvent(repositoryKey, entry.eventId(), deleted ? "deleted" : "skipped");
            return;
        }

        if (!scopeResolver.isInScope(node)) {
            boolean deleted = nodeSyncService.deleteNode(entry.docUUID(), effectiveEventTime(entry));
            metrics.recordEvent(repositoryKey, entry.eventId(), deleted ? "deleted" : "skipped");
            return;
        }

        nodeSyncService.syncNode(node);
        metrics.recordEvent(repositoryKey, entry.eventId(), "synced");
    }

    private void processPermissionUpdate(String repositoryKey, NuxeoAuditEntry entry) {
        SourceNode node = nuxeoClient.getNode(entry.docUUID());
        if (node == null) {
            boolean deleted = nodeSyncService.deleteNode(entry.docUUID(), effectiveEventTime(entry));
            metrics.recordEvent(repositoryKey, entry.eventId(), deleted ? "deleted" : "skipped");
            return;
        }

        if (!scopeResolver.isInScope(node)) {
            boolean deleted = nodeSyncService.deleteNode(entry.docUUID(), effectiveEventTime(entry));
            metrics.recordEvent(repositoryKey, entry.eventId(), deleted ? "deleted" : "skipped");
            return;
        }

        nodeSyncService.updatePermissions(node);
        metrics.recordEvent(repositoryKey, entry.eventId(), "updated");
    }

    private void processDelete(String repositoryKey, NuxeoAuditEntry entry) {
        nodeSyncService.deleteNode(entry.docUUID(), effectiveEventTime(entry));
        metrics.recordEvent(repositoryKey, entry.eventId(), "deleted");
    }

    private OffsetDateTime effectiveEventTime(NuxeoAuditEntry entry) {
        return entry.eventDate() != null ? entry.eventDate() : entry.logDate();
    }

    private String repositoryKey() {
        return nuxeoClient.getSourceType() + ":" + nuxeoClient.getSourceId();
    }
}
