package org.hyland.nuxeo.contentlake.live.service;

import io.micrometer.core.instrument.Timer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.hyland.nuxeo.contentlake.client.NuxeoClient;
import org.hyland.nuxeo.contentlake.live.client.NuxeoAuditClient;
import org.hyland.nuxeo.contentlake.live.config.NuxeoLiveProperties;
import org.hyland.nuxeo.contentlake.live.model.AuditCursor;
import org.hyland.nuxeo.contentlake.live.model.NuxeoAuditEntry;
import org.hyland.nuxeo.contentlake.live.model.NuxeoAuditPage;
import org.hyland.nuxeo.contentlake.service.NuxeoScopeResolver;
import org.hyland.contentlake.service.NodeSyncService;
import org.hyland.contentlake.spi.SourceNode;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import org.hyland.contentlake.model.ContentLakeIngestProperties;

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

        if (node.folder()) {
            handleFolderModified(repositoryKey, node, entry);
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

    private void handleFolderModified(String repositoryKey, SourceNode folder, NuxeoAuditEntry entry) {
        String folderPath = getFolderPath(folder);
        boolean hadCachedEntry = scopeResolver.invalidateFolderScope(folderPath);

        Object facetsObj = folder.sourceProperties().get(ContentLakeIngestProperties.NUXEO_FACETS);
        boolean hasScopeFacet = facetsObj instanceof List<?> facets
                && (facets.contains("ContentLakeIndexed") || facets.contains("ContentLakeScope"));

        if ((hasScopeFacet || hadCachedEntry) && liveProperties.getSubtreeReeval().isEnabled()) {
            log.info("Folder {} scope changed; invalidated cache and triggering subtree re-evaluation", folderPath);
            reevaluateSubtree(folder, entry, 0, new int[]{0});
        } else {
            log.debug("Folder {} modified; scope cache invalidated", folderPath);
        }
        metrics.recordEvent(repositoryKey, entry.eventId(), "folder-scope-invalidated");
    }

    private void reevaluateSubtree(SourceNode folder,
                                   NuxeoAuditEntry triggerEntry,
                                   int currentDepth,
                                   int[] nodeCount) {
        NuxeoLiveProperties.SubtreeReeval cfg = liveProperties.getSubtreeReeval();
        if (currentDepth > cfg.getMaxDepth()) {
            log.warn("Subtree re-evaluation for folder {} exceeded maxDepth={}; aborting. "
                    + "Run a full batch resync to cover the remaining subtree.",
                    getFolderPath(folder), cfg.getMaxDepth());
            return;
        }

        int pageSize = liveProperties.getAudit().getPageSize();
        int skip = 0;
        while (true) {
            List<SourceNode> children = nuxeoClient.getChildren(folder.nodeId(), skip, pageSize);
            if (children.isEmpty()) {
                break;
            }
            for (SourceNode child : children) {
                if (nodeCount[0]++ >= cfg.getMaxNodes()) {
                    log.warn("Subtree re-evaluation for folder {} exceeded maxNodes={}; aborting. "
                            + "Run a full batch resync to cover the remaining subtree.",
                            getFolderPath(folder), cfg.getMaxNodes());
                    return;
                }
                if (child.folder()) {
                    reevaluateSubtree(child, triggerEntry, currentDepth + 1, nodeCount);
                } else {
                    if (scopeResolver.isInScope(child)) {
                        nodeSyncService.syncNode(child);
                    } else {
                        nodeSyncService.deleteNode(child.nodeId(), effectiveEventTime(triggerEntry));
                    }
                }
            }
            if (children.size() < pageSize) {
                break;
            }
            skip += children.size();
        }
    }

    private String getFolderPath(SourceNode folder) {
        Object pathProp = folder.sourceProperties().get(ContentLakeIngestProperties.NUXEO_PATH);
        return pathProp instanceof String s ? s : folder.path();
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
