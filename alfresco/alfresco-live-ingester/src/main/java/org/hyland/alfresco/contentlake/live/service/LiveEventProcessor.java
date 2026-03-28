package org.hyland.alfresco.contentlake.live.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.hyland.alfresco.contentlake.adapter.AlfrescoSourceNodeAdapter;
import org.hyland.alfresco.contentlake.client.AlfrescoClient;
import org.hyland.alfresco.contentlake.service.ContentLakeScopeResolver;
import org.hyland.contentlake.service.NodeSyncService;
import org.hyland.contentlake.spi.SourceNode;
import org.alfresco.core.model.Node;
import org.alfresco.repo.event.v1.model.ChildAssociationResource;
import org.alfresco.repo.event.v1.model.DataAttributes;
import org.alfresco.repo.event.v1.model.NodeResource;
import org.alfresco.repo.event.v1.model.PeerAssociationResource;
import org.alfresco.repo.event.v1.model.RepoEvent;
import org.alfresco.repo.event.v1.model.Resource;
import org.springframework.stereotype.Service;

import java.time.OffsetDateTime;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

@Slf4j
@Service
@RequiredArgsConstructor
public class LiveEventProcessor {

    private final AlfrescoClient alfrescoClient;
    private final NodeSyncService nodeSyncService;
    private final ContentLakeScopeResolver scopeResolver;
    private final FolderSubtreeReconciler folderSubtreeReconciler;
    private final RecentEventDeduplicator deduplicator;
    private final LiveIngesterMetrics metrics;

    public void processNodeStateChange(RepoEvent<DataAttributes<Resource>> event) {
        processNodeStateChange(event, extractPrimaryNodeId(event), true);
    }

    public void processNodeStateChange(RepoEvent<DataAttributes<Resource>> event, String nodeId, boolean deleteIfMissing) {
        metrics.recordReceived();

        if (nodeId == null || nodeId.isBlank()) {
            metrics.recordFiltered();
            log.warn("Skipping node state change event {} — missing node id", event.getId());
            return;
        }
        if (deduplicator.shouldSkip(event, nodeId)) {
            metrics.recordDeduplicated();
            log.debug("Skipping duplicate node event {} for node {}", event.getId(), nodeId);
            return;
        }

        try {
            Node node = alfrescoClient.getAlfrescoNode(nodeId);
            if (node == null) {
                if (deleteIfMissing) {
                    nodeSyncService.deleteNode(nodeId, resolveEventTimestamp(event, null));
                } else {
                    metrics.recordFiltered();
                    log.warn("Node {} not found for event {}", nodeId, event.getId());
                    return;
                }
            } else if (scopeResolver.isInScope(node)) {
                SourceNode sourceNode = toSourceNode(node);
                nodeSyncService.syncNode(sourceNode);
            } else {
                nodeSyncService.deleteNode(nodeId, resolveEventTimestamp(event, node));
                metrics.recordFiltered();
                log.debug("Node {} is out of scope after event {}", nodeId, event.getId());
                return;
            }

            metrics.recordProcessed();
        } catch (Exception e) {
            metrics.recordError();
            log.error("Failed to process node state change {} for node {}: {}", event.getType(), nodeId, e.getMessage(), e);
        }
    }

    public void processDeletion(RepoEvent<DataAttributes<Resource>> event) {
        metrics.recordReceived();

        String nodeId = extractPrimaryNodeId(event);
        if (nodeId == null || nodeId.isBlank()) {
            metrics.recordFiltered();
            log.warn("Skipping delete event {} — missing node id", event.getId());
            return;
        }
        if (deduplicator.shouldSkip(event, nodeId)) {
            metrics.recordDeduplicated();
            log.debug("Skipping duplicate delete event {} for node {}", event.getId(), nodeId);
            return;
        }

        try {
            nodeSyncService.deleteNode(nodeId, resolveEventTimestamp(event, null));
            metrics.recordProcessed();
        } catch (Exception e) {
            metrics.recordError();
            log.error("Failed to process delete event {} for node {}: {}", event.getType(), nodeId, e.getMessage(), e);
        }
    }

    public void processPermissionUpdate(RepoEvent<DataAttributes<Resource>> event) {
        metrics.recordReceived();

        List<String> candidateNodeIds = extractCandidateNodeIds(event);
        String dedupNodeId = candidateNodeIds.isEmpty() ? null : candidateNodeIds.getFirst();
        if (dedupNodeId == null || dedupNodeId.isBlank()) {
            metrics.recordFiltered();
            log.warn("Skipping permission event {} — no candidate node ids", event.getId());
            return;
        }
        if (deduplicator.shouldSkip(event, dedupNodeId)) {
            metrics.recordDeduplicated();
            log.debug("Skipping duplicate permission event {} for node {}", event.getId(), dedupNodeId);
            return;
        }

        try {
            Node node = fetchFirstAvailableNode(candidateNodeIds);
            if (node == null) {
                metrics.recordFiltered();
                log.warn("No accessible node found for permission event {}", event.getId());
                return;
            }

            if (!scopeResolver.isInScope(node)) {
                nodeSyncService.deleteNode(node.getId(), resolveEventTimestamp(event, node));
                metrics.recordFiltered();
                log.debug("Node {} is out of scope during permission event {}", node.getId(), event.getId());
                return;
            }

            nodeSyncService.updatePermissions(toSourceNode(node));
            metrics.recordProcessed();
        } catch (Exception e) {
            metrics.recordError();
            log.error("Failed to process permission event {}: {}", event.getType(), e.getMessage(), e);
        }
    }

    public void processFolderScopeChange(RepoEvent<DataAttributes<Resource>> event) {
        metrics.recordReceived();

        String nodeId = extractPrimaryNodeId(event);
        if (nodeId == null || nodeId.isBlank()) {
            metrics.recordFiltered();
            log.warn("Skipping folder scope change event {} — missing node id", event.getId());
            return;
        }
        if (deduplicator.shouldSkip(event, nodeId)) {
            metrics.recordDeduplicated();
            log.debug("Skipping duplicate folder scope change event {} for folder {}", event.getId(), nodeId);
            return;
        }

        try {
            Node folder = alfrescoClient.getAlfrescoNode(nodeId);
            if (folder == null) {
                metrics.recordFiltered();
                log.warn("Folder {} not found for scope change event {}", nodeId, event.getId());
                return;
            }
            if (!Boolean.TRUE.equals(folder.isIsFolder())) {
                metrics.recordFiltered();
                log.debug("Node {} is not a folder for scope change event {}", nodeId, event.getId());
                return;
            }
            if (!scopeResolver.shouldTraverse(folder)) {
                metrics.recordFiltered();
                log.debug("Folder {} is excluded from subtree reconciliation for event {}", nodeId, event.getId());
                return;
            }

            scopeResolver.invalidateFolderScope(nodeId);

            FolderSubtreeReconciler.ReconciliationResult result =
                    folderSubtreeReconciler.reconcile(folder, resolveEventTimestamp(event, folder));

            metrics.recordProcessed();
            log.info(
                    "Reconciled subtree for folder {} after {}: synced={}, deleted={}, skipped={}, failed={}",
                    nodeId,
                    event.getType(),
                    result.synced(),
                    result.deleted(),
                    result.skipped(),
                    result.failed()
            );
        } catch (Exception e) {
            metrics.recordError();
            log.error("Failed to process folder scope change {} for folder {}: {}", event.getType(), nodeId, e.getMessage(), e);
        }
    }

    public String extractPrimaryNodeId(RepoEvent<DataAttributes<Resource>> event) {
        List<String> ids = extractCandidateNodeIds(event);
        return ids.isEmpty() ? null : ids.getFirst();
    }

    public List<String> extractCandidateNodeIds(RepoEvent<DataAttributes<Resource>> event) {
        LinkedHashSet<String> ids = new LinkedHashSet<>();
        if (event != null && event.getData() != null) {
            addNodeIds(ids, event.getData().getResource());
            addNodeIds(ids, event.getData().getResourceBefore());
        }
        return List.copyOf(ids);
    }

    // ──────────────────────────────────────────────────────────────────────
    // Alfresco boundary: Node → SourceNode conversion
    // ──────────────────────────────────────────────────────────────────────

    private SourceNode toSourceNode(Node node) {
        Set<String> readers = alfrescoClient.extractReadAuthorities(node);
        return AlfrescoSourceNodeAdapter.toSourceNode(node, alfrescoClient.getSourceId(), readers);
    }

    private Node fetchFirstAvailableNode(List<String> candidateNodeIds) {
        for (String candidateNodeId : candidateNodeIds) {
            Node node = alfrescoClient.getAlfrescoNode(candidateNodeId);
            if (node != null) {
                return node;
            }
        }
        return null;
    }

    private void addNodeIds(Set<String> ids, Resource resource) {
        if (resource instanceof NodeResource nodeResource) {
            addIfPresent(ids, nodeResource.getId());
            return;
        }
        if (resource instanceof ChildAssociationResource assocResource) {
            if (assocResource.getChild() != null) {
                addIfPresent(ids, assocResource.getChild().getId());
            }
            return;
        }
        if (resource instanceof PeerAssociationResource assocResource) {
            if (assocResource.getSource() != null) {
                addIfPresent(ids, assocResource.getSource().getId());
            }
            if (assocResource.getTarget() != null) {
                addIfPresent(ids, assocResource.getTarget().getId());
            }
        }
    }

    private void addIfPresent(Set<String> ids, String nodeId) {
        if (nodeId != null && !nodeId.isBlank()) {
            ids.add(nodeId);
        }
    }

    private OffsetDateTime resolveEventTimestamp(RepoEvent<DataAttributes<Resource>> event, Node node) {
        if (node != null && node.getModifiedAt() != null) {
            return node.getModifiedAt();
        }

        Resource resource = event.getData() != null ? event.getData().getResource() : null;
        if (resource instanceof NodeResource nodeResource && nodeResource.getModifiedAt() != null) {
            return nodeResource.getModifiedAt().toOffsetDateTime();
        }

        if (event.getTime() != null) {
            return event.getTime().toOffsetDateTime();
        }

        return null;
    }
}
