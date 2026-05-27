package org.hyland.alfresco.contentlake.batch.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.alfresco.core.model.Node;
import org.hyland.alfresco.contentlake.adapter.AlfrescoSourceNodeAdapter;
import org.hyland.alfresco.contentlake.batch.model.PermissionSyncRequest;
import org.hyland.alfresco.contentlake.batch.model.PermissionSyncResult;
import org.hyland.alfresco.contentlake.client.AlfrescoClient;
import org.hyland.alfresco.contentlake.client.AlfrescoSearchService;
import org.hyland.alfresco.contentlake.service.ContentLakeScopeResolver;
import org.hyland.contentlake.service.NodeSyncService;
import org.hyland.contentlake.spi.SourceNode;
import org.springframework.stereotype.Service;

import java.util.Set;

/**
 * Explicit ACL reconciliation path used when repository permission changes are
 * not emitted as live events.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class PermissionSyncService {

    private final AlfrescoClient alfrescoClient;
    private final AlfrescoSearchService searchService;
    private final ContentLakeScopeResolver scopeResolver;
    private final NodeSyncService nodeSyncService;

    public PermissionSyncResult syncPermissions(PermissionSyncRequest request) {
        MutableResult result = new MutableResult();
        if (request == null || request.getNodeIds() == null || request.getNodeIds().isEmpty()) {
            return result.toImmutable();
        }

        for (String nodeId : request.getNodeIds()) {
            if (nodeId == null || nodeId.isBlank()) {
                result.skipped++;
                continue;
            }
            reconcileNode(nodeId.trim(), request.isRecursive(), result);
        }

        return result.toImmutable();
    }

    private void reconcileNode(String nodeId, boolean recursive, MutableResult result) {
        try {
            Node node = alfrescoClient.getAlfrescoNode(nodeId);
            if (node == null) {
                result.skipped++;
                log.warn("Skipping permission reconciliation for missing node {}", nodeId);
                return;
            }

            if (Boolean.TRUE.equals(node.isIsFolder())) {
                reconcileFolder(node, recursive, result);
                return;
            }

            reconcileFile(node, result);
        } catch (Exception e) {
            result.failed++;
            log.error("Failed permission reconciliation for node {}", nodeId, e);
        }
    }

    private void reconcileFolder(Node folder, boolean recursive, MutableResult result) {
        if (!recursive) {
            result.skipped++;
            log.info("Skipping non-recursive permission reconciliation for folder {}", folder.getId());
            return;
        }
        if (!scopeResolver.shouldTraverse(folder)) {
            result.skipped++;
            log.info("Skipping permission reconciliation for excluded folder {}", folder.getId());
            return;
        }

        for (Node child : searchService.findDescendantFiles(folder.getId(), scopeResolver.getExcludedAspects())) {
            try {
                reconcileFile(child, result);
            } catch (Exception e) {
                result.failed++;
                log.error("Failed permission reconciliation for descendant {}", child.getId(), e);
            }
        }
    }

    private void reconcileFile(Node file, MutableResult result) {
        if (!scopeResolver.isInScope(file)) {
            nodeSyncService.deleteNode(file.getId(), file.getModifiedAt());
            result.deleted++;
            return;
        }

        nodeSyncService.updatePermissions(toSourceNode(file));
        result.updated++;
    }

    private SourceNode toSourceNode(Node node) {
        Set<String> readers = alfrescoClient.extractReadAuthorities(node);
        return AlfrescoSourceNodeAdapter.toSourceNode(node, alfrescoClient.getSourceId(), readers);
    }

    private static final class MutableResult {
        private int updated;
        private int deleted;
        private int skipped;
        private int failed;

        private PermissionSyncResult toImmutable() {
            return new PermissionSyncResult(updated, deleted, skipped, failed);
        }
    }
}
