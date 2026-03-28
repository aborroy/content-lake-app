package org.hyland.alfresco.contentlake.live.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.hyland.alfresco.contentlake.adapter.AlfrescoSourceNodeAdapter;
import org.hyland.alfresco.contentlake.client.AlfrescoClient;
import org.hyland.alfresco.contentlake.service.ContentLakeScopeResolver;
import org.hyland.contentlake.service.NodeSyncService;
import org.hyland.contentlake.spi.SourceNode;
import org.alfresco.core.model.Node;
import org.springframework.stereotype.Service;

import java.time.OffsetDateTime;
import java.util.Set;

/**
 * Reconciles all descendant files after a folder-level scope change such as
 * adding or removing {@code cl:indexed}.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class FolderSubtreeReconciler {

    private final AlfrescoClient alfrescoClient;
    private final ContentLakeScopeResolver scopeResolver;
    private final NodeSyncService nodeSyncService;

    public ReconciliationResult reconcile(Node folder, OffsetDateTime eventTimestamp) {
        ReconciliationResult result = new ReconciliationResult();

        if (folder == null || !Boolean.TRUE.equals(folder.isIsFolder())) {
            return result;
        }

        reconcileChildren(folder.getId(), eventTimestamp, result);
        return result;
    }

    private void reconcileChildren(String folderId, OffsetDateTime eventTimestamp, ReconciliationResult result) {
        for (Node child : alfrescoClient.getAllChildren(folderId)) {
            if (Boolean.TRUE.equals(child.isIsFolder())) {
                if (!scopeResolver.shouldTraverse(child)) {
                    result.skipped++;
                    continue;
                }
                reconcileChildren(child.getId(), eventTimestamp, result);
                continue;
            }

            if (!Boolean.TRUE.equals(child.isIsFile())) {
                result.skipped++;
                continue;
            }

            try {
                if (scopeResolver.isInScope(child)) {
                    Set<String> readers = alfrescoClient.extractReadAuthorities(child);
                    SourceNode sourceNode = AlfrescoSourceNodeAdapter.toSourceNode(
                            child, alfrescoClient.getSourceId(), readers);
                    nodeSyncService.syncNode(sourceNode);
                    result.synced++;
                } else {
                    nodeSyncService.deleteNode(child.getId(), resolveDeleteTimestamp(eventTimestamp, child));
                    result.deleted++;
                }
            } catch (Exception e) {
                result.failed++;
                log.error("Failed to reconcile node {} during folder subtree reconciliation", child.getId(), e);
            }
        }
    }

    private OffsetDateTime resolveDeleteTimestamp(OffsetDateTime eventTimestamp, Node child) {
        if (eventTimestamp != null) {
            return eventTimestamp;
        }
        return child.getModifiedAt();
    }

    public static final class ReconciliationResult {
        private int synced;
        private int deleted;
        private int skipped;
        private int failed;

        public int synced()   { return synced; }
        public int deleted()  { return deleted; }
        public int skipped()  { return skipped; }
        public int failed()   { return failed; }
    }
}
