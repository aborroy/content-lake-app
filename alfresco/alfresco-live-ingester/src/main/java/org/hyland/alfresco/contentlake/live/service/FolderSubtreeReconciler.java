package org.hyland.alfresco.contentlake.live.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.hyland.alfresco.contentlake.adapter.AlfrescoSourceNodeAdapter;
import org.hyland.alfresco.contentlake.client.AlfrescoClient;
import org.hyland.alfresco.contentlake.client.AlfrescoSearchService;
import org.hyland.alfresco.contentlake.service.ContentLakeScopeResolver;
import org.hyland.contentlake.service.NodeSyncService;
import org.hyland.contentlake.spi.SourceNode;
import org.alfresco.core.model.Node;
import org.springframework.stereotype.Service;

import java.time.OffsetDateTime;
import java.util.Set;

/**
 * Reconciles all descendant files after a folder-level scope change.
 *
 * <p>Three modes:</p>
 * <ul>
 *   <li>{@link #reconcile} — {@code cl:indexed} added: sync each descendant.</li>
 *   <li>{@link #reconcilePermissions} — folder permissions changed: refresh ACLs only.</li>
 *   <li>{@link #reconcileTearDown} — {@code cl:indexed} removed (or
 *       {@code cl:excludeFromLake=true} added) and no ancestor keeps the subtree
 *       in scope: delete each descendant from hxpr and clear its
 *       {@code cl:syncStatus} aspect on the source.</li>
 * </ul>
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class FolderSubtreeReconciler {

    private final AlfrescoClient alfrescoClient;
    private final AlfrescoSearchService searchService;
    private final ContentLakeScopeResolver scopeResolver;
    private final NodeSyncService nodeSyncService;

    public ReconciliationResult reconcile(Node folder, OffsetDateTime eventTimestamp) {
        return reconcile(folder, eventTimestamp, ReconciliationMode.SCOPE);
    }

    public ReconciliationResult reconcilePermissions(Node folder, OffsetDateTime eventTimestamp) {
        return reconcile(folder, eventTimestamp, ReconciliationMode.PERMISSIONS);
    }

    /**
     * Tears down a subtree that has just left Content Lake scope (e.g. its
     * {@code cl:indexed} aspect was removed and no ancestor keeps it in scope).
     *
     * <p>For each descendant file: deletes the hxpr document and best-effort clears
     * the {@code cl:syncStatus} aspect on the source node. The descendant query is the
     * same AFTS one used for sync, so previously-indexed files (still aspect-tagged in
     * Solr) are picked up reliably.</p>
     */
    public ReconciliationResult reconcileTearDown(Node folder, OffsetDateTime eventTimestamp) {
        ReconciliationResult result = new ReconciliationResult();

        if (folder == null || !Boolean.TRUE.equals(folder.isIsFolder())) {
            return result;
        }

        Set<String> excludedAspects = scopeResolver.getExcludedAspects();
        for (Node child : searchService.findDescendantFiles(folder.getId(), excludedAspects)) {
            try {
                nodeSyncService.deleteNode(child.getId(), eventTimestamp);
                alfrescoClient.clearSyncStatus(child.getId());
                result.deleted++;
            } catch (Exception e) {
                result.failed++;
                log.error("Failed to tear down node {} during folder subtree reconciliation", child.getId(), e);
            }
        }
        return result;
    }

    private ReconciliationResult reconcile(Node folder,
                                           OffsetDateTime eventTimestamp,
                                           ReconciliationMode mode) {
        ReconciliationResult result = new ReconciliationResult();

        if (folder == null || !Boolean.TRUE.equals(folder.isIsFolder())) {
            return result;
        }

        reconcileChildren(folder.getId(), eventTimestamp, result, mode);
        return result;
    }

    private void reconcileChildren(String folderId,
                                   OffsetDateTime eventTimestamp,
                                   ReconciliationResult result,
                                   ReconciliationMode mode) {
        // The descendant AFTS query already excludes non-cm:content, files with
        // cl:excludeFromLake=true, and configured excludedAspects (e.g. cm:workingcopy).
        //
        // Reaching this method means the root folder's cl:indexed aspect was just
        // added (or its permissions changed). Every descendant returned is therefore
        // in scope by construction. We deliberately skip scopeResolver.isInScope(child)
        // here: that would issue a per-child AFTS query for cl:indexed on ancestors,
        // which races with the Solr commit of the very aspect that triggered THIS
        // reconciliation.
        Set<String> excludedAspects = scopeResolver.getExcludedAspects();
        for (Node child : searchService.findDescendantFiles(folderId, excludedAspects)) {
            try {
                if (matchesTechnicalPathExclusion(child)) {
                    result.skipped++;
                    continue;
                }
                SourceNode sourceNode = toSourceNode(child);
                if (mode == ReconciliationMode.PERMISSIONS) {
                    nodeSyncService.updatePermissions(sourceNode);
                } else {
                    nodeSyncService.syncNode(sourceNode);
                }
                result.synced++;
            } catch (Exception e) {
                result.failed++;
                log.error("Failed to reconcile node {} during folder subtree reconciliation", child.getId(), e);
            }
        }
    }

    private boolean matchesTechnicalPathExclusion(Node child) {
        String path = child.getPath() != null ? child.getPath().getName() : null;
        return scopeResolver.matchesExcludedPath(path);
    }

    private SourceNode toSourceNode(Node child) {
        Set<String> readers = alfrescoClient.extractReadAuthorities(child);
        return AlfrescoSourceNodeAdapter.toSourceNode(child, alfrescoClient.getSourceId(), readers);
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

    private enum ReconciliationMode {
        SCOPE,
        PERMISSIONS
    }
}
