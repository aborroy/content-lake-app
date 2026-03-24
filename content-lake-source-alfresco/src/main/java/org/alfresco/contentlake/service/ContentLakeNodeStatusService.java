package org.alfresco.contentlake.service;

import lombok.RequiredArgsConstructor;
import org.alfresco.contentlake.client.AlfrescoClient;
import org.alfresco.contentlake.client.HxprService;
import org.alfresco.contentlake.model.ContentLakeIngestProperties;
import org.alfresco.contentlake.model.ContentLakeNodeStatus;
import org.alfresco.contentlake.model.HxprDocument;
import org.alfresco.core.model.Node;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Service
@RequiredArgsConstructor
public class ContentLakeNodeStatusService {

    private static final int HXPR_LOOKUP_BATCH_SIZE = 500;

    private final AlfrescoClient alfrescoClient;
    private final HxprService hxprService;
    private final ContentLakeScopeResolver scopeResolver;

    public ContentLakeNodeStatus getNodeStatus(String nodeId) {
        return getNodeStatus(nodeId, false);
    }

    public ContentLakeNodeStatus getNodeStatus(String nodeId, boolean includeFolderAggregate) {
        return getNodeStatuses(List.of(nodeId), includeFolderAggregate).get(nodeId);
    }

    public Map<String, ContentLakeNodeStatus> getNodeStatuses(Collection<String> nodeIds) {
        return getNodeStatuses(nodeIds, false);
    }

    public Map<String, ContentLakeNodeStatus> getNodeStatuses(Collection<String> nodeIds, boolean includeFolderAggregate) {
        List<String> sanitizedIds = nodeIds == null
                ? List.of()
                : nodeIds.stream()
                .filter(nodeId -> nodeId != null && !nodeId.isBlank())
                .distinct()
                .toList();

        String sourceId = formatSourceId(alfrescoClient.getSourceType(), alfrescoClient.getSourceId());
        Map<String, Node> nodesById = new LinkedHashMap<>();
        List<String> fileNodeIds = sanitizedIds.stream()
                .map(nodeId -> {
                    Node node = alfrescoClient.getAlfrescoNode(nodeId);
                    nodesById.put(nodeId, node);
                    return node;
                })
                .filter(node -> node != null && !Boolean.TRUE.equals(node.isIsFolder()))
                .map(Node::getId)
                .toList();

        Map<String, HxprDocument> documentsByNodeId = fileNodeIds.isEmpty()
                ? Map.of()
                : hxprService.findByNodeIds(fileNodeIds, sourceId);
        Map<String, ContentLakeNodeStatus> statusesByNodeId = new LinkedHashMap<>();

        for (String nodeId : sanitizedIds) {
            Node node = nodesById.get(nodeId);
            if (node == null) {
                statusesByNodeId.put(nodeId, new ContentLakeNodeStatus(nodeId, null, false, false, false, false, null));
                continue;
            }

            if (Boolean.TRUE.equals(node.isIsFolder())) {
                statusesByNodeId.put(nodeId, resolveFolderStatus(node, includeFolderAggregate, sourceId));
            } else {
                statusesByNodeId.put(nodeId, resolveFileStatus(node, documentsByNodeId.get(nodeId)));
            }
        }

        return statusesByNodeId;
    }

    private ContentLakeNodeStatus resolveFolderStatus(Node folderNode, boolean includeFolderAggregate, String repositoryId) {
        boolean excluded = scopeResolver.isExcludedBySelfOrAncestor(folderNode);
        boolean inScope = scopeResolver.isFolderInScope(folderNode);

        if (!includeFolderAggregate || !inScope) {
            return new ContentLakeNodeStatus(folderNode.getId(), null, true, true, inScope, excluded, null);
        }

        List<String> inScopeFileIds = collectInScopeFileIds(folderNode.getId());
        if (inScopeFileIds.isEmpty()) {
            return new ContentLakeNodeStatus(
                    folderNode.getId(),
                    ContentLakeNodeStatus.Status.INDEXED,
                    true,
                    true,
                    true,
                    false,
                    null,
                    new ContentLakeNodeStatus.FolderStatusSummary(0, 0, 0, 0)
            );
        }

        Map<String, HxprDocument> subtreeDocuments = findByNodeIdsInChunks(inScopeFileIds, repositoryId);
        long indexedCount = 0;
        long pendingCount = 0;
        long failedCount = 0;

        for (String fileId : inScopeFileIds) {
            ContentLakeNodeStatus.Status fileStatus = readStoredStatus(subtreeDocuments.get(fileId));
            if (fileStatus == null) {
                fileStatus = ContentLakeNodeStatus.Status.PENDING;
            }

            switch (fileStatus) {
                case INDEXED -> indexedCount++;
                case FAILED -> failedCount++;
                case PENDING -> pendingCount++;
            }
        }

        ContentLakeNodeStatus.Status folderStatus = failedCount > 0
                ? ContentLakeNodeStatus.Status.FAILED
                : (pendingCount > 0 ? ContentLakeNodeStatus.Status.PENDING : ContentLakeNodeStatus.Status.INDEXED);

        String aggregateError = failedCount > 0 ? failedCount + " document(s) failed indexing" : null;

        return new ContentLakeNodeStatus(
                folderNode.getId(),
                folderStatus,
                true,
                true,
                true,
                false,
                aggregateError,
                new ContentLakeNodeStatus.FolderStatusSummary(
                        inScopeFileIds.size(),
                        indexedCount,
                        pendingCount,
                        failedCount
                )
        );
    }

    private ContentLakeNodeStatus resolveFileStatus(Node node, HxprDocument document) {
        boolean excluded = scopeResolver.isExcludedBySelfOrAncestor(node);
        boolean inScope = scopeResolver.isInScope(node);
        if (!inScope) {
            return new ContentLakeNodeStatus(node.getId(), null, true, false, false, excluded, null);
        }

        ContentLakeNodeStatus.Status status = readStoredStatus(document);
        if (status == null) {
            status = ContentLakeNodeStatus.Status.PENDING;
        }

        return new ContentLakeNodeStatus(
                node.getId(),
                status,
                true,
                false,
                true,
                false,
                readStoredError(document),
                null
        );
    }

    private List<String> collectInScopeFileIds(String rootFolderId) {
        Set<String> visitedFolders = new LinkedHashSet<>();
        Set<String> fileIds = new LinkedHashSet<>();
        collectInScopeFileIds(rootFolderId, visitedFolders, fileIds);
        return new ArrayList<>(fileIds);
    }

    private void collectInScopeFileIds(String folderId, Set<String> visitedFolders, Set<String> fileIds) {
        if (folderId == null || folderId.isBlank() || !visitedFolders.add(folderId)) {
            return;
        }

        List<Node> children = alfrescoClient.getAllChildren(folderId);
        for (Node child : children) {
            if (child == null || child.getId() == null || child.getId().isBlank()) {
                continue;
            }

            if (Boolean.TRUE.equals(child.isIsFolder())) {
                if (!scopeResolver.shouldTraverse(child) || scopeResolver.isExcludedBySelfOrAncestor(child)) {
                    continue;
                }
                collectInScopeFileIds(child.getId(), visitedFolders, fileIds);
                continue;
            }

            if (scopeResolver.isInScope(child)) {
                fileIds.add(child.getId());
            }
        }
    }

    private Map<String, HxprDocument> findByNodeIdsInChunks(List<String> nodeIds, String repositoryId) {
        if (nodeIds == null || nodeIds.isEmpty()) {
            return Map.of();
        }

        Map<String, HxprDocument> documentsByNodeId = new LinkedHashMap<>();
        for (int offset = 0; offset < nodeIds.size(); offset += HXPR_LOOKUP_BATCH_SIZE) {
            int limit = Math.min(offset + HXPR_LOOKUP_BATCH_SIZE, nodeIds.size());
            List<String> batch = nodeIds.subList(offset, limit);
            documentsByNodeId.putAll(hxprService.findByNodeIds(batch, repositoryId));
        }
        return documentsByNodeId;
    }

    private String formatSourceId(String sourceType, String sourceId) {
        if (sourceId == null || sourceId.isBlank()) {
            return sourceId;
        }
        if (sourceType == null || sourceType.isBlank() || sourceId.contains(":")) {
            return sourceId;
        }
        return sourceType + ":" + sourceId;
    }

    private ContentLakeNodeStatus.Status readStoredStatus(HxprDocument document) {
        if (document == null) {
            return null;
        }

        Map<String, Object> ingestProperties = document.getCinIngestProperties();
        if (ingestProperties == null) {
            return ContentLakeNodeStatus.Status.INDEXED;
        }

        Object rawStatus = ingestProperties.get(ContentLakeIngestProperties.CONTENT_LAKE_SYNC_STATUS);
        if (rawStatus == null) {
            return ContentLakeNodeStatus.Status.INDEXED;
        }

        try {
            return ContentLakeNodeStatus.Status.valueOf(rawStatus.toString());
        } catch (IllegalArgumentException ignored) {
            return ContentLakeNodeStatus.Status.INDEXED;
        }
    }

    private String readStoredError(HxprDocument document) {
        if (document == null || document.getCinIngestProperties() == null) {
            return null;
        }

        Object rawError = document.getCinIngestProperties().get(ContentLakeIngestProperties.CONTENT_LAKE_SYNC_ERROR);
        return rawError != null ? rawError.toString() : null;
    }
}
