package org.hyland.alfresco.contentlake.service;

import org.hyland.alfresco.contentlake.client.AlfrescoClient;
import org.hyland.alfresco.contentlake.client.AlfrescoSearchService;
import org.hyland.alfresco.contentlake.client.FolderStatusCounts;
import org.hyland.contentlake.client.HxprService;
import org.hyland.contentlake.model.ContentLakeIngestProperties;
import org.hyland.contentlake.model.ContentLakeNodeStatus;
import org.hyland.contentlake.model.HxprDocument;
import org.alfresco.core.model.Node;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;

@Service
public class ContentLakeNodeStatusService {

    private final AlfrescoClient alfrescoClient;
    private final HxprService hxprService;
    private final ContentLakeScopeResolver scopeResolver;
    private final AlfrescoSearchService searchService;
    private final Executor statusLookupExecutor;

    public ContentLakeNodeStatusService(AlfrescoClient alfrescoClient,
                                        HxprService hxprService,
                                        ContentLakeScopeResolver scopeResolver,
                                        AlfrescoSearchService searchService,
                                        @Qualifier("statusLookupExecutor") Executor statusLookupExecutor) {
        this.alfrescoClient = alfrescoClient;
        this.hxprService = hxprService;
        this.scopeResolver = scopeResolver;
        this.searchService = searchService;
        this.statusLookupExecutor = statusLookupExecutor;
    }

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

        List<CompletableFuture<Entry<String, Node>>> futures = sanitizedIds.stream()
                .map(nodeId -> CompletableFuture.supplyAsync(
                        () -> Map.entry(nodeId, alfrescoClient.getAlfrescoNode(nodeId)),
                        statusLookupExecutor))
                .toList();
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

        Map<String, Node> nodesById = futures.stream()
                .map(CompletableFuture::join)
                .filter(e -> e.getValue() != null)
                .collect(Collectors.toMap(Entry::getKey, Entry::getValue,
                        (a, b) -> a, LinkedHashMap::new));

        List<String> fileNodeIds = nodesById.values().stream()
                .filter(node -> !Boolean.TRUE.equals(node.isIsFolder()))
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

        FolderStatusCounts counts = searchService.getFolderStatusCounts(
                folderNode.getId(), scopeResolver.getExcludedAspects());

        if (counts.total() == 0) {
            return new ContentLakeNodeStatus(
                    folderNode.getId(),
                    ContentLakeNodeStatus.Status.INDEXED,
                    true, true, true, false, null,
                    new ContentLakeNodeStatus.FolderStatusSummary(0, 0, 0, 0)
            );
        }

        ContentLakeNodeStatus.Status folderStatus = counts.failed() > 0
                ? ContentLakeNodeStatus.Status.FAILED
                : (counts.pending() > 0 ? ContentLakeNodeStatus.Status.PENDING : ContentLakeNodeStatus.Status.INDEXED);

        String aggregateError = counts.failed() > 0 ? counts.failed() + " document(s) failed indexing" : null;

        return new ContentLakeNodeStatus(
                folderNode.getId(),
                folderStatus,
                true, true, true, false,
                aggregateError,
                new ContentLakeNodeStatus.FolderStatusSummary(
                        counts.total(),
                        counts.indexed(),
                        counts.pending(),
                        counts.failed()
                )
        );
    }

    private ContentLakeNodeStatus resolveFileStatus(Node node, HxprDocument document) {
        // Fast path: status is stored on the node itself -- no scope resolution or hxpr query needed.
        String nodeStatus = readNodeProperty(node, "cl:syncStatusValue");
        if (nodeStatus != null) {
            ContentLakeNodeStatus.Status status = parseNodeStatus(nodeStatus);
            String error = readNodeProperty(node, "cl:syncError");
            return new ContentLakeNodeStatus(node.getId(), status, true, false, true, false, error, null);
        }

        // Legacy fallback: node pre-dates write-back; derive status from hxpr document.
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

    private String readNodeProperty(Node node, String propertyName) {
        if (node.getProperties() instanceof Map<?, ?> props) {
            Object value = props.get(propertyName);
            return value != null ? value.toString() : null;
        }
        return null;
    }

    private ContentLakeNodeStatus.Status parseNodeStatus(String raw) {
        try {
            return ContentLakeNodeStatus.Status.valueOf(raw);
        } catch (IllegalArgumentException ignored) {
            return ContentLakeNodeStatus.Status.INDEXED;
        }
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
