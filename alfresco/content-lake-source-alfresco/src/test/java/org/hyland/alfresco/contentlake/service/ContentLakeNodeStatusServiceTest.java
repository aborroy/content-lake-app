package org.hyland.alfresco.contentlake.service;

import org.hyland.alfresco.contentlake.client.AlfrescoClient;
import org.hyland.contentlake.client.HxprService;
import org.hyland.contentlake.model.ContentLakeIngestProperties;
import org.hyland.contentlake.model.ContentLakeNodeStatus;
import org.hyland.contentlake.model.HxprDocument;
import org.alfresco.core.model.Node;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class ContentLakeNodeStatusServiceTest {

    private StubAlfrescoClient alfrescoClient;
    private StubHxprService hxprService;
    private StubScopeResolver scopeResolver;
    private ContentLakeNodeStatusService service;

    @BeforeEach
    void setUp() {
        alfrescoClient = new StubAlfrescoClient();
        hxprService = new StubHxprService();
        scopeResolver = new StubScopeResolver(alfrescoClient);
        service = new ContentLakeNodeStatusService(alfrescoClient, hxprService, scopeResolver);
    }

    @Test
    void getNodeStatuses_folderWithoutAggregate_keepsLegacyNullStatus() {
        Node folder = folder("folder-1");
        alfrescoClient.nodesById.put("folder-1", folder);
        scopeResolver.folderInScopeIds.add("folder-1");

        Map<String, ContentLakeNodeStatus> results = service.getNodeStatuses(List.of("folder-1"), false);
        ContentLakeNodeStatus status = results.get("folder-1");

        assertThat(status).isNotNull();
        assertThat(status.status()).isNull();
        assertThat(status.folder()).isTrue();
        assertThat(status.inScope()).isTrue();
        assertThat(status.folderSummary()).isNull();
        assertThat(alfrescoClient.getAllChildrenCalls).isZero();
        assertThat(hxprService.batchCalls).isEmpty();
    }

    @Test
    void getNodeStatuses_folderWithAggregate_computesSummaryAndWorstStatus() {
        Node folder = folder("folder-1");
        Node subFolder = folder("folder-2");
        Node indexedFile = file("file-indexed");
        Node failedFile = file("file-failed");
        Node pendingFile = file("file-pending");

        alfrescoClient.nodesById.put("folder-1", folder);
        scopeResolver.folderInScopeIds.add("folder-1");
        scopeResolver.traversableFolderIds.add("folder-2");
        scopeResolver.fileInScopeIds.addAll(Set.of("file-indexed", "file-failed", "file-pending"));

        alfrescoClient.childrenByFolderId.put("folder-1", List.of(indexedFile, failedFile, subFolder));
        alfrescoClient.childrenByFolderId.put("folder-2", List.of(pendingFile));

        hxprService.documentsByNodeId.put("file-indexed", statusDoc(ContentLakeNodeStatus.Status.INDEXED, null));
        hxprService.documentsByNodeId.put("file-failed", statusDoc(ContentLakeNodeStatus.Status.FAILED, "transform failed"));

        Map<String, ContentLakeNodeStatus> results = service.getNodeStatuses(List.of("folder-1"), true);
        ContentLakeNodeStatus status = results.get("folder-1");

        assertThat(status).isNotNull();
        assertThat(status.status()).isEqualTo(ContentLakeNodeStatus.Status.FAILED);
        assertThat(status.error()).isEqualTo("1 document(s) failed indexing");
        assertThat(status.folderSummary()).isNotNull();
        assertThat(status.folderSummary().totalDocuments()).isEqualTo(3);
        assertThat(status.folderSummary().indexedDocuments()).isEqualTo(1);
        assertThat(status.folderSummary().pendingDocuments()).isEqualTo(1);
        assertThat(status.folderSummary().failedDocuments()).isEqualTo(1);
        assertThat(alfrescoClient.getAllChildrenCalls).isEqualTo(2);
        assertThat(hxprService.batchCalls).containsExactly(List.of("file-indexed", "file-failed", "file-pending"));
    }

    @Test
    void getNodeStatuses_folderWithAggregate_outOfScopeDoesNotTraverse() {
        Node folder = folder("folder-1");
        alfrescoClient.nodesById.put("folder-1", folder);
        scopeResolver.excludedIds.add("folder-1");

        Map<String, ContentLakeNodeStatus> results = service.getNodeStatuses(List.of("folder-1"), true);
        ContentLakeNodeStatus status = results.get("folder-1");

        assertThat(status).isNotNull();
        assertThat(status.status()).isNull();
        assertThat(status.inScope()).isFalse();
        assertThat(status.excluded()).isTrue();
        assertThat(status.folderSummary()).isNull();
        assertThat(alfrescoClient.getAllChildrenCalls).isZero();
        assertThat(hxprService.batchCalls).isEmpty();
    }

    private static Node folder(String nodeId) {
        return new Node().id(nodeId).isFolder(true).isFile(false);
    }

    private static Node file(String nodeId) {
        return new Node().id(nodeId).isFolder(false).isFile(true);
    }

    private static HxprDocument statusDoc(ContentLakeNodeStatus.Status status, String error) {
        HxprDocument doc = new HxprDocument();
        Map<String, Object> ingestProperties = new LinkedHashMap<>();
        ingestProperties.put(ContentLakeIngestProperties.CONTENT_LAKE_SYNC_STATUS, status.name());
        if (error != null) {
            ingestProperties.put(ContentLakeIngestProperties.CONTENT_LAKE_SYNC_ERROR, error);
        }
        doc.setCinIngestProperties(ingestProperties);
        return doc;
    }

    private static final class StubAlfrescoClient extends AlfrescoClient {
        private final Map<String, Node> nodesById = new LinkedHashMap<>();
        private final Map<String, List<Node>> childrenByFolderId = new LinkedHashMap<>();
        private int getAllChildrenCalls = 0;

        private StubAlfrescoClient() {
            super(null, null);
        }

        @Override
        public String getRepositoryId() {
            return "repo-main";
        }

        @Override
        public Node getAlfrescoNode(String nodeId) {
            return nodesById.get(nodeId);
        }

        @Override
        public List<Node> getAllChildren(String folderId) {
            getAllChildrenCalls++;
            return childrenByFolderId.getOrDefault(folderId, List.of());
        }
    }

    private static final class StubHxprService extends HxprService {
        private final Map<String, HxprDocument> documentsByNodeId = new LinkedHashMap<>();
        private final List<List<String>> batchCalls = new ArrayList<>();

        private StubHxprService() {
            super(null, null, null);
        }

        @Override
        public Map<String, HxprDocument> findByNodeIds(Collection<String> nodeIds, String sourceId) {
            List<String> orderedNodeIds = List.copyOf(nodeIds);
            batchCalls.add(orderedNodeIds);

            Map<String, HxprDocument> results = new LinkedHashMap<>();
            for (String nodeId : orderedNodeIds) {
                HxprDocument document = documentsByNodeId.get(nodeId);
                if (document != null) {
                    results.put(nodeId, document);
                }
            }
            return results;
        }
    }

    private static final class StubScopeResolver extends ContentLakeScopeResolver {
        private final Set<String> folderInScopeIds = new LinkedHashSet<>();
        private final Set<String> fileInScopeIds = new LinkedHashSet<>();
        private final Set<String> excludedIds = new LinkedHashSet<>();
        private final Set<String> traversableFolderIds = new LinkedHashSet<>();

        private StubScopeResolver(AlfrescoClient alfrescoClient) {
            super(List.of(), List.of(), alfrescoClient);
        }

        @Override
        public boolean shouldTraverse(Node node) {
            return traversableFolderIds.contains(node.getId());
        }

        @Override
        public boolean isInScope(Node node) {
            return fileInScopeIds.contains(node.getId());
        }

        @Override
        public boolean isFolderInScope(Node node) {
            return folderInScopeIds.contains(node.getId()) && !excludedIds.contains(node.getId());
        }

        @Override
        public boolean isExcludedBySelfOrAncestor(Node node) {
            return excludedIds.contains(node.getId());
        }
    }
}
