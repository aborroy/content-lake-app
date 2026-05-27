package org.hyland.alfresco.contentlake.batch.service;

import org.alfresco.core.model.Node;
import org.alfresco.core.model.PermissionElement;
import org.alfresco.core.model.PermissionsInfo;
import org.hyland.alfresco.contentlake.batch.model.PermissionSyncRequest;
import org.hyland.alfresco.contentlake.batch.model.PermissionSyncResult;
import org.hyland.alfresco.contentlake.client.AlfrescoClient;
import org.hyland.alfresco.contentlake.client.AlfrescoSearchService;
import org.hyland.alfresco.contentlake.service.ContentLakeScopeResolver;
import org.hyland.contentlake.service.NodeSyncService;
import org.hyland.contentlake.spi.SourceNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.OffsetDateTime;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class PermissionSyncServiceTest {

    @Mock
    private ContentLakeScopeResolver scopeResolver;

    @Mock
    private NodeSyncService nodeSyncService;

    private StubAlfrescoClient alfrescoClient;
    private StubSearchService searchService;
    private PermissionSyncService permissionSyncService;

    @BeforeEach
    void setUp() {
        alfrescoClient = new StubAlfrescoClient();
        searchService = new StubSearchService();
        permissionSyncService = new PermissionSyncService(alfrescoClient, searchService, scopeResolver, nodeSyncService);
    }

    @Test
    void syncPermissions_updatesExplicitFileAclOnly() {
        Node file = file("file-1")
                .name("confidential-hr.txt")
                .modifiedAt(OffsetDateTime.parse("2026-03-30T09:10:00Z"))
                .permissions(new PermissionsInfo()
                        .isInheritanceEnabled(false)
                        .addLocallySetItem(allowed("user-a", "Consumer")));

        alfrescoClient.nodesById.put("file-1", file);
        when(scopeResolver.isInScope(file)).thenReturn(true);

        PermissionSyncResult result = permissionSyncService.syncPermissions(request("file-1"));

        ArgumentCaptor<SourceNode> sourceNode = ArgumentCaptor.forClass(SourceNode.class);
        verify(nodeSyncService).updatePermissions(sourceNode.capture());
        verify(nodeSyncService, never()).deleteNode(any(), any());
        assertThat(result).isEqualTo(new PermissionSyncResult(1, 0, 0, 0));
        assertThat(sourceNode.getValue().nodeId()).isEqualTo("file-1");
        assertThat(sourceNode.getValue().readPrincipals()).containsExactly("user-a");
    }

    @Test
    void syncPermissions_reconcilesFolderDescendantsRecursively() {
        Node folder = folder("folder-1");
        Node childA = file("file-a")
                .permissions(new PermissionsInfo()
                        .isInheritanceEnabled(false)
                        .addLocallySetItem(allowed("user-a", "Consumer")));
        Node childB = file("file-b")
                .permissions(new PermissionsInfo()
                        .isInheritanceEnabled(false)
                        .addLocallySetItem(allowed("user-b", "Consumer")));

        alfrescoClient.nodesById.put("folder-1", folder);
        // AFTS returns all descendants directly -- no recursive folder traversal needed
        searchService.descendantsByFolderId.put("folder-1", List.of(childA, childB));

        when(scopeResolver.shouldTraverse(folder)).thenReturn(true);
        when(scopeResolver.isInScope(childA)).thenReturn(true);
        when(scopeResolver.isInScope(childB)).thenReturn(true);

        PermissionSyncResult result = permissionSyncService.syncPermissions(request("folder-1"));

        ArgumentCaptor<SourceNode> sourceNode = ArgumentCaptor.forClass(SourceNode.class);
        verify(nodeSyncService, times(2)).updatePermissions(sourceNode.capture());
        assertThat(sourceNode.getAllValues())
                .extracting(SourceNode::nodeId)
                .containsExactlyInAnyOrder("file-a", "file-b");
        assertThat(result).isEqualTo(new PermissionSyncResult(2, 0, 0, 0));
    }

    @Test
    void syncPermissions_deletesOutOfScopeDescendants() {
        Node folder = folder("folder-1");
        Node child = file("file-1")
                .modifiedAt(OffsetDateTime.parse("2026-03-30T09:12:00Z"));

        alfrescoClient.nodesById.put("folder-1", folder);
        searchService.descendantsByFolderId.put("folder-1", List.of(child));

        when(scopeResolver.shouldTraverse(folder)).thenReturn(true);
        when(scopeResolver.isInScope(child)).thenReturn(false);

        PermissionSyncResult result = permissionSyncService.syncPermissions(request("folder-1"));

        verify(nodeSyncService).deleteNode("file-1", OffsetDateTime.parse("2026-03-30T09:12:00Z"));
        verify(nodeSyncService, never()).updatePermissions(any());
        assertThat(result).isEqualTo(new PermissionSyncResult(0, 1, 0, 0));
    }

    private static PermissionSyncRequest request(String nodeId) {
        PermissionSyncRequest request = new PermissionSyncRequest();
        request.setNodeIds(List.of(nodeId));
        request.setRecursive(true);
        return request;
    }

    private static PermissionElement allowed(String authorityId, String role) {
        return new PermissionElement()
                .authorityId(authorityId)
                .name(role)
                .accessStatus(PermissionElement.AccessStatusEnum.ALLOWED);
    }

    private static Node folder(String nodeId) {
        return new Node().id(nodeId).isFolder(true).isFile(false);
    }

    private static Node file(String nodeId) {
        return new Node().id(nodeId).isFolder(false).isFile(true);
    }

    private static final class StubSearchService extends AlfrescoSearchService {
        private final Map<String, List<Node>> descendantsByFolderId = new LinkedHashMap<>();

        private StubSearchService() {
            super(null, null, null);
        }

        @Override
        public List<Node> findDescendantFiles(String folderId, Collection<String> excludedAspects) {
            return descendantsByFolderId.getOrDefault(folderId, List.of());
        }
    }

    private static final class StubAlfrescoClient extends AlfrescoClient {
        private final Map<String, Node> nodesById = new LinkedHashMap<>();

        private StubAlfrescoClient() {
            super(null, null);
        }

        @Override
        public String getSourceId() {
            return "repo-main";
        }

        @Override
        public Node getAlfrescoNode(String nodeId) {
            return nodesById.get(nodeId);
        }
    }
}
