package org.hyland.alfresco.contentlake.live.service;

import org.alfresco.core.model.Node;
import org.alfresco.core.model.PermissionElement;
import org.alfresco.core.model.PermissionsInfo;
import org.hyland.alfresco.contentlake.client.AlfrescoClient;
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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class FolderSubtreeReconcilerTest {

    @Mock
    private ContentLakeScopeResolver scopeResolver;

    @Mock
    private NodeSyncService nodeSyncService;

    private StubAlfrescoClient alfrescoClient;
    private FolderSubtreeReconciler reconciler;

    @BeforeEach
    void setUp() {
        alfrescoClient = new StubAlfrescoClient();
        reconciler = new FolderSubtreeReconciler(alfrescoClient, scopeResolver, nodeSyncService);
    }

    @Test
    void reconcile_syncsInScopeDescendantsForScopeChanges() {
        Node folder = folder("folder-1");
        Node child = file("file-1")
                .name("confidential-hr.txt")
                .modifiedAt(OffsetDateTime.parse("2026-03-30T09:10:00Z"))
                .permissions(new PermissionsInfo()
                        .isInheritanceEnabled(false)
                        .addLocallySetItem(allowed("user-a", "Consumer")));

        alfrescoClient.childrenByFolderId.put("folder-1", List.of(child));
        when(scopeResolver.isInScope(child)).thenReturn(true);

        reconciler.reconcile(folder, OffsetDateTime.parse("2026-03-30T09:11:00Z"));

        verify(nodeSyncService).syncNode(any());
        verify(nodeSyncService, never()).updatePermissions(any());
    }

    @Test
    void reconcilePermissions_updatesInScopeDescendantsViaAclOnlyPath() {
        Node folder = folder("folder-1");
        Node child = file("file-1")
                .name("confidential-hr.txt")
                .modifiedAt(OffsetDateTime.parse("2026-03-30T09:10:00Z"))
                .permissions(new PermissionsInfo()
                        .isInheritanceEnabled(false)
                        .addLocallySetItem(allowed("user-a", "Consumer")));

        alfrescoClient.childrenByFolderId.put("folder-1", List.of(child));
        when(scopeResolver.isInScope(child)).thenReturn(true);

        reconciler.reconcilePermissions(folder, OffsetDateTime.parse("2026-03-30T09:11:00Z"));

        verify(nodeSyncService, never()).syncNode(any());
        verify(nodeSyncService).updatePermissions(any());
    }

    @Test
    void reconcilePermissions_keepsChildLocalAclWhenInheritanceIsDisabled() {
        Node folder = folder("folder-1");
        Node child = file("file-2")
                .name("tech-spec-restricted.txt")
                .modifiedAt(OffsetDateTime.parse("2026-03-30T09:12:00Z"))
                .permissions(new PermissionsInfo()
                        .isInheritanceEnabled(false)
                        .addInheritedItem(allowed("GROUP_parent", "Consumer"))
                        .addLocallySetItem(allowed("user-b", "Consumer")));

        alfrescoClient.childrenByFolderId.put("folder-1", List.of(child));
        when(scopeResolver.isInScope(child)).thenReturn(true);

        reconciler.reconcilePermissions(folder, OffsetDateTime.parse("2026-03-30T09:13:00Z"));

        ArgumentCaptor<SourceNode> sourceNode = ArgumentCaptor.forClass(SourceNode.class);
        verify(nodeSyncService).updatePermissions(sourceNode.capture());
        verify(nodeSyncService, never()).syncNode(any());

        assertThat(sourceNode.getValue().nodeId()).isEqualTo("file-2");
        assertThat(sourceNode.getValue().readPrincipals()).containsExactly("user-b");
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

    private static final class StubAlfrescoClient extends AlfrescoClient {
        private final Map<String, List<Node>> childrenByFolderId = new LinkedHashMap<>();

        private StubAlfrescoClient() {
            super(null, null);
        }

        @Override
        public String getSourceId() {
            return "repo-main";
        }

        @Override
        public List<Node> getAllChildren(String folderId) {
            return childrenByFolderId.getOrDefault(folderId, List.of());
        }
    }
}
