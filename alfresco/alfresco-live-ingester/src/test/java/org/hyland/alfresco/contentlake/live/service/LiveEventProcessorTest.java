package org.hyland.alfresco.contentlake.live.service;

import org.alfresco.core.model.Node;
import org.alfresco.repo.event.v1.model.DataAttributes;
import org.alfresco.repo.event.v1.model.NodeResource;
import org.alfresco.repo.event.v1.model.RepoEvent;
import org.alfresco.repo.event.v1.model.Resource;
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
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class LiveEventProcessorTest {

    @Mock
    private AlfrescoClient alfrescoClient;

    @Mock
    private NodeSyncService nodeSyncService;

    @Mock
    private ContentLakeScopeResolver scopeResolver;

    @Mock
    private FolderSubtreeReconciler folderSubtreeReconciler;

    @Mock
    private RecentEventDeduplicator deduplicator;

    @Mock
    private RepoEvent<DataAttributes<Resource>> event;

    @Mock
    private DataAttributes<Resource> data;

    @Mock
    private NodeResource resource;

    private LiveIngesterMetrics metrics;
    private LiveEventProcessor processor;

    @BeforeEach
    void setUp() {
        metrics = new LiveIngesterMetrics();
        processor = new LiveEventProcessor(
                alfrescoClient,
                nodeSyncService,
                scopeResolver,
                folderSubtreeReconciler,
                deduplicator,
                metrics
        );

        when(event.getData()).thenReturn(data);
        when(data.getResource()).thenReturn(resource);
    }

    @Test
    void processPermissionUpdate_forFile_updatesThatFileAclOnly() {
        Node file = new Node()
                .id("file-1")
                .name("confidential-hr.txt")
                .isFile(true)
                .isFolder(false)
                .modifiedAt(OffsetDateTime.parse("2026-03-30T09:00:00Z"));

        when(resource.getId()).thenReturn("file-1");
        when(deduplicator.shouldSkip(event, "file-1")).thenReturn(false);
        when(alfrescoClient.getAlfrescoNode("file-1")).thenReturn(file);
        when(scopeResolver.isInScope(file)).thenReturn(true);
        when(alfrescoClient.extractReadAuthorities(file)).thenReturn(Set.of("user-a"));
        when(alfrescoClient.getSourceId()).thenReturn("repo-main");

        processor.processPermissionUpdate(event);

        ArgumentCaptor<SourceNode> sourceNode = ArgumentCaptor.forClass(SourceNode.class);
        verify(nodeSyncService).updatePermissions(sourceNode.capture());
        verify(nodeSyncService, never()).syncNode(any());
        verify(folderSubtreeReconciler, never()).reconcile(any(), any());

        assertThat(sourceNode.getValue().nodeId()).isEqualTo("file-1");
        assertThat(sourceNode.getValue().readPrincipals()).containsExactly("user-a");
    }

    @Test
    void processPermissionUpdate_forFolder_reconcilesSubtreeInsteadOfUpdatingFolderItself() {
        OffsetDateTime modifiedAt = OffsetDateTime.parse("2026-03-30T09:05:00Z");
        Node folder = new Node()
                .id("folder-1")
                .name("HR")
                .isFolder(true)
                .isFile(false)
                .modifiedAt(modifiedAt);

        when(resource.getId()).thenReturn("folder-1");
        when(deduplicator.shouldSkip(event, "folder-1")).thenReturn(false);
        when(alfrescoClient.getAlfrescoNode("folder-1")).thenReturn(folder);
        when(scopeResolver.shouldTraverse(folder)).thenReturn(true);
        when(folderSubtreeReconciler.reconcilePermissions(folder, modifiedAt))
                .thenReturn(new FolderSubtreeReconciler.ReconciliationResult());

        processor.processPermissionUpdate(event);

        verify(folderSubtreeReconciler).reconcilePermissions(folder, modifiedAt);
        verify(nodeSyncService, never()).updatePermissions(any());
        verify(nodeSyncService, never()).syncNode(any());
    }
}
