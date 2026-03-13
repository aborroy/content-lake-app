package org.alfresco.contentlake.service;

import org.alfresco.contentlake.client.AlfrescoClient;
import org.alfresco.contentlake.client.HxprDocumentApi;
import org.alfresco.contentlake.client.HxprService;
import org.alfresco.contentlake.client.TransformClient;
import org.alfresco.contentlake.model.HxprDocument;
import org.alfresco.contentlake.service.chunking.SimpleChunkingService;
import org.alfresco.core.model.ContentInfo;
import org.alfresco.core.model.Node;
import org.alfresco.core.model.PathInfo;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.web.client.HttpClientErrorException;

import java.nio.charset.StandardCharsets;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class NodeSyncServicePathConflictTest {

    private static final String REPOSITORY_ID = "default";
    private static final String TARGET_PATH = "/alfresco-sync";

    @Mock
    private AlfrescoClient alfrescoClient;

    @Mock
    private HxprDocumentApi documentApi;

    @Mock
    private HxprService hxprService;

    @Mock
    private TransformClient transformClient;

    @Mock
    private EmbeddingService embeddingService;

    @Mock
    private SimpleChunkingService chunkingService;

    private NodeSyncService service;

    @BeforeEach
    void setUp() {
        service = new NodeSyncService(
                alfrescoClient,
                documentApi,
                hxprService,
                transformClient,
                embeddingService,
                chunkingService,
                TARGET_PATH,
                null
        );
    }

    @Test
    void ingestMetadata_whenCreateConflicts_reusesExistingDocumentAtPath() {
        Node node = new Node()
                .id("node-123")
                .name("Meeting Notes 2011-01-27.doc")
                .isFile(true)
                .isFolder(false)
                .path(new PathInfo().name("/Company Home/Sites/swsdp/documentLibrary/Meeting Notes"))
                .content(new ContentInfo().mimeType("application/msword"))
                .modifiedAt(OffsetDateTime.parse("2026-03-13T12:00:00Z"));

        HxprDocument existingAtPath = new HxprDocument();
        existingAtPath.setSysId("hxpr-123");

        HxprDocument updated = new HxprDocument();
        updated.setSysId("hxpr-123");
        updated.setCinId("node-123");
        updated.setCinSourceId(REPOSITORY_ID);

        String parentPath = "/alfresco-sync/default/Company Home/Sites/swsdp/documentLibrary/Meeting Notes";
        String documentPath = parentPath + "/Meeting Notes 2011-01-27.doc";

        when(alfrescoClient.getRepositoryId()).thenReturn(REPOSITORY_ID);
        when(alfrescoClient.extractReadAuthorities(node)).thenReturn(Set.of("GROUP_EVERYONE"));
        when(hxprService.findByNodeId("node-123", REPOSITORY_ID)).thenReturn(null);
        when(hxprService.findByPath(documentPath)).thenReturn(null, existingAtPath);
        when(hxprService.createDocument(eq(parentPath), any(HxprDocument.class)))
                .thenThrow(HttpClientErrorException.create(
                        HttpStatus.CONFLICT,
                        "Conflict",
                        HttpHeaders.EMPTY,
                        "{\"message\":\"Duplicate name in parent\"}".getBytes(StandardCharsets.UTF_8),
                        StandardCharsets.UTF_8
                ));
        when(documentApi.updateById(eq("hxpr-123"), any(HxprDocument.class))).thenReturn(updated);

        NodeSyncService.SyncResult result = service.ingestMetadata(node);

        assertThat(result.hxprDocId()).isEqualTo("hxpr-123");
        assertThat(result.nodeId()).isEqualTo("node-123");
        assertThat(result.skipped()).isFalse();

        ArgumentCaptor<HxprDocument> captor = ArgumentCaptor.forClass(HxprDocument.class);
        verify(documentApi).updateById(eq("hxpr-123"), captor.capture());
        verify(hxprService).ensureFolder(parentPath);
        verify(hxprService).createDocument(eq(parentPath), any(HxprDocument.class));
        verify(hxprService, never()).updateEmbeddings(any(), any());

        HxprDocument updatePayload = captor.getValue();
        assertThat(updatePayload.getSysId()).isEqualTo("hxpr-123");
        assertThat(updatePayload.getCinId()).isEqualTo("node-123");
        assertThat(updatePayload.getCinSourceId()).isEqualTo(REPOSITORY_ID);
        assertThat(updatePayload.getCinPaths()).containsExactly(documentPath);
        assertThat(updatePayload.getCinIngestProperties())
                .containsEntry("alfresco_nodeId", "node-123")
                .containsEntry("alfresco_repositoryId", REPOSITORY_ID)
                .containsEntry("alfresco_path", "/Company Home/Sites/swsdp/documentLibrary/Meeting Notes");
    }
}
