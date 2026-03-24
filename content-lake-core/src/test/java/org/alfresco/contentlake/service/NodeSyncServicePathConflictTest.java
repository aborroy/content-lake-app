package org.alfresco.contentlake.service;

import org.alfresco.contentlake.client.HxprDocumentApi;
import org.alfresco.contentlake.client.HxprService;
import org.alfresco.contentlake.model.HxprDocument;
import org.alfresco.contentlake.service.chunking.SimpleChunkingService;
import org.alfresco.contentlake.spi.ContentSourceClient;
import org.alfresco.contentlake.spi.SourceNode;
import org.alfresco.contentlake.spi.TextExtractor;
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
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class NodeSyncServicePathConflictTest {

    private static final String SOURCE_ID  = "default";
    private static final String TARGET_PATH = "/alfresco-sync";

    @Mock
    private ContentSourceClient sourceClient;

    @Mock
    private HxprDocumentApi documentApi;

    @Mock
    private HxprService hxprService;

    @Mock
    private TextExtractor textExtractor;

    @Mock
    private EmbeddingService embeddingService;

    @Mock
    private SimpleChunkingService chunkingService;

    private NodeSyncService service;

    @BeforeEach
    void setUp() {
        service = new NodeSyncService(
                sourceClient,
                documentApi,
                hxprService,
                textExtractor,
                embeddingService,
                chunkingService,
                TARGET_PATH,
                null
        );
    }

    @Test
    void ingestMetadata_whenCreateConflicts_reusesExistingDocumentAtPath() {
        SourceNode node = new SourceNode(
                "node-123",
                SOURCE_ID,
                "alfresco",
                "Meeting Notes 2011-01-27.doc",
                "/Company Home/Sites/swsdp/documentLibrary/Meeting Notes",
                "application/msword",
                OffsetDateTime.parse("2026-03-13T12:00:00Z"),
                false,
                Set.of("GROUP_EVERYONE"),
                Map.of(
                        "alfresco_nodeId",       "node-123",
                        "alfresco_repositoryId", SOURCE_ID,
                        "alfresco_path",         "/Company Home/Sites/swsdp/documentLibrary/Meeting Notes",
                        "alfresco_name",         "Meeting Notes 2011-01-27.doc",
                        "alfresco_mimeType",     "application/msword",
                        "alfresco_modifiedAt",   "2026-03-13T12:00:00Z"
                )
        );

        HxprDocument existingAtPath = new HxprDocument();
        existingAtPath.setSysId("hxpr-123");

        HxprDocument updated = new HxprDocument();
        updated.setSysId("hxpr-123");
        updated.setCinId("node-123");
        updated.setCinSourceId(SOURCE_ID);

        String parentPath = "/alfresco-sync/default/Company Home/Sites/swsdp/documentLibrary/Meeting Notes";
        String documentPath = parentPath + "/Meeting Notes 2011-01-27.doc";

        when(hxprService.findByNodeId("node-123", SOURCE_ID)).thenReturn(null);
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
        assertThat(updatePayload.getCinSourceId()).isEqualTo(SOURCE_ID);
        assertThat(updatePayload.getCinPaths()).containsExactly(documentPath);
        assertThat(updatePayload.getCinIngestProperties())
                .containsEntry("alfresco_nodeId",       "node-123")
                .containsEntry("alfresco_repositoryId", SOURCE_ID)
                .containsEntry("alfresco_path",         "/Company Home/Sites/swsdp/documentLibrary/Meeting Notes");
    }
}
