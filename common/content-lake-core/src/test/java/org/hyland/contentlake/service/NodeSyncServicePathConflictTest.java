package org.hyland.contentlake.service;

import org.hyland.contentlake.client.HxprDocumentApi;
import org.hyland.contentlake.client.HxprService;
import org.hyland.contentlake.hxpr.api.model.ACE;
import org.hyland.contentlake.model.HxprDocument;
import org.hyland.contentlake.service.chunking.SimpleChunkingService;
import org.hyland.contentlake.spi.ContentSourceClient;
import org.hyland.contentlake.spi.SourceNode;
import org.hyland.contentlake.spi.TextExtractor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.core.io.Resource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.web.client.HttpClientErrorException;

import java.nio.charset.StandardCharsets;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class NodeSyncServicePathConflictTest {

    private static final String SOURCE_ID = "default";
    private static final String FORMATTED_SOURCE_ID = "alfresco:default";
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
                Set.of(),
                Map.ofEntries(
                        Map.entry("source_nodeId", "node-123"),
                        Map.entry("source_type", "alfresco"),
                        Map.entry("source_path", "/Company Home/Sites/swsdp/documentLibrary/Meeting Notes"),
                        Map.entry("source_name", "Meeting Notes 2011-01-27.doc"),
                        Map.entry("source_mimeType", "application/msword"),
                        Map.entry("source_modifiedAt", "2026-03-13T12:00:00Z"),
                        Map.entry("alfresco_nodeId", "node-123"),
                        Map.entry("alfresco_repositoryId", SOURCE_ID),
                        Map.entry("alfresco_path", "/Company Home/Sites/swsdp/documentLibrary/Meeting Notes"),
                        Map.entry("alfresco_name", "Meeting Notes 2011-01-27.doc"),
                        Map.entry("alfresco_mimeType", "application/msword"),
                        Map.entry("alfresco_modifiedAt", "2026-03-13T12:00:00Z")
                )
        );

        HxprDocument existingAtPath = new HxprDocument();
        existingAtPath.setSysId("hxpr-123");

        HxprDocument updated = new HxprDocument();
        updated.setSysId("hxpr-123");
        updated.setCinId("node-123");
        updated.setCinSourceId(FORMATTED_SOURCE_ID);

        String parentPath = "/alfresco-sync/default/Company Home/Sites/swsdp/documentLibrary/Meeting Notes";
        String documentPath = parentPath + "/Meeting Notes 2011-01-27.doc";

        when(hxprService.findByNodeId("node-123", FORMATTED_SOURCE_ID)).thenReturn(null);
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
        assertThat(updatePayload.getCinSourceId()).isEqualTo(FORMATTED_SOURCE_ID);
        assertThat(updatePayload.getCinPaths()).containsExactly(documentPath);
        assertThat(updatePayload.getCinIngestProperties())
                .containsEntry("source_nodeId",        "node-123")
                .containsEntry("source_type",          "alfresco")
                .containsEntry("source_path",          "/Company Home/Sites/swsdp/documentLibrary/Meeting Notes")
                .containsEntry("alfresco_nodeId",       "node-123")
                .containsEntry("alfresco_repositoryId", SOURCE_ID)
                .containsEntry("alfresco_path",         "/Company Home/Sites/swsdp/documentLibrary/Meeting Notes");
    }

    @Test
    void buildDocument_mapsPrincipalsToSysAclWithCorrectFormat() {
        // GROUP_EVERYONE  → user ACE "__Everyone__" (no suffix — universal public principal)
        // GROUP_sales     → group ACE "GROUP_sales_#_<sourceId>"
        // bob             → user ACE  "bob_#_<sourceId>"
        SourceNode node = new SourceNode(
                "node-acl",
                SOURCE_ID,
                "alfresco",
                "report.pdf",
                "/docs",
                "application/pdf",
                OffsetDateTime.parse("2026-03-25T08:00:00Z"),
                false,
                Set.of("GROUP_EVERYONE", "GROUP_sales", "bob"),
                Set.of("GROUP_secret"),
                Map.of("source_nodeId", "node-acl")
        );

        HxprDocument created = new HxprDocument();
        created.setSysId("hxpr-acl");

        when(hxprService.findByNodeId("node-acl", FORMATTED_SOURCE_ID)).thenReturn(null);
        when(hxprService.findByPath(any())).thenReturn(null);
        when(hxprService.createDocument(any(), any(HxprDocument.class))).thenReturn(created);

        service.ingestMetadata(node);

        ArgumentCaptor<HxprDocument> captor = ArgumentCaptor.forClass(HxprDocument.class);
        verify(hxprService).createDocument(any(), captor.capture());

        HxprDocument payload = captor.getValue();
        List<ACE> sysAcl = payload.getSysAcl();
        assertThat(sysAcl).hasSize(3);
        assertThat(payload.getCinRead()).containsExactly("GROUP_EVERYONE", "GROUP_sales", "bob");
        assertThat(payload.getCinDeny()).containsExactly("GROUP_secret");

        assertThat(sysAcl).anyMatch(ace ->
                ace.getUser() != null
                && "__Everyone__".equals(ace.getUser().getId())
                && Boolean.TRUE.equals(ace.getGranted())
                && "Read".equals(ace.getPermission()));

        assertThat(sysAcl).anyMatch(ace ->
                ace.getGroup() != null
                && ("GROUP_sales_#_" + SOURCE_ID).equals(ace.getGroup().getId())
                && Boolean.TRUE.equals(ace.getGranted())
                && "Read".equals(ace.getPermission()));

        assertThat(sysAcl).anyMatch(ace ->
                ace.getUser() != null
                && ("bob_#_" + SOURCE_ID).equals(ace.getUser().getId())
                && Boolean.TRUE.equals(ace.getGranted())
                && "Read".equals(ace.getPermission()));
    }

    @Test
    void updatePermissions_patchesSysAclCinReadAndCinDenyWithoutContentChanges() {
        SourceNode node = new SourceNode(
                "node-acl",
                SOURCE_ID,
                "alfresco",
                "report.pdf",
                "/docs",
                "application/pdf",
                OffsetDateTime.parse("2026-03-25T08:00:00Z"),
                false,
                Set.of("GROUP_EVERYONE", "GROUP_sales", "bob"),
                Set.of("GROUP_secret"),
                Map.of("source_nodeId", "node-acl")
        );

        HxprDocument existing = new HxprDocument();
        existing.setSysId("hxpr-acl");
        when(hxprService.findByNodeId("node-acl", FORMATTED_SOURCE_ID)).thenReturn(existing);

        service.updatePermissions(node);

        ArgumentCaptor<HxprDocument> captor = ArgumentCaptor.forClass(HxprDocument.class);
        verify(documentApi).updateById(eq("hxpr-acl"), captor.capture());

        HxprDocument update = captor.getValue();
        assertThat(update.getCinRead()).containsExactly("GROUP_EVERYONE", "GROUP_sales", "bob");
        assertThat(update.getCinDeny()).containsExactly("GROUP_secret");
        assertThat(update.getSysAcl()).hasSize(3);
        assertThat(update.getSysFulltextBinary()).isNull();
        assertThat(update.getSysembedEmbeddings()).isNull();
    }

    @Test
    void updatePermissions_whenDocumentMissing_createsMetadataOnlyDocumentForFile() {
        SourceNode node = new SourceNode(
                "node-new",
                SOURCE_ID,
                "alfresco",
                "restricted.txt",
                "/restricted",
                "text/plain",
                OffsetDateTime.parse("2026-03-30T09:30:00Z"),
                false,
                Set.of("user-a"),
                Set.of(),
                Map.of(
                        "source_nodeId", "node-new",
                        "source_type", "alfresco",
                        "source_path", "/restricted",
                        "source_name", "restricted.txt",
                        "source_mimeType", "text/plain",
                        "source_modifiedAt", "2026-03-30T09:30:00Z"
                )
        );

        HxprDocument created = new HxprDocument();
        created.setSysId("hxpr-new");

        when(hxprService.findByNodeId("node-new", FORMATTED_SOURCE_ID)).thenReturn(null);
        when(hxprService.findByPath("/alfresco-sync/default/restricted/restricted.txt")).thenReturn(null);
        when(hxprService.createDocument(eq("/alfresco-sync/default/restricted"), any(HxprDocument.class)))
                .thenReturn(created);

        service.updatePermissions(node);

        ArgumentCaptor<HxprDocument> captor = ArgumentCaptor.forClass(HxprDocument.class);
        verify(hxprService).ensureFolder("/alfresco-sync/default/restricted");
        verify(hxprService).createDocument(eq("/alfresco-sync/default/restricted"), captor.capture());
        verify(documentApi, never()).updateById(any(), any());
        verify(hxprService, never()).updateEmbeddings(any(), any());

        HxprDocument payload = captor.getValue();
        assertThat(payload.getCinId()).isEqualTo("node-new");
        assertThat(payload.getCinSourceId()).isEqualTo(FORMATTED_SOURCE_ID);
        assertThat(payload.getCinRead()).containsExactly("user-a");
        assertThat(payload.getSysAcl()).hasSize(1);
        assertThat(payload.getSysFulltextBinary()).isNull();
    }

    @Test
    void updatePermissions_whenDocumentMissingForFolder_skipsFallbackCreation() {
        SourceNode folder = new SourceNode(
                "folder-acl",
                SOURCE_ID,
                "alfresco",
                "Restricted Folder",
                "/restricted",
                null,
                OffsetDateTime.parse("2026-03-30T09:31:00Z"),
                true,
                Set.of("user-a"),
                Set.of(),
                Map.of("source_nodeId", "folder-acl")
        );

        when(hxprService.findByNodeId("folder-acl", FORMATTED_SOURCE_ID)).thenReturn(null);

        service.updatePermissions(folder);

        verify(hxprService, never()).createDocument(any(), any());
        verify(documentApi, never()).updateById(any(), any());
        verify(hxprService, never()).updateEmbeddings(any(), any());
    }

    @Test
    void ingestMetadata_whenStale_refreshesPermissionsWithoutReprocessingContent() {
        SourceNode node = new SourceNode(
                "node-stale",
                SOURCE_ID,
                "alfresco",
                "restricted.txt",
                "/restricted",
                "text/plain",
                OffsetDateTime.parse("2026-03-30T09:30:00Z"),
                false,
                Set.of("user-a"),
                Set.of("GROUP_secret"),
                Map.of(
                        "source_nodeId", "node-stale",
                        "source_modifiedAt", "2026-03-30T09:30:00Z"
                )
        );

        HxprDocument existing = new HxprDocument();
        existing.setSysId("hxpr-stale");
        existing.setCinIngestProperties(Map.of("source_modifiedAt", "2026-03-30T09:30:00Z"));
        when(hxprService.findByNodeId("node-stale", FORMATTED_SOURCE_ID)).thenReturn(existing);

        NodeSyncService.SyncResult result = service.ingestMetadata(node);

        assertThat(result.hxprDocId()).isEqualTo("hxpr-stale");
        assertThat(result.skipped()).isTrue();

        ArgumentCaptor<HxprDocument> captor = ArgumentCaptor.forClass(HxprDocument.class);
        verify(documentApi).updateById(eq("hxpr-stale"), captor.capture());
        verify(hxprService, never()).createDocument(any(), any());
        verify(hxprService, never()).updateEmbeddings(any(), any());

        HxprDocument update = captor.getValue();
        assertThat(update.getCinRead()).containsExactly("user-a");
        assertThat(update.getCinDeny()).containsExactly("GROUP_secret");
        assertThat(update.getSysAcl()).hasSize(1);
    }

    @Test
    void syncNode_whenStale_refreshesPermissionsWithoutReprocessingContent() {
        SourceNode node = new SourceNode(
                "node-stale-sync",
                SOURCE_ID,
                "alfresco",
                "restricted.txt",
                "/restricted",
                "text/plain",
                OffsetDateTime.parse("2026-03-30T09:30:00Z"),
                false,
                Set.of("user-a"),
                Set.of(),
                Map.of(
                        "source_nodeId", "node-stale-sync",
                        "source_modifiedAt", "2026-03-30T09:30:00Z"
                )
        );

        HxprDocument existing = new HxprDocument();
        existing.setSysId("hxpr-stale-sync");
        existing.setCinIngestProperties(Map.of("source_modifiedAt", "2026-03-30T09:30:00Z"));
        when(hxprService.findByNodeId("node-stale-sync", FORMATTED_SOURCE_ID)).thenReturn(existing);

        String sysId = service.syncNode(node);

        assertThat(sysId).isEqualTo("hxpr-stale-sync");
        verify(documentApi).updateById(eq("hxpr-stale-sync"), any(HxprDocument.class));
        verify(textExtractor, never()).extractText(anyString(), anyString());
        verify(textExtractor, never()).extractText(any(Resource.class), anyString());
        verify(embeddingService, never()).embedChunks(any());
        verify(hxprService, never()).updateEmbeddings(any(), any());
    }
}
