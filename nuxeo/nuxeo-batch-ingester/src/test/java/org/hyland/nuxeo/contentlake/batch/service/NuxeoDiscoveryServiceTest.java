package org.hyland.nuxeo.contentlake.batch.service;

import org.hyland.nuxeo.contentlake.client.NuxeoClient;
import org.hyland.nuxeo.contentlake.config.NuxeoProperties;
import org.hyland.nuxeo.contentlake.model.NuxeoDocument;
import org.hyland.nuxeo.contentlake.batch.model.NuxeoSyncRequest;
import org.hyland.contentlake.spi.SourceNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class NuxeoDiscoveryServiceTest {

    @Mock
    private NuxeoClient nuxeoClient;

    private NuxeoProperties props;
    private NuxeoDiscoveryService service;

    @BeforeEach
    void setUp() {
        props = new NuxeoProperties();
        props.getScope().setIncludedRoots(List.of("/default-domain/workspaces"));
        props.getScope().setIncludedTypes(List.of("File", "Note"));
        props.getScope().setExcludedLifecycleStates(List.of("deleted"));
        props.getDiscovery().setPageSize(2);
        props.getDiscovery().setMode(NuxeoProperties.Mode.NXQL);
        service = new NuxeoDiscoveryService(nuxeoClient, props);
    }

    @Test
    void discover_usesNxqlAndFiltersOutOfScopeNodes() {
        NuxeoDocument doc1 = nuxeoDoc("doc-1", "/default-domain/workspaces/finance/doc-1.pdf", "File", "project");
        NuxeoDocument doc2 = nuxeoDoc("doc-2", "/default-domain/workspaces/finance/doc-2.pdf", "Picture", "project");
        NuxeoDocument.Page firstPage = pageOf(true, doc1, doc2);
        NuxeoDocument.Page emptyPage = pageOf(false);

        when(nuxeoClient.searchPageByNxql(anyString(), anyInt(), anyInt()))
                .thenReturn(firstPage)
                .thenReturn(emptyPage);
        when(nuxeoClient.toSourceNode(doc1))
                .thenReturn(fileNode("doc-1", "/default-domain/workspaces/finance/doc-1.pdf", "File", "project"));
        when(nuxeoClient.toSourceNode(doc2))
                .thenReturn(fileNode("doc-2", "/default-domain/workspaces/finance/doc-2.pdf", "Picture", "project"));

        List<SourceNode> discovered = service.discoverFromConfig();

        assertThat(discovered).extracting(SourceNode::nodeId).containsExactly("doc-1");
        verify(nuxeoClient).searchPageByNxql(anyString(), org.mockito.ArgumentMatchers.eq(0), org.mockito.ArgumentMatchers.eq(2));
        verify(nuxeoClient).searchPageByNxql(anyString(), org.mockito.ArgumentMatchers.eq(1), org.mockito.ArgumentMatchers.eq(2));
    }

    @Test
    void discover_fallsBackToChildrenWhenNxqlIsUnavailable() {
        SourceNode root = folderNode("folder-1", "/default-domain/workspaces");
        SourceNode child = fileNode("doc-3", "/default-domain/workspaces/finance/doc-3.pdf", "File", "project");

        when(nuxeoClient.searchPageByNxql(anyString(), anyInt(), anyInt()))
                .thenThrow(new UnsupportedOperationException("not supported"));  // triggers CHILDREN fallback
        when(nuxeoClient.getNodeByPath("/default-domain/workspaces")).thenReturn(root);
        when(nuxeoClient.getChildren("folder-1", 0, 2)).thenReturn(List.of(child));

        List<SourceNode> discovered = service.discoverFromConfig();

        assertThat(discovered).extracting(SourceNode::nodeId).containsExactly("doc-3");
        verify(nuxeoClient).getNodeByPath("/default-domain/workspaces");
    }

    /**
     * A page shorter than the page size is not an exhausted container (#136): the SPI lets a client return
     * fewer entries than asked for, so only an empty page ends the loop. Ending on the short page here would
     * drop the rest of the folder, and the pass would still report itself complete, which is what exposes
     * the tail to the reconciliation sweep.
     */
    @Test
    void childrenTraversalKeepsPagingPastAShortPage() {
        props.getDiscovery().setMode(NuxeoProperties.Mode.CHILDREN);
        SourceNode root = folderNode("folder-1", "/default-domain/workspaces");

        // The scope resolver asks NXQL which ancestors carry the facets; none here, so scope falls back to
        // the configured roots.
        when(nuxeoClient.searchPageByNxql(anyString(), anyInt(), anyInt())).thenReturn(pageOf(false));
        when(nuxeoClient.getNodeByPath("/default-domain/workspaces")).thenReturn(root);
        when(nuxeoClient.getChildren("folder-1", 0, 2)).thenReturn(List.of(doc("doc-1")));
        when(nuxeoClient.getChildren("folder-1", 2, 2)).thenReturn(List.of(doc("doc-2"), doc("doc-3")));
        when(nuxeoClient.getChildren("folder-1", 4, 2)).thenReturn(List.of());

        NuxeoDiscoveryService.NuxeoDiscovery discovery = service.discoverFromConfigTallied();

        assertThat(discovery.nodes()).extracting(SourceNode::nodeId)
                .containsExactly("doc-1", "doc-2", "doc-3");
        assertThat(discovery.outcome().complete()).isTrue();
    }

    /**
     * The cursor advances by what was asked for, not by what came back. Advancing by the page size received
     * would re-read the entries a short page dropped, which shifts every later window.
     */
    @Test
    void childrenTraversalAdvancesTheCursorByThePageSize() {
        props.getDiscovery().setMode(NuxeoProperties.Mode.CHILDREN);
        SourceNode root = folderNode("folder-1", "/default-domain/workspaces");

        // The scope resolver asks NXQL which ancestors carry the facets; none here, so scope falls back to
        // the configured roots.
        when(nuxeoClient.searchPageByNxql(anyString(), anyInt(), anyInt())).thenReturn(pageOf(false));
        when(nuxeoClient.getNodeByPath("/default-domain/workspaces")).thenReturn(root);
        when(nuxeoClient.getChildren("folder-1", 0, 2)).thenReturn(List.of(doc("doc-1")));
        when(nuxeoClient.getChildren("folder-1", 2, 2)).thenReturn(List.of());

        service.discoverFromConfig();

        verify(nuxeoClient).getChildren("folder-1", 2, 2);
        verify(nuxeoClient, never()).getChildren("folder-1", 1, 2);
    }

    private static SourceNode doc(String nodeId) {
        return fileNode(nodeId, "/default-domain/workspaces/finance/" + nodeId + ".pdf", "File", "project");
    }

    private static SourceNode fileNode(String nodeId, String fullPath, String type, String lifecycleState) {
        return new SourceNode(
                nodeId,
                "nuxeo-dev",
                "nuxeo",
                "Document",
                fullPath.substring(0, fullPath.lastIndexOf('/')),
                "application/pdf",
                OffsetDateTime.parse("2026-03-24T10:00:00Z"),
                false,
                Set.of(),
                Set.of(),
                Map.of(
                        "nuxeo_path", fullPath,
                        "nuxeo_documentType", type,
                        "nuxeo_lifecycleState", lifecycleState
                )
        );
    }

    private static SourceNode folderNode(String nodeId, String fullPath) {
        return new SourceNode(
                nodeId,
                "nuxeo-dev",
                "nuxeo",
                "Folder",
                fullPath,
                null,
                OffsetDateTime.parse("2026-03-24T10:00:00Z"),
                true,
                Set.of(),
                Set.of(),
                Map.of(
                        "nuxeo_path", fullPath,
                        "nuxeo_documentType", "Workspace",
                        "nuxeo_lifecycleState", "project"
                )
        );
    }

    private static NuxeoDocument nuxeoDoc(String uid, String path, String type, String lifecycleState) {
        NuxeoDocument doc = new NuxeoDocument();
        doc.setUid(uid);
        doc.setPath(path);
        doc.setType(type);
        doc.setState(lifecycleState);
        doc.setTitle(uid);
        doc.setProperties(Map.of(
                "dc:modified", "2026-03-24T10:00:00Z",
                "file:content", Map.of("mime-type", "application/pdf")
        ));
        return doc;
    }

    private static NuxeoDocument.Page pageOf(boolean hasMore, NuxeoDocument... docs) {
        NuxeoDocument.Page page = new NuxeoDocument.Page();
        page.setEntries(List.of(docs));
        page.setNextPageAvailable(hasMore);  // Lombok setter for field nextPageAvailable
        return page;
    }
}
