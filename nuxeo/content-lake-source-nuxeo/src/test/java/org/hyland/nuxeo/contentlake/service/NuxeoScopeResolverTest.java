package org.hyland.nuxeo.contentlake.service;

import org.hyland.nuxeo.contentlake.client.NuxeoClient;
import org.hyland.contentlake.model.ContentLakeIngestProperties;
import org.hyland.nuxeo.contentlake.model.NuxeoDocument;
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
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class NuxeoScopeResolverTest {

    @Mock
    private NuxeoClient nuxeoClient;

    private NuxeoScopeResolver resolver;

    @BeforeEach
    void setUp() {
        resolver = new NuxeoScopeResolver(
                Set.of("/default-domain/workspaces"),
                Set.of("File", "Note"),
                Set.of("deleted", "obsolete"),
                nuxeoClient
        );
        // Default: empty NXQL results so existing tests fall back to includedRoots
        lenient().when(nuxeoClient.searchPageByNxql(anyString(), anyInt(), anyInt()))
                .thenReturn(emptyPage());
    }

    // ──────────────────────────────────────────────────────────────────────
    // Existing tests (now with mock client — fallback to includedRoots)
    // ──────────────────────────────────────────────────────────────────────

    @Test
    void isInScope_acceptsIncludedDocumentInsideConfiguredRoot() {
        SourceNode node = fileNode(
                "/default-domain/workspaces/finance/report.pdf",
                "File",
                "project"
        );

        assertThat(resolver.isInScope(node)).isTrue();
    }

    @Test
    void isInScope_rejectsDeletedDocumentsAndWrongTypes() {
        SourceNode deleted = fileNode(
                "/default-domain/workspaces/finance/report.pdf",
                "File",
                "deleted"
        );
        SourceNode wrongType = fileNode(
                "/default-domain/workspaces/finance/note.txt",
                "Picture",
                "project"
        );

        assertThat(resolver.isInScope(deleted)).isFalse();
        assertThat(resolver.isInScope(wrongType)).isFalse();
    }

    @Test
    void isInScope_rejectsConfiguredExcludedLifecycleStates() {
        SourceNode obsolete = fileNode(
                "/default-domain/workspaces/finance/report.pdf",
                "File",
                "obsolete"
        );

        assertThat(resolver.isInScope(obsolete)).isFalse();
    }

    @Test
    void shouldTraverse_allowsFolderWithinIncludedRoots() {
        SourceNode folder = folderNode(
                "/default-domain/workspaces/finance",
                "Workspace",
                "project"
        );
        SourceNode outsideRoot = folderNode(
                "/default-domain/sections/archive",
                "Workspace",
                "project"
        );

        assertThat(resolver.shouldTraverse(folder)).isTrue();
        assertThat(resolver.shouldTraverse(outsideRoot)).isFalse();
    }

    // ──────────────────────────────────────────────────────────────────────
    // New facet-based tests
    // ──────────────────────────────────────────────────────────────────────

    @Test
    void isInScope_returnsTrueWhenAncestorHasContentLakeIndexedFacet() {
        // Create resolver with NO includedRoots so fallback doesn't interfere
        NuxeoScopeResolver freshResolver = new NuxeoScopeResolver(
                List.of(), Set.of("File"), Set.of("deleted"), nuxeoClient);

        SourceNode node = fileNode("/projects/alpha/report.pdf", "File", "project");

        // /projects/alpha is indexed
        NuxeoDocument.Page indexedPage = pageOf("/projects/alpha");
        when(nuxeoClient.searchPageByNxql(anyString(), anyInt(), anyInt()))
                .thenReturn(indexedPage)    // Phase 1 (indexed)
                .thenReturn(emptyPage());   // Phase 2 (exclusion)

        assertThat(freshResolver.isInScope(node)).isTrue();
    }

    @Test
    void isInScope_returnsTrueWhenNoIndexedFacetButFallsBackToIncludedRoots() {
        // NXQL returns nothing → falls back to includedRoots
        SourceNode node = fileNode(
                "/default-domain/workspaces/finance/report.pdf",
                "File",
                "project"
        );

        when(nuxeoClient.searchPageByNxql(anyString(), anyInt(), anyInt()))
                .thenReturn(emptyPage());

        assertThat(resolver.isInScope(node)).isTrue();
    }

    @Test
    void isInScope_returnsFalseWhenNoIndexedAncestorAndNoConfigRoot() {
        NuxeoScopeResolver freshResolver = new NuxeoScopeResolver(
                List.of(), Set.of("File"), Set.of("deleted"), nuxeoClient);

        when(nuxeoClient.searchPageByNxql(anyString(), anyInt(), anyInt()))
                .thenReturn(emptyPage());

        SourceNode node = fileNode("/projects/alpha/report.pdf", "File", "project");

        assertThat(freshResolver.isInScope(node)).isFalse();
    }

    @Test
    void isInScope_returnsFalseWhenSelfHasExclusionFacetAndProperty() {
        // Self-excluded document: no NXQL should be called
        SourceNode node = new SourceNode(
                "doc-123", "nuxeo-dev", "nuxeo", "report.pdf",
                "/default-domain/workspaces/finance",
                "application/pdf",
                OffsetDateTime.parse("2026-03-24T09:15:30Z"),
                false,
                Set.of(), Set.of(),
                Map.of(
                        ContentLakeIngestProperties.NUXEO_PATH, "/default-domain/workspaces/finance/report.pdf",
                        ContentLakeIngestProperties.NUXEO_DOCUMENT_TYPE, "File",
                        ContentLakeIngestProperties.NUXEO_LIFECYCLE_STATE, "project",
                        ContentLakeIngestProperties.NUXEO_FACETS, List.of("ContentLakeScope"),
                        ContentLakeIngestProperties.NUXEO_EXCLUDE_FROM_SCOPE, true
                )
        );

        assertThat(resolver.isInScope(node)).isFalse();
        // No NXQL call should be made when self-excluded
        verify(nuxeoClient, times(0)).searchPageByNxql(anyString(), anyInt(), anyInt());
    }

    @Test
    void isInScope_returnsFalseWhenExclusionAncestorFound() {
        NuxeoScopeResolver freshResolver = new NuxeoScopeResolver(
                List.of(), Set.of("File"), Set.of("deleted"), nuxeoClient);

        SourceNode node = fileNode("/projects/alpha/sub/report.pdf", "File", "project");

        // Phase 1: /projects/alpha is indexed
        NuxeoDocument.Page indexedPage = pageOf("/projects/alpha");
        // Phase 2: /projects/alpha/sub is excluded
        NuxeoDocument.Page excludedPage = pageOf("/projects/alpha/sub");
        when(nuxeoClient.searchPageByNxql(anyString(), anyInt(), anyInt()))
                .thenReturn(indexedPage)
                .thenReturn(excludedPage);

        assertThat(freshResolver.isInScope(node)).isFalse();
    }

    @Test
    void isInScope_usesCache_doesNotRepeatNxqlForSameAncestorPath() {
        NuxeoScopeResolver freshResolver = new NuxeoScopeResolver(
                List.of(), Set.of("File"), Set.of("deleted"), nuxeoClient);

        SourceNode node1 = fileNode("/projects/alpha/doc1.txt", "File", "project");
        SourceNode node2 = fileNode("/projects/alpha/doc2.txt", "File", "project");

        NuxeoDocument.Page indexedPage = pageOf("/projects/alpha");
        when(nuxeoClient.searchPageByNxql(anyString(), anyInt(), anyInt()))
                .thenReturn(indexedPage)  // Phase 1 first call
                .thenReturn(emptyPage()); // Phase 2 first call
        // Second call should hit cache — no additional NXQL

        assertThat(freshResolver.isInScope(node1)).isTrue();
        assertThat(freshResolver.isInScope(node2)).isTrue();

        // Only 2 NXQL calls total (phase 1 and phase 2 for first node; second hits cache)
        verify(nuxeoClient, times(2)).searchPageByNxql(anyString(), anyInt(), anyInt());
    }

    @Test
    void invalidateFolderScope_clearsCache_forcesNxqlOnNextCall() {
        NuxeoScopeResolver freshResolver = new NuxeoScopeResolver(
                List.of(), Set.of("File"), Set.of("deleted"), nuxeoClient);

        SourceNode node = fileNode("/projects/alpha/doc.txt", "File", "project");

        NuxeoDocument.Page indexedPage = pageOf("/projects/alpha");
        when(nuxeoClient.searchPageByNxql(anyString(), anyInt(), anyInt()))
                .thenReturn(indexedPage)
                .thenReturn(emptyPage())   // Phase 2 first call
                .thenReturn(indexedPage)   // Phase 1 after invalidation
                .thenReturn(emptyPage());  // Phase 2 after invalidation

        assertThat(freshResolver.isInScope(node)).isTrue(); // populates cache
        freshResolver.invalidateFolderScope("/projects/alpha"); // evict
        assertThat(freshResolver.isInScope(node)).isTrue(); // re-queries

        verify(nuxeoClient, times(4)).searchPageByNxql(anyString(), anyInt(), anyInt());
    }

    @Test
    void shouldTraverse_returnsTrueForFolderWithIndexedAncestor() {
        NuxeoScopeResolver freshResolver = new NuxeoScopeResolver(
                List.of(), Set.of("File"), Set.of("deleted"), nuxeoClient);

        SourceNode folder = folderNode("/projects/alpha/sub", "Workspace", "project");

        // /projects/alpha/sub or /projects/alpha is indexed
        NuxeoDocument.Page indexedPage = pageOf("/projects/alpha");
        when(nuxeoClient.searchPageByNxql(anyString(), anyInt(), anyInt()))
                .thenReturn(indexedPage)
                .thenReturn(emptyPage());

        assertThat(freshResolver.shouldTraverse(folder)).isTrue();
    }

    @Test
    void shouldTraverse_returnsFalseWhenFolderHasExclusionFacetInSourceProperties() {
        SourceNode excludedFolder = new SourceNode(
                "folder-123", "nuxeo-dev", "nuxeo", "sub",
                "/default-domain/workspaces/finance/sub",
                null,
                OffsetDateTime.parse("2026-03-24T09:15:30Z"),
                true,
                Set.of(), Set.of(),
                Map.of(
                        ContentLakeIngestProperties.NUXEO_PATH, "/default-domain/workspaces/finance/sub",
                        ContentLakeIngestProperties.NUXEO_DOCUMENT_TYPE, "Workspace",
                        ContentLakeIngestProperties.NUXEO_LIFECYCLE_STATE, "project",
                        ContentLakeIngestProperties.NUXEO_FACETS, List.of("ContentLakeScope"),
                        ContentLakeIngestProperties.NUXEO_EXCLUDE_FROM_SCOPE, true
                )
        );

        assertThat(resolver.shouldTraverse(excludedFolder)).isFalse();
        verify(nuxeoClient, times(0)).searchPageByNxql(anyString(), anyInt(), anyInt());
    }

    // ──────────────────────────────────────────────────────────────────────
    // Test for deriveAncestorPaths (package-visible for testing)
    // ──────────────────────────────────────────────────────────────────────

    @Test
    void deriveAncestorPaths_returnsAncestorsFromDeepestToShallowest() {
        assertThat(resolver.deriveAncestorPaths("/a/b/c/doc.pdf", false))
                .containsExactly("/a/b/c", "/a/b", "/a");
    }

    @Test
    void deriveAncestorPaths_includesSelfWhenRequested() {
        assertThat(resolver.deriveAncestorPaths("/a/b/c", true))
                .containsExactly("/a/b/c", "/a/b", "/a");
    }

    @Test
    void deriveAncestorPaths_returnsEmptyForRootOrBlank() {
        assertThat(resolver.deriveAncestorPaths("/", false)).isEmpty();
        assertThat(resolver.deriveAncestorPaths("", false)).isEmpty();
    }

    // ──────────────────────────────────────────────────────────────────────
    // Helpers
    // ──────────────────────────────────────────────────────────────────────

    private static SourceNode fileNode(String fullPath, String type, String lifecycleState) {
        return new SourceNode(
                "doc-123",
                "nuxeo-dev",
                "nuxeo",
                "Document",
                "/default-domain/workspaces/finance",
                "application/pdf",
                OffsetDateTime.parse("2026-03-24T09:15:30Z"),
                false,
                Set.of(),
                Set.of(),
                Map.of(
                        ContentLakeIngestProperties.NUXEO_PATH, fullPath,
                        ContentLakeIngestProperties.NUXEO_DOCUMENT_TYPE, type,
                        ContentLakeIngestProperties.NUXEO_LIFECYCLE_STATE, lifecycleState
                )
        );
    }

    private static SourceNode folderNode(String fullPath, String type, String lifecycleState) {
        return new SourceNode(
                "folder-123",
                "nuxeo-dev",
                "nuxeo",
                "Folder",
                fullPath,
                null,
                OffsetDateTime.parse("2026-03-24T09:15:30Z"),
                true,
                Set.of(),
                Set.of(),
                Map.of(
                        ContentLakeIngestProperties.NUXEO_PATH, fullPath,
                        ContentLakeIngestProperties.NUXEO_DOCUMENT_TYPE, type,
                        ContentLakeIngestProperties.NUXEO_LIFECYCLE_STATE, lifecycleState
                )
        );
    }

    private static NuxeoDocument.Page emptyPage() {
        return new NuxeoDocument.Page();
    }

    private static NuxeoDocument.Page pageOf(String... paths) {
        List<NuxeoDocument> docs = new java.util.ArrayList<>();
        for (String path : paths) {
            NuxeoDocument doc = new NuxeoDocument();
            doc.setPath(path);
            docs.add(doc);
        }
        NuxeoDocument.Page page = new NuxeoDocument.Page();
        page.setEntries(docs);
        return page;
    }
}
