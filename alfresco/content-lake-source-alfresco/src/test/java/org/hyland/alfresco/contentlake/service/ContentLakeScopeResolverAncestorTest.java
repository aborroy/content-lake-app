package org.hyland.alfresco.contentlake.service;

import org.alfresco.core.model.Node;
import org.alfresco.core.model.PathElement;
import org.alfresco.core.model.PathInfo;
import org.hyland.alfresco.contentlake.client.AlfrescoClient;
import org.hyland.alfresco.contentlake.client.AlfrescoSearchService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for ancestor-scope checks in ContentLakeScopeResolver.
 *
 * Previously, AlfrescoSearchService.hasIndexedAncestor() used
 * ANCESTOR:"workspace://SpacesStore/{nodeId}" which returns DESCENDANTS of that
 * node, not its ancestors -- always returning false for a file that is a leaf.
 * The fix passes the path element IDs to a ID:-based OR query instead.
 */
class ContentLakeScopeResolverAncestorTest {

    private StubSearchService searchService;
    private StubAlfrescoClient alfrescoClient;
    private ContentLakeScopeResolver resolver;

    @BeforeEach
    void setUp() {
        searchService = new StubSearchService();
        alfrescoClient = new StubAlfrescoClient();
        resolver = new ContentLakeScopeResolver(List.of(), Set.of(),
                alfrescoClient, searchService);
    }

    @Test
    void isInScope_returnsTrueWhenAncestorHasIndexedAspect() {
        // File under a folder whose ID will be passed to hasIndexedAncestor
        Node file = fileWithPath("file-1", List.of("root-id", "sites-id", "doclib-id"));
        searchService.indexedAncestorIds.add("doclib-id");

        assertThat(resolver.isInScope(file)).isTrue();
    }

    @Test
    void isInScope_returnsFalseWhenNoAncestorHasIndexedAspect() {
        Node file = fileWithPath("file-1", List.of("root-id", "sites-id", "doclib-id"));
        // No ancestor IDs added to indexedAncestorIds

        assertThat(resolver.isInScope(file)).isFalse();
    }

    @Test
    void isInScope_returnsFalseWhenAncestorIsExcluded() {
        Node file = fileWithPath("file-1", List.of("root-id", "sites-id", "doclib-id"));
        searchService.indexedAncestorIds.add("doclib-id");
        searchService.excludedAncestorIds.add("sites-id");

        assertThat(resolver.isInScope(file)).isFalse();
    }

    @Test
    void isInScope_returnsTrueWhenFileHasIndexedAspectDirectly() {
        Node file = fileWithPath("file-1", List.of("root-id"));
        file.aspectNames(List.of("cl:indexed"));
        // No ancestor indexed, but file itself has cl:indexed -- unusual but supported

        assertThat(resolver.isInScope(file)).isTrue();
    }

    // ──────────────────────────────────────────────────────────────────────
    // REST-based ancestor checks (Part 2 — tear-down race avoidance)
    // ──────────────────────────────────────────────────────────────────────

    @Test
    void hasIndexedAncestorViaRest_walksRestNotSolr() {
        Node folder = folderWithPath("folder-1", List.of("root-id", "parent-id"));
        alfrescoClient.nodesById.put("parent-id", folderWithAspects("parent-id", List.of("cl:indexed")));
        alfrescoClient.nodesById.put("root-id", folderWithAspects("root-id", List.of()));

        assertThat(resolver.hasIndexedAncestorViaRest(folder)).isTrue();
        assertThat(searchService.lastQueriedIds).isNull(); // Solr not consulted
    }

    @Test
    void isFolderInScopeViaRest_returnsTrueViaAncestorIndex() {
        Node folder = folderWithAspects("folder-1", List.of()); // no self cl:indexed
        folder.path(new PathInfo()
                .elements(List.of(new PathElement().id("ancestor-1").name("Sites")))
                .name("/Sites/...")
                .isComplete(true));
        alfrescoClient.nodesById.put("ancestor-1", folderWithAspects("ancestor-1", List.of("cl:indexed")));

        assertThat(resolver.isFolderInScopeViaRest(folder)).isTrue();
    }

    @Test
    void isFolderInScopeViaRest_returnsFalseWhenNoSelfNorAncestorIndex() {
        Node folder = folderWithAspects("folder-1", List.of());
        folder.path(new PathInfo()
                .elements(List.of(new PathElement().id("ancestor-1").name("Sites")))
                .name("/Sites/...")
                .isComplete(true));
        alfrescoClient.nodesById.put("ancestor-1", folderWithAspects("ancestor-1", List.of()));

        assertThat(resolver.isFolderInScopeViaRest(folder)).isFalse();
    }

    @Test
    void isFolderInScopeViaRest_returnsFalseWhenSelfExcludedFromLake() {
        Node folder = folderWithAspects("folder-1", List.of("cl:indexed", "cl:fileScope"));
        folder.properties(Map.of("cl:excludeFromLake", true));
        folder.path(new PathInfo().elements(List.of()).name("/Sites/...").isComplete(true));

        assertThat(resolver.isFolderInScopeViaRest(folder)).isFalse();
    }

    @Test
    void searchService_receivesPathElementIdsNotNodeId() {
        // Guard against regression: the query must use ancestor path element IDs,
        // NOT the file's own node ID.
        Node file = fileWithPath("file-node-id", List.of("ancestor-a", "ancestor-b"));
        searchService.indexedAncestorIds.add("ancestor-a");

        resolver.isInScope(file);

        assertThat(searchService.lastQueriedIds)
                .containsExactlyInAnyOrder("ancestor-a", "ancestor-b")
                .doesNotContain("file-node-id");
    }

    // ──────────────────────────────────────────────────────────────────────
    // Helpers
    // ──────────────────────────────────────────────────────────────────────

    private static Node fileWithPath(String nodeId, List<String> ancestorIds) {
        List<PathElement> elements = ancestorIds.stream()
                .map(id -> new PathElement().id(id).name("name-" + id))
                .toList();
        PathInfo path = new PathInfo()
                .elements(elements)
                .name("/Company Home/...")
                .isComplete(true);
        return new Node()
                .id(nodeId)
                .isFolder(false)
                .isFile(true)
                .path(path);
    }

    private static Node folderWithPath(String nodeId, List<String> ancestorIds) {
        List<PathElement> elements = ancestorIds.stream()
                .map(id -> new PathElement().id(id).name("name-" + id))
                .toList();
        PathInfo path = new PathInfo()
                .elements(elements)
                .name("/Company Home/...")
                .isComplete(true);
        return new Node()
                .id(nodeId)
                .isFolder(true)
                .isFile(false)
                .path(path);
    }

    private static Node folderWithAspects(String nodeId, List<String> aspects) {
        return new Node()
                .id(nodeId)
                .isFolder(true)
                .isFile(false)
                .aspectNames(aspects);
    }

    private static final class StubSearchService extends AlfrescoSearchService {
        final Set<String> indexedAncestorIds = new java.util.HashSet<>();
        final Set<String> excludedAncestorIds = new java.util.HashSet<>();
        Collection<String> lastQueriedIds;

        StubSearchService() {
            super(null, null, null);
        }

        @Override
        public boolean hasIndexedAncestor(Collection<String> ancestorIds) {
            this.lastQueriedIds = ancestorIds;
            return ancestorIds.stream().anyMatch(indexedAncestorIds::contains);
        }

        @Override
        public boolean hasExcludedAncestor(Collection<String> ancestorIds) {
            return ancestorIds.stream().anyMatch(excludedAncestorIds::contains);
        }
    }

    private static final class StubAlfrescoClient extends AlfrescoClient {
        final Map<String, Node> nodesById = new HashMap<>();

        StubAlfrescoClient() {
            super(null, null);
        }

        @Override
        public String getSourceId() { return "test-repo"; }

        @Override
        public String getRepositoryId() { return "test-repo"; }

        @Override
        public Node getAlfrescoNode(String nodeId) {
            return nodesById.get(nodeId);
        }
    }
}
