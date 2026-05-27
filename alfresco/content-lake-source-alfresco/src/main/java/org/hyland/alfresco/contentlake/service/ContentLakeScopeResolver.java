package org.hyland.alfresco.contentlake.service;

import org.hyland.alfresco.contentlake.client.AlfrescoClient;
import org.hyland.alfresco.contentlake.client.AlfrescoSearchService;
import org.hyland.contentlake.spi.ScopeResolver;
import org.hyland.contentlake.spi.SourceNode;
import org.alfresco.core.model.Node;

import org.alfresco.core.model.PathElement;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Shared scope rules used by both ingesters to decide whether a node belongs in
 * Content Lake.
 *
 * <p>Business scope is controlled by {@code cl:indexed} on any ancestor folder,
 * with {@code cl:excludeFromLake} able to opt individual files or entire folder
 * subtrees back out. Technical exclusions such as working copies and thumbnail
 * paths are still applied first.</p>
 *
 * <h3>Ancestor scope check</h3>
 * The Alfresco REST API does not populate aspect names on path elements, so ancestor
 * detection requires individual {@code GET /nodes/{id}} calls. Results are cached in
 * a bounded in-memory map (max {@value CACHE_MAX_SIZE} entries). Call
 * {@link #invalidateFolderScope(String)} whenever {@code cl:indexed} is added to or
 * removed from a folder so the cache stays consistent.
 */
public class ContentLakeScopeResolver implements ScopeResolver {

    public static final String INDEXED_ASPECT = "cl:indexed";
    public static final String EXCLUDE_FROM_LAKE_PROPERTY = "cl:excludeFromLake";

    private final List<String> excludedPathPatterns;
    private final Set<String> excludedAspects;
    private final AlfrescoClient alfrescoClient;
    private final AlfrescoSearchService searchService;

    public ContentLakeScopeResolver(Collection<String> excludedPathPatterns,
                                    Collection<String> excludedAspects,
                                    AlfrescoClient alfrescoClient,
                                    AlfrescoSearchService searchService) {
        this.excludedPathPatterns = List.copyOf(excludedPathPatterns);
        this.excludedAspects = Set.copyOf(excludedAspects);
        this.alfrescoClient = alfrescoClient;
        this.searchService = searchService;
    }

    /** Returns the configured excluded aspects (used by AFTS query builders). */
    public Set<String> getExcludedAspects() {
        return excludedAspects;
    }

    /**
     * Returns {@code true} when the node should still be traversed during folder
     * discovery. This only applies technical exclusions, not business scope.
     */
    public boolean shouldTraverse(Node node) {
        if (node == null) {
            return false;
        }
        if (hasExcludedAspect(node.getAspectNames())) {
            return false;
        }

        String path = node.getPath() != null ? node.getPath().getName() : null;
        return !matchesExcludedPath(path);
    }

    // ──────────────────────────────────────────────────────────────────────
    // ScopeResolver — SPI implementation
    // ──────────────────────────────────────────────────────────────────────

    /**
     * Determines scope for a source-agnostic {@link SourceNode} by fetching the
     * underlying Alfresco node and delegating to {@link #isInScope(Node)}.
     */
    @Override
    public boolean isInScope(SourceNode node) {
        if (node == null || node.folder()) {
            return false;
        }
        Node alfrescoNode = alfrescoClient.getAlfrescoNode(node.nodeId());
        return alfrescoNode != null && isInScope(alfrescoNode);
    }

    /**
     * Determines traversal eligibility for a source-agnostic {@link SourceNode} by
     * fetching the underlying Alfresco node and delegating to {@link #shouldTraverse(Node)}.
     */
    @Override
    public boolean shouldTraverse(SourceNode node) {
        if (node == null) {
            return false;
        }
        Node alfrescoNode = alfrescoClient.getAlfrescoNode(node.nodeId());
        return alfrescoNode != null && shouldTraverse(alfrescoNode);
    }

    // ──────────────────────────────────────────────────────────────────────
    // Alfresco Node-based methods (used by live/batch ingesters directly)
    // ──────────────────────────────────────────────────────────────────────

    /**
     * Returns {@code true} when the node is an in-scope file for Content Lake.
     */
    public boolean isInScope(Node node) {
        if (node == null || Boolean.TRUE.equals(node.isIsFolder())) {
            return false;
        }
        if (!shouldTraverse(node)) {
            return false;
        }
        if (isExcludedBySelfOrAncestor(node)) {
            return false;
        }

        return hasIndexedAspect(node.getAspectNames()) || hasIndexedAncestor(node);
    }

    /**
     * Returns {@code true} when a folder itself belongs to an indexed subtree and
     * is not excluded by a folder-level override.
     */
    public boolean isFolderInScope(Node node) {
        if (node == null || !Boolean.TRUE.equals(node.isIsFolder())) {
            return false;
        }
        if (!shouldTraverse(node)) {
            return false;
        }
        if (isExcludedBySelfOrAncestor(node)) {
            return false;
        }

        return hasIndexedAspect(node.getAspectNames()) || hasIndexedAncestor(node);
    }

    public boolean hasExcludedAspect(Collection<String> aspects) {
        if (aspects == null || aspects.isEmpty()) {
            return false;
        }
        return aspects.stream().anyMatch(excludedAspects::contains);
    }

    public boolean matchesExcludedPath(String path) {
        return matchesAny(path, excludedPathPatterns);
    }

    /** No-op: cache was removed; kept for call-site compatibility. */
    public void invalidateFolderScope(String folderId) {}

    /**
     * Walks the path elements via REST and returns {@code true} when any ancestor folder
     * has the {@code cl:indexed} aspect.
     *
     * <p>Used by the tear-down path (cl:indexed removal) where the AFTS-based
     * {@link AlfrescoSearchService#hasIndexedAncestor} would race with the Solr commit
     * in the wrong direction (returning a stale {@code true} and skipping a valid
     * tear-down). REST reads from the DB and is therefore consistent with the
     * triggering event.</p>
     */
    public boolean hasIndexedAncestorViaRest(Node node) {
        for (String ancestorId : pathElementIds(node)) {
            Node ancestor = alfrescoClient.getAlfrescoNode(ancestorId);
            if (ancestor != null && hasIndexedAspect(ancestor.getAspectNames())) {
                return true;
            }
        }
        return false;
    }

    /**
     * REST-based variant of {@link #isExcludedBySelfOrAncestor} that does not race
     * with Solr commits.
     */
    public boolean isExcludedBySelfOrAncestorViaRest(Node node) {
        if (node == null) {
            return false;
        }
        if (isExcludedFromLake(node)) {
            return true;
        }
        for (String ancestorId : pathElementIds(node)) {
            Node ancestor = alfrescoClient.getAlfrescoNode(ancestorId);
            if (ancestor != null && isExcludedFromLake(ancestor)) {
                return true;
            }
        }
        return false;
    }

    /**
     * REST-based variant of {@link #isFolderInScope}. Used by the tear-down dispatcher
     * to decide whether a {@code cl:indexed}-removed folder still has scope via an
     * ancestor — without racing the Solr commit of the just-removed aspect.
     */
    public boolean isFolderInScopeViaRest(Node folder) {
        if (folder == null || !Boolean.TRUE.equals(folder.isIsFolder())) {
            return false;
        }
        if (!shouldTraverse(folder)) {
            return false;
        }
        if (isExcludedBySelfOrAncestorViaRest(folder)) {
            return false;
        }
        return hasIndexedAspect(folder.getAspectNames()) || hasIndexedAncestorViaRest(folder);
    }

    private boolean hasIndexedAncestor(Node node) {
        return searchService.hasIndexedAncestor(pathElementIds(node));
    }

    private boolean hasExcludedAncestor(Node node) {
        return searchService.hasExcludedAncestor(pathElementIds(node));
    }

    private static List<String> pathElementIds(Node node) {
        if (node.getPath() == null || node.getPath().getElements() == null) {
            return List.of();
        }
        return node.getPath().getElements().stream()
                .map(PathElement::getId)
                .filter(id -> id != null && !id.isBlank())
                .collect(Collectors.toList());
    }

    private boolean hasIndexedAspect(Collection<String> aspects) {
        return aspects != null && aspects.contains(INDEXED_ASPECT);
    }

    public boolean isExcludedFromLake(Node node) {
        Object properties = node.getProperties();
        if (!(properties instanceof Map<?, ?> propertyMap)) {
            return false;
        }

        Object value = propertyMap.get(EXCLUDE_FROM_LAKE_PROPERTY);
        if (value instanceof Boolean boolValue) {
            return boolValue;
        }
        if (value instanceof String stringValue) {
            return Boolean.parseBoolean(stringValue);
        }
        return false;
    }

    /**
     * Returns {@code true} when the node or one of its ancestor folders has an
     * active {@code cl:excludeFromLake} override.
     */
    public boolean isExcludedBySelfOrAncestor(Node node) {
        if (node == null) {
            return false;
        }
        return isExcludedFromLake(node) || hasExcludedAncestor(node);
    }

    private boolean matchesAny(String path, List<String> patterns) {
        if (path == null || path.isBlank() || patterns.isEmpty()) {
            return false;
        }

        for (String pattern : patterns) {
            if (path.matches(pattern.replace("*", ".*"))) {
                return true;
            }
        }

        return false;
    }
}
