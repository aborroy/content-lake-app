package org.hyland.nuxeo.contentlake.service;

import org.hyland.nuxeo.contentlake.client.NuxeoClient;
import org.hyland.contentlake.model.ContentLakeIngestProperties;
import org.hyland.nuxeo.contentlake.model.NuxeoDocument;
import org.hyland.contentlake.spi.ScopeResolver;
import org.hyland.contentlake.spi.SourceNode;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Nuxeo scope resolver that supports both config-driven path prefixes (bootstrap
 * fallback) and runtime per-folder facet marking via the {@code ContentLakeIndexed}
 * and {@code ContentLakeScope} custom facets.
 *
 * <h3>Scope algorithm</h3>
 * <ol>
 *   <li>A document whose own facets include {@code ContentLakeScope} with
 *       {@code cls:excludeFromScope = true} is always excluded (self-exclusion).</li>
 *   <li>Phase 1 — indexed ancestor check: one NXQL batch query verifies whether any
 *       ancestor folder carries the {@code ContentLakeIndexed} facet.  If none is
 *       found, the resolver falls back to the configured {@code includedRoots} prefixes
 *       (bootstrap path, for repositories where no folder has been facet-marked yet).</li>
 *   <li>Phase 2 — exclusion ancestor check: a second NXQL batch query checks whether
 *       any ancestor (between the node and the indexed root) has
 *       {@code cls:excludeFromScope = true}.</li>
 * </ol>
 *
 * <h3>Cache</h3>
 * Resolved {@link FolderScopeState} entries are cached by folder path (bounded at
 * {@value CACHE_MAX_SIZE} entries). Call {@link #invalidateFolderScope(String)} when
 * a folder's facets change so that subsequent calls reflect the updated state.
 */
public class NuxeoScopeResolver implements ScopeResolver {

    static final String FACET_INDEXED = "ContentLakeIndexed";
    static final String FACET_SCOPE = "ContentLakeScope";

    private static final int CACHE_MAX_SIZE = 2000;

    private final List<String> includedRoots;
    private final Set<String> includedTypes;
    private final Set<String> excludedLifecycleStates;
    private final NuxeoClient nuxeoClient;

    /**
     * Separately-keyed caches for indexed and excluded states. Using two maps
     * avoids a subtle bug where the indexed-check phase would pre-populate the
     * excluded state as {@code false} (as a default), preventing the exclusion
     * NXQL query from running.
     */
    private final ConcurrentHashMap<String, Boolean> indexedCache = new ConcurrentHashMap<>(256);
    private final ConcurrentHashMap<String, Boolean> excludedCache = new ConcurrentHashMap<>(256);

    public NuxeoScopeResolver(Collection<String> includedRoots,
                              Collection<String> includedTypes,
                              Collection<String> excludedLifecycleStates,
                              NuxeoClient nuxeoClient) {
        this.includedRoots = includedRoots.stream()
                .filter(Objects::nonNull)
                .map(this::normalizePath)
                .toList();
        this.includedTypes = Set.copyOf(includedTypes);
        this.excludedLifecycleStates = excludedLifecycleStates.stream()
                .filter(Objects::nonNull)
                .map(value -> value.toLowerCase(Locale.ROOT))
                .collect(Collectors.toUnmodifiableSet());
        this.nuxeoClient = nuxeoClient;
    }

    // ──────────────────────────────────────────────────────────────────────
    // ScopeResolver SPI
    // ──────────────────────────────────────────────────────────────────────

    @Override
    public boolean isInScope(SourceNode node) {
        if (node == null || node.folder()) {
            return false;
        }
        String documentType = getStringProperty(node, ContentLakeIngestProperties.NUXEO_DOCUMENT_TYPE);
        if (!includedTypes.contains(documentType)) {
            return false;
        }
        if (isDeleted(node)) {
            return false;
        }
        if (isSelfExcluded(node)) {
            return false;
        }

        String fullPath = resolveRepositoryPath(node);
        List<String> ancestorPaths = deriveAncestorPaths(fullPath, false);
        if (ancestorPaths.isEmpty()) {
            return isIncludedPath(fullPath);
        }

        // Phase 1 — indexed ancestor check (cache + NXQL batch)
        if (!hasIndexedAncestor(ancestorPaths)) {
            return isIncludedPath(fullPath);
        }

        // Phase 2 — exclusion ancestor check (cache + NXQL batch)
        return !hasExcludedAncestor(ancestorPaths);
    }

    @Override
    public boolean shouldTraverse(SourceNode node) {
        if (node == null || !node.folder()) {
            return false;
        }
        if (isDeleted(node)) {
            return false;
        }
        if (isSelfExcluded(node)) {
            return false;
        }

        String fullPath = resolveRepositoryPath(node);
        // Include the folder itself as a potential indexed target
        List<String> paths = deriveAncestorPaths(fullPath, true);
        if (paths.isEmpty()) {
            return isIncludedPath(fullPath);
        }

        if (!hasIndexedAncestor(paths)) {
            return isIncludedPath(fullPath);
        }

        return !hasExcludedAncestor(paths);
    }

    // ──────────────────────────────────────────────────────────────────────
    // Cache management
    // ──────────────────────────────────────────────────────────────────────

    /**
     * Removes the given folder from the ancestor-scope cache.
     *
     * <p>Must be called after {@code ContentLakeIndexed} or {@code ContentLakeScope}
     * is added to or removed from a folder so that subsequent {@link #isInScope} calls
     * reflect the updated facet state.</p>
     */
    public void invalidateFolderScope(String folderPath) {
        indexedCache.remove(folderPath);
        excludedCache.remove(folderPath);
    }

    // ──────────────────────────────────────────────────────────────────────
    // Ancestor-walk helpers
    // ──────────────────────────────────────────────────────────────────────

    /**
     * Returns {@code true} when at least one path in {@code paths} has the
     * {@code ContentLakeIndexed} facet, using the cache and falling back to NXQL.
     */
    private boolean hasIndexedAncestor(List<String> paths) {
        List<String> uncached = new ArrayList<>();
        for (String path : paths) {
            Boolean cached = indexedCache.get(path);
            if (cached != null) {
                if (cached) {
                    return true;
                }
            } else {
                uncached.add(path);
            }
        }

        if (uncached.isEmpty()) {
            return false;
        }

        Set<String> indexedPaths = queryPathsWithFacet(uncached, FACET_INDEXED);
        for (String path : uncached) {
            if (indexedCache.size() < CACHE_MAX_SIZE) {
                indexedCache.putIfAbsent(path, indexedPaths.contains(path));
            }
        }

        return !indexedPaths.isEmpty();
    }

    /**
     * Returns {@code true} when at least one path in {@code paths} is excluded
     * ({@code cls:excludeFromScope = true}), using the cache and falling back to NXQL.
     */
    private boolean hasExcludedAncestor(List<String> paths) {
        List<String> uncached = new ArrayList<>();
        for (String path : paths) {
            Boolean cached = excludedCache.get(path);
            if (cached != null) {
                if (cached) {
                    return true;
                }
                // already resolved as not-excluded — skip
            } else {
                uncached.add(path);
            }
        }

        if (uncached.isEmpty()) {
            return false;
        }

        Set<String> excludedPaths = queryExcludedPaths(uncached);
        for (String path : uncached) {
            if (excludedCache.size() < CACHE_MAX_SIZE) {
                excludedCache.putIfAbsent(path, excludedPaths.contains(path));
            }
        }

        return !excludedPaths.isEmpty();
    }

    // ──────────────────────────────────────────────────────────────────────
    // NXQL queries
    // ──────────────────────────────────────────────────────────────────────

    private Set<String> queryPathsWithFacet(List<String> paths, String facetName) {
        String inClause = paths.stream()
                .map(p -> "'" + p.replace("'", "\\'") + "'")
                .collect(Collectors.joining(", "));
        String nxql = "SELECT * FROM Document"
                + " WHERE ecm:mixinType = '" + facetName + "'"
                + " AND ecm:path IN (" + inClause + ")"
                + " AND ecm:isProxy = 0 AND ecm:isCheckedInVersion = 0";

        return executeNxqlForPaths(nxql);
    }

    private Set<String> queryExcludedPaths(List<String> paths) {
        String inClause = paths.stream()
                .map(p -> "'" + p.replace("'", "\\'") + "'")
                .collect(Collectors.joining(", "));
        String nxql = "SELECT * FROM Document"
                + " WHERE ecm:mixinType = '" + FACET_SCOPE + "'"
                + " AND cls:excludeFromScope = 1"
                + " AND ecm:path IN (" + inClause + ")"
                + " AND ecm:isProxy = 0 AND ecm:isCheckedInVersion = 0";

        return executeNxqlForPaths(nxql);
    }

    private Set<String> executeNxqlForPaths(String nxql) {
        Set<String> result = new HashSet<>();
        int pageIndex = 0;
        int pageSize = 50;

        while (true) {
            NuxeoDocument.Page page = nuxeoClient.searchPageByNxql(nxql, pageIndex, pageSize);
            for (NuxeoDocument doc : page.getEntries()) {
                if (doc.getPath() != null) {
                    result.add(doc.getPath());
                }
            }
            if (!page.hasMore()) {
                break;
            }
            pageIndex++;
        }

        return result;
    }

    // ──────────────────────────────────────────────────────────────────────
    // Path helpers
    // ──────────────────────────────────────────────────────────────────────

    /**
     * Derives the ordered list of ancestor paths for a given document or folder path,
     * from deepest to shallowest (excluding the root {@code /}).
     *
     * <p>For {@code includeSelf = true} (folder traversal), the path itself is included
     * as the first element so that the folder can be checked for the
     * {@code ContentLakeIndexed} facet directly.</p>
     *
     * <p>Examples (includeSelf = false):
     * {@code /a/b/c/doc.txt} → {@code ["/a/b/c", "/a/b", "/a"]}
     * </p>
     */
    List<String> deriveAncestorPaths(String path, boolean includeSelf) {
        if (path == null || path.isBlank() || "/".equals(path)) {
            return List.of();
        }
        String normalized = normalizePath(path);
        List<String> result = new ArrayList<>();
        if (includeSelf && !"/".equals(normalized)) {
            result.add(normalized);
        }
        int idx = normalized.lastIndexOf('/');
        while (idx > 0) {
            String ancestor = normalized.substring(0, idx);
            result.add(ancestor);
            idx = ancestor.lastIndexOf('/');
        }
        return result;
    }

    private boolean isIncludedPath(String path) {
        if (path == null || path.isBlank()) {
            return false;
        }
        String normalized = normalizePath(path);
        for (String root : includedRoots) {
            if (normalized.equals(root) || normalized.startsWith(root + "/")) {
                return true;
            }
        }
        return false;
    }

    // ──────────────────────────────────────────────────────────────────────
    // SourceNode property helpers
    // ──────────────────────────────────────────────────────────────────────

    private boolean isSelfExcluded(SourceNode node) {
        Object facetsObj = node.sourceProperties().get(ContentLakeIngestProperties.NUXEO_FACETS);
        if (!(facetsObj instanceof List<?> facetList)) {
            return false;
        }
        if (!facetList.contains(FACET_SCOPE)) {
            return false;
        }
        return Boolean.TRUE.equals(node.sourceProperties().get(ContentLakeIngestProperties.NUXEO_EXCLUDE_FROM_SCOPE));
    }

    private boolean isDeleted(SourceNode node) {
        String lifecycleState = getStringProperty(node, ContentLakeIngestProperties.NUXEO_LIFECYCLE_STATE);
        return lifecycleState != null
                && excludedLifecycleStates.contains(lifecycleState.toLowerCase(Locale.ROOT));
    }

    private String resolveRepositoryPath(SourceNode node) {
        String fullPath = getStringProperty(node, ContentLakeIngestProperties.NUXEO_PATH);
        return fullPath != null ? fullPath : node.path();
    }

    private String getStringProperty(SourceNode node, String key) {
        Map<String, Object> sourceProperties = node.sourceProperties();
        if (sourceProperties == null) {
            return null;
        }
        Object value = sourceProperties.get(key);
        return value instanceof String stringValue ? stringValue : null;
    }

    private String normalizePath(String path) {
        if (path == null || path.isBlank()) {
            return "/";
        }
        String normalized = path.startsWith("/") ? path : "/" + path;
        return normalized.length() > 1 && normalized.endsWith("/")
                ? normalized.substring(0, normalized.length() - 1)
                : normalized;
    }

}
