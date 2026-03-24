package org.alfresco.contentlake.service;

import org.alfresco.contentlake.model.ContentLakeIngestProperties;
import org.alfresco.contentlake.spi.ScopeResolver;
import org.alfresco.contentlake.spi.SourceNode;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Config-driven Nuxeo scope rules for the MVP adapter.
 */
public class NuxeoScopeResolver implements ScopeResolver {

    private static final String DELETED_STATE = "deleted";

    private final List<String> includedRoots;
    private final Set<String> includedTypes;

    public NuxeoScopeResolver(Collection<String> includedRoots, Collection<String> includedTypes) {
        this.includedRoots = includedRoots.stream()
                .filter(Objects::nonNull)
                .map(this::normalizePath)
                .toList();
        this.includedTypes = Set.copyOf(includedTypes);
    }

    @Override
    public boolean isInScope(SourceNode node) {
        if (node == null || node.folder()) {
            return false;
        }
        String documentType = getStringProperty(node, ContentLakeIngestProperties.NUXEO_DOCUMENT_TYPE);
        if (!includedTypes.contains(documentType)) {
            return false;
        }
        return isIncludedPath(resolveRepositoryPath(node)) && !isDeleted(node);
    }

    @Override
    public boolean shouldTraverse(SourceNode node) {
        if (node == null || !node.folder()) {
            return false;
        }
        return isIncludedPath(resolveRepositoryPath(node)) && !isDeleted(node);
    }

    private boolean isDeleted(SourceNode node) {
        String lifecycleState = getStringProperty(node, ContentLakeIngestProperties.NUXEO_LIFECYCLE_STATE);
        return lifecycleState != null && DELETED_STATE.equalsIgnoreCase(lifecycleState);
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
