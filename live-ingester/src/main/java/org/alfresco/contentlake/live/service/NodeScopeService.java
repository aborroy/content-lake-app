package org.alfresco.contentlake.live.service;

import org.alfresco.contentlake.live.config.LiveIngesterProperties;
import org.alfresco.core.model.Node;
import org.springframework.stereotype.Service;

import java.util.Collection;
import java.util.List;

@Service
public class NodeScopeService {

    private final List<String> excludePaths;
    private final List<String> includePaths;
    private final List<String> requiredAspects;
    private final List<String> excludedAspects;

    public NodeScopeService(LiveIngesterProperties props) {
        this.excludePaths = List.copyOf(props.getFilter().getExcludePaths());
        this.includePaths = List.copyOf(props.getScope().getIncludePaths());
        this.requiredAspects = List.copyOf(props.getScope().getRequiredAspects());
        this.excludedAspects = List.copyOf(props.getFilter().getExcludeAspects());
    }

    public boolean isInScope(Node node) {
        if (node == null || Boolean.TRUE.equals(node.isIsFolder())) {
            return false;
        }

        if (hasExcludedAspect(node.getAspectNames())) {
            return false;
        }

        String path = node.getPath() != null ? node.getPath().getName() : null;
        if (matchesExcludedPath(path)) {
            return false;
        }

        boolean scopeConfigured = !includePaths.isEmpty() || !requiredAspects.isEmpty();
        if (!scopeConfigured) {
            return true;
        }

        return matchesIncludePath(path) || matchesRequiredAspect(node.getAspectNames());
    }

    public boolean hasExcludedAspect(Collection<String> aspects) {
        if (aspects == null || aspects.isEmpty()) {
            return false;
        }
        return aspects.stream().anyMatch(excludedAspects::contains);
    }

    public boolean matchesExcludedPath(String path) {
        return matchesAny(path, excludePaths);
    }

    public boolean matchesIncludePath(String path) {
        return matchesAny(path, includePaths);
    }

    public boolean matchesRequiredAspect(Collection<String> aspects) {
        if (requiredAspects.isEmpty() || aspects == null || aspects.isEmpty()) {
            return false;
        }
        return aspects.stream().anyMatch(requiredAspects::contains);
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
