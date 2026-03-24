package org.alfresco.contentlake.nuxeo.batch.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.alfresco.contentlake.adapter.NuxeoSourceNodeAdapter;
import org.alfresco.contentlake.client.NuxeoClient;
import org.alfresco.contentlake.config.NuxeoProperties;
import org.alfresco.contentlake.model.NuxeoDocument;
import org.alfresco.contentlake.nuxeo.batch.model.NuxeoSyncRequest;
import org.alfresco.contentlake.service.NuxeoScopeResolver;
import org.alfresco.contentlake.spi.SourceNode;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

/**
 * Discovers Nuxeo documents either with NXQL paging or recursive {@code @children} traversal.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class NuxeoDiscoveryService {

    private final NuxeoClient nuxeoClient;
    private final NuxeoProperties props;

    public List<SourceNode> discoverFromConfig() {
        return discover(new NuxeoSyncRequest());
    }

    public List<SourceNode> discover(NuxeoSyncRequest request) {
        DiscoverySettings settings = resolveSettings(request);
        return switch (settings.discoveryMode()) {
            case CHILDREN -> discoverWithChildren(settings);
            case NXQL -> discoverWithNxqlOrFallback(settings);
        };
    }

    private List<SourceNode> discoverWithNxqlOrFallback(DiscoverySettings settings) {
        try {
            return discoverWithNxql(settings);
        } catch (UnsupportedOperationException e) {
            log.warn("NXQL discovery is unavailable; falling back to @children traversal: {}", e.getMessage());
            return discoverWithChildren(settings);
        }
    }

    private List<SourceNode> discoverWithNxql(DiscoverySettings settings) {
        NuxeoScopeResolver scopeResolver = settings.scopeResolver();
        String query = buildNxqlQuery(settings);
        String sourceId = props.getSourceId();
        String blobXpath = props.getBlobXpath();
        int pageIndex = 0;
        List<SourceNode> discovered = new ArrayList<>();

        while (true) {
            NuxeoDocument.Page page = nuxeoClient.searchPageByNxql(query, pageIndex, settings.pageSize());
            if (page.getEntries().isEmpty()) {
                break;
            }
            page.getEntries().stream()
                    .map(doc -> NuxeoSourceNodeAdapter.toSourceNode(doc, sourceId, blobXpath))
                    .filter(scopeResolver::isInScope)
                    .forEach(discovered::add);

            if (page.getEntries().size() < settings.pageSize() || !page.hasMore()) {
                break;
            }
            pageIndex++;
        }

        return discovered;
    }

    private List<SourceNode> discoverWithChildren(DiscoverySettings settings) {
        NuxeoScopeResolver scopeResolver = settings.scopeResolver();
        List<SourceNode> discovered = new ArrayList<>();

        for (String rootPath : settings.includedRoots()) {
            SourceNode root = nuxeoClient.getNodeByPath(rootPath);
            if (root == null) {
                log.warn("Configured Nuxeo root path not found: {}", rootPath);
                continue;
            }
            collectFromNode(root, scopeResolver, settings.pageSize(), discovered);
        }

        return discovered;
    }

    private void collectFromNode(SourceNode node,
                                 NuxeoScopeResolver scopeResolver,
                                 int pageSize,
                                 List<SourceNode> discovered) {
        if (!node.folder()) {
            if (scopeResolver.isInScope(node)) {
                discovered.add(node);
            }
            return;
        }

        if (!scopeResolver.shouldTraverse(node)) {
            return;
        }

        int skip = 0;
        while (true) {
            List<SourceNode> children = nuxeoClient.getChildren(node.nodeId(), skip, pageSize);
            if (children.isEmpty()) {
                break;
            }

            for (SourceNode child : children) {
                collectFromNode(child, scopeResolver, pageSize, discovered);
            }

            if (children.size() < pageSize) {
                break;
            }
            skip += children.size();
        }
    }

    private DiscoverySettings resolveSettings(NuxeoSyncRequest request) {
        List<String> includedRoots = firstNonEmpty(
                request.getIncludedRoots(),
                props.getScope().getIncludedRoots()
        );
        List<String> includedTypes = firstNonEmpty(
                request.getIncludedDocumentTypes(),
                props.getScope().getIncludedTypes()
        );
        List<String> excludedStates = firstNonEmpty(
                request.getExcludedLifecycleStates(),
                props.getScope().getExcludedLifecycleStates()
        );

        int configuredPageSize = props.getDiscovery().getPageSize();
        int pageSize = request.getPageSize() != null ? request.getPageSize() : configuredPageSize;
        pageSize = pageSize > 0 ? pageSize : configuredPageSize;

        NuxeoProperties.Mode discoveryMode = request.getDiscoveryMode() != null
                ? request.getDiscoveryMode()
                : props.getDiscovery().getMode();

        return new DiscoverySettings(
                sanitizeValues(includedRoots),
                sanitizeValues(includedTypes),
                sanitizeValues(excludedStates),
                pageSize,
                discoveryMode
        );
    }

    private String buildNxqlQuery(DiscoverySettings settings) {
        String pathClause = settings.includedRoots().stream()
                .map(root -> "ecm:path STARTSWITH '" + escapeNxql(root) + "'")
                .collect(java.util.stream.Collectors.joining(" OR ", "(", ")"));

        StringBuilder query = new StringBuilder("SELECT * FROM Document WHERE ")
                .append(pathClause);

        if (!settings.includedTypes().isEmpty()) {
            String typeClause = settings.includedTypes().stream()
                    .map(type -> "'" + escapeNxql(type) + "'")
                    .collect(java.util.stream.Collectors.joining(", "));
            query.append(" AND ecm:primaryType IN (").append(typeClause).append(")");
        }

        if (!settings.excludedLifecycleStates().isEmpty()) {
            String lifecycleClause = settings.excludedLifecycleStates().stream()
                    .map(state -> "'" + escapeNxql(state) + "'")
                    .collect(java.util.stream.Collectors.joining(", "));
            query.append(" AND ecm:currentLifeCycleState NOT IN (").append(lifecycleClause).append(")");
        }

        return query.append(" AND ecm:isProxy = 0")
                .append(" AND ecm:isCheckedInVersion = 0")
                .toString();
    }

    private static List<String> firstNonEmpty(List<String> preferred, List<String> fallback) {
        return preferred != null && !preferred.isEmpty() ? preferred : fallback;
    }

    private static List<String> sanitizeValues(Collection<String> values) {
        return values.stream()
                .filter(Objects::nonNull)
                .map(String::trim)
                .filter(value -> !value.isEmpty())
                .toList();
    }

    private static String escapeNxql(String value) {
        return value.replace("'", "''");
    }

    private record DiscoverySettings(List<String> includedRoots,
                                     List<String> includedTypes,
                                     List<String> excludedLifecycleStates,
                                     int pageSize,
                                     NuxeoProperties.Mode discoveryMode) {

        private NuxeoScopeResolver scopeResolver() {
            List<String> normalizedStates = excludedLifecycleStates.stream()
                    .map(state -> state.toLowerCase(Locale.ROOT))
                    .toList();
            return new NuxeoScopeResolver(includedRoots, includedTypes, normalizedStates);
        }
    }
}
