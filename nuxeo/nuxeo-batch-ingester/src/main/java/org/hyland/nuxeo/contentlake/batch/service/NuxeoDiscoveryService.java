package org.hyland.nuxeo.contentlake.batch.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.hyland.nuxeo.contentlake.client.NuxeoClient;
import org.hyland.nuxeo.contentlake.config.NuxeoProperties;
import org.hyland.nuxeo.contentlake.model.NuxeoDocument;
import org.hyland.nuxeo.contentlake.batch.model.NuxeoSyncRequest;
import org.hyland.nuxeo.contentlake.service.NuxeoScopeResolver;
import org.hyland.contentlake.service.DiscoveryOutcome;
import org.hyland.contentlake.spi.SourceNode;
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

    /**
     * A discovery pass and its own account of whether it covered its whole scope.
     *
     * @param nodes   the discovered nodes
     * @param outcome whether every configured root was enumerated in full, and the roots covered
     */
    public record NuxeoDiscovery(List<SourceNode> nodes, DiscoveryOutcome outcome) {
    }

    public List<SourceNode> discoverFromConfig() {
        return discoverFromConfigTallied().nodes();
    }

    public List<SourceNode> discover(NuxeoSyncRequest request) {
        return discoverTallied(request).nodes();
    }

    /** As {@link #discoverFromConfig}, but also reporting the pass's completeness. */
    public NuxeoDiscovery discoverFromConfigTallied() {
        return discoverTallied(new NuxeoSyncRequest());
    }

    /** As {@link #discover}, but also reporting the pass's completeness. */
    public NuxeoDiscovery discoverTallied(NuxeoSyncRequest request) {
        DiscoverySettings settings = resolveSettings(request);
        List<String> reasons = new ArrayList<>();
        List<SourceNode> nodes = switch (settings.discoveryMode()) {
            case CHILDREN -> discoverWithChildren(settings, reasons);
            case NXQL -> discoverWithNxqlOrFallback(settings, reasons);
        };
        DiscoveryOutcome outcome = reasons.isEmpty()
                ? DiscoveryOutcome.complete(settings.includedRoots())
                : DiscoveryOutcome.incomplete(settings.includedRoots(), reasons);
        return new NuxeoDiscovery(nodes, outcome);
    }

    private List<SourceNode> discoverWithNxqlOrFallback(DiscoverySettings settings, List<String> reasons) {
        try {
            return discoverWithNxql(settings, reasons);
        } catch (UnsupportedOperationException e) {
            // NOT a completeness problem. The fallback covers the same roots and applies
            // scopeResolver.isInScope in place of the NXQL predicates, so it can only over-include,
            // and over-inclusion cannot cause a deletion.
            log.warn("NXQL discovery is unavailable; falling back to @children traversal: {}", e.getMessage());
            return discoverWithChildren(settings, reasons);
        }
    }

    private List<SourceNode> discoverWithNxql(DiscoverySettings settings, List<String> reasons) {
        NuxeoScopeResolver scopeResolver = settings.scopeResolver();
        String query = buildNxqlQuery(settings);
        int pageIndex = 0;
        List<SourceNode> discovered = new ArrayList<>();

        while (true) {
            NuxeoDocument.Page page = nuxeoClient.searchPageByNxql(query, pageIndex, settings.pageSize());
            if (page.getEntries().isEmpty()) {
                break;
            }
            page.getEntries().stream()
                    .map(nuxeoClient::toSourceNode)
                    .filter(scopeResolver::isInScope)
                    .forEach(discovered::add);

            if (page.getEntries().size() < settings.pageSize() || !page.hasMore()) {
                // A short page while Nuxeo still reports more is a silent truncation: Page exposes no
                // total, so the loop cannot tell it apart from the last page.
                if (page.getEntries().size() < settings.pageSize() && page.hasMore()) {
                    log.warn("NXQL discovery stopped on a short page while Nuxeo still reports more "
                            + "results; the pass is incomplete");
                    reasons.add("NXQL paging ended on a short page while more results were reported");
                }
                break;
            }
            pageIndex++;
        }

        return discovered;
    }

    private List<SourceNode> discoverWithChildren(DiscoverySettings settings, List<String> reasons) {
        NuxeoScopeResolver scopeResolver = settings.scopeResolver();
        List<SourceNode> discovered = new ArrayList<>();

        for (String rootPath : settings.includedRoots()) {
            SourceNode root = nuxeoClient.getNodeByPath(rootPath);
            if (root == null) {
                log.warn("Configured Nuxeo root path not found: {}", rootPath);
                reasons.add("configured root path '" + rootPath + "' was not found");
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

            // Only an empty page ends a container, and the cursor advances by what was asked for rather
            // than by what came back. The SPI does not promise a full page while more remains.
            skip += pageSize;
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
                discoveryMode,
                nuxeoClient
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
                                     NuxeoProperties.Mode discoveryMode,
                                     NuxeoClient nuxeoClient) {

        private NuxeoScopeResolver scopeResolver() {
            List<String> normalizedStates = excludedLifecycleStates.stream()
                    .map(state -> state.toLowerCase(Locale.ROOT))
                    .toList();
            return new NuxeoScopeResolver(includedRoots, includedTypes, normalizedStates, nuxeoClient);
        }
    }
}
