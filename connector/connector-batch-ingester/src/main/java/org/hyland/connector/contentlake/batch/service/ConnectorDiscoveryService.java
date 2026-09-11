package org.hyland.connector.contentlake.batch.service;

import lombok.extern.slf4j.Slf4j;
import org.hyland.connector.contentlake.batch.config.ConnectorBatchProperties;
import org.hyland.connector.contentlake.batch.config.SelectedConnector;
import org.hyland.contentlake.service.DiscoveryOutcome;
import org.hyland.contentlake.spi.ContentSourceClient;
import org.hyland.contentlake.spi.ScopeResolver;
import org.hyland.contentlake.spi.SourceNode;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Walks a plugin connector's containers through the SPI alone, returning the in-scope documents (#132).
 *
 * <p>The same shape as the filesystem walker, with three differences that all come from not knowing the
 * source:</p>
 *
 * <ul>
 *   <li><strong>Node ids are visited once.</strong> A filesystem path is in exactly one directory, but CMIS
 *       multi-filing puts one document in several folders, and a walk that does not remember what it has
 *       seen ingests such a document once per parent. Its second ingest is not even harmless: the pipeline
 *       is idempotent per node, so the duplicate work is silent.</li>
 *   <li><strong>A depth cap.</strong> The visited set handles multi-filing and ordinary cycles. It cannot
 *       help against a source that mints a fresh id for the same container on each listing, so the walk
 *       also stops descending at {@code connector.max-depth} and says so.</li>
 *   <li><strong>A failed container is contained.</strong> One listing that throws costs its subtree, not
 *       the pass: the nodes already found are ingested and the outcome is marked incomplete, which is what
 *       stops the reconciliation sweep from reading the gap as "deleted at source". A root that cannot be
 *       read is different and propagates, because a pass that enumerated none of its scope has nothing
 *       worth keeping.</li>
 * </ul>
 */
@Slf4j
public class ConnectorDiscoveryService {

    private final ContentSourceClient client;
    private final ScopeResolver scopeResolver;
    private final List<String> roots;
    private final int pageSize;
    private final int maxDepth;

    public ConnectorDiscoveryService(SelectedConnector connector,
                                     List<String> roots,
                                     ConnectorBatchProperties properties) {
        this.client = connector.client();
        this.scopeResolver = connector.scopeResolver();
        this.roots = List.copyOf(roots);
        this.pageSize = Math.max(1, properties.getPageSize());
        this.maxDepth = Math.max(1, properties.getMaxDepth());
    }

    /**
     * The entry points for a batch pass: configured roots when there are any, otherwise whatever the
     * connector names as its own.
     *
     * <p>Resolved at startup rather than per sync, so a connector that can neither be configured with a root
     * nor name one fails the container rather than reporting an empty source on every run.</p>
     *
     * @throws IllegalStateException when neither source yields a root
     */
    public static List<String> resolveRoots(List<String> configured, ContentSourceClient client) {
        List<String> fromConfig = configured == null ? List.of() : configured.stream()
                .filter(root -> root != null && !root.isBlank())
                .map(String::trim)
                .distinct()
                .toList();
        if (!fromConfig.isEmpty()) {
            return fromConfig;
        }

        String own = client.getRootNodeId();
        if (own != null && !own.isBlank()) {
            return List.of(own.trim());
        }

        throw new IllegalStateException(
                "Connector '" + client.getSourceType() + "' does not name a root container and connector.roots "
                        + "is empty, so a batch pass has nowhere to start. Set connector.roots to one or more "
                        + "node ids, or implement ContentSourceClient.getRootNodeId() in the connector.");
    }

    /** A discovery pass and its own account of whether it covered its whole scope. */
    public record ConnectorDiscovery(List<SourceNode> nodes, DiscoveryOutcome outcome) {
    }

    public List<SourceNode> discover() {
        return discoverTallied().nodes();
    }

    /**
     * Walks every root, depth first.
     *
     * @throws RuntimeException whatever the connector throws for a root it cannot read
     */
    public ConnectorDiscovery discoverTallied() {
        List<SourceNode> discovered = new ArrayList<>();
        List<String> resolvedRootPaths = new ArrayList<>();
        List<String> reasons = new ArrayList<>();
        Set<String> visited = new LinkedHashSet<>();

        for (String rootId : roots) {
            SourceNode root = client.getNode(rootId);
            if (root == null) {
                // Distinct from a listing failure: the connector answered, and the answer is that the
                // configured root is not there. Nothing under it can be enumerated, so the sweep must not
                // treat this pass as authoritative.
                String reason = "Root '" + rootId + "' does not exist in the source";
                log.error("Connector discovery skipped a root that does not exist: {}", rootId);
                reasons.add(reason);
                continue;
            }
            resolvedRootPaths.add(root.path() != null && !root.path().isBlank() ? root.path() : rootId);
            collect(root, discovered, visited, reasons, 0);
        }

        DiscoveryOutcome outcome = reasons.isEmpty()
                ? DiscoveryOutcome.complete(resolvedRootPaths)
                : DiscoveryOutcome.incomplete(resolvedRootPaths, reasons);

        log.info("Connector discovery over {} root(s) found {} in-scope node(s); pass {}",
                roots.size(), discovered.size(),
                outcome.complete() ? "complete" : "incomplete: " + outcome.reasonSummary());
        return new ConnectorDiscovery(discovered, outcome);
    }

    private void collect(SourceNode node,
                         List<SourceNode> discovered,
                         Set<String> visited,
                         List<String> reasons,
                         int depth) {
        if (node.nodeId() == null || !visited.add(node.nodeId())) {
            // Already walked, under another parent or on a previous root. Not a problem and not a reason
            // to call the pass incomplete: everything reachable through this node was enumerated then.
            return;
        }

        if (!node.folder()) {
            if (scopeResolver.isInScope(node)) {
                discovered.add(node);
            }
            return;
        }

        if (!scopeResolver.shouldTraverse(node)) {
            return;
        }

        if (depth >= maxDepth) {
            String reason = "Depth limit " + maxDepth + " reached at container '" + node.nodeId()
                    + "', so its contents were not enumerated";
            log.warn("Connector discovery stopped descending at container {}: depth limit {} reached",
                    node.nodeId(), maxDepth);
            reasons.add(reason);
            return;
        }

        int skip = 0;
        while (true) {
            List<SourceNode> children;
            try {
                children = client.getChildren(node.nodeId(), skip, pageSize);
            } catch (Exception e) {
                String reason = "Listing container '" + node.nodeId() + "' failed: " + e.getMessage();
                log.error("Connector discovery could not list container {}; skipping its remaining children",
                        node.nodeId(), e);
                reasons.add(reason);
                return;
            }
            if (children == null || children.isEmpty()) {
                return;
            }
            for (SourceNode child : children) {
                if (child != null) {
                    collect(child, discovered, visited, reasons, depth + 1);
                }
            }
            if (children.size() < pageSize) {
                return;
            }
            skip += children.size();
        }
    }
}
