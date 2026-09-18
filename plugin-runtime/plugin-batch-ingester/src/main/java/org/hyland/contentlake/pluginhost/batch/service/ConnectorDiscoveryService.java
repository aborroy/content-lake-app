package org.hyland.contentlake.pluginhost.batch.service;

import lombok.extern.slf4j.Slf4j;
import org.hyland.contentlake.pluginhost.batch.config.ConnectorBatchProperties;
import org.hyland.contentlake.pluginhost.batch.config.SelectedConnector;
import org.hyland.contentlake.service.DiscoveryOutcome;
import org.hyland.contentlake.spi.ContentSourceClient;
import org.hyland.contentlake.spi.ScopeResolver;
import org.hyland.contentlake.spi.SourceChangePage;
import org.hyland.contentlake.spi.SourceNode;
import org.hyland.contentlake.spi.SourceTombstone;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Walks a plugin connector's containers through the SPI alone, returning the in-scope documents (#132).
 *
 * <p>The same shape as the filesystem walker, with four differences that all come from not knowing the
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
 *   <li><strong>A page bound.</strong> Only an empty page ends a container, because a connector is allowed
 *       to return a short one. A connector that ignores {@code skip} would therefore never end, so the
 *       listing is abandoned as incomplete after {@link #MAX_PAGES_PER_CONTAINER} non-empty pages.</li>
 * </ul>
 */
@Slf4j
public class ConnectorDiscoveryService {

    /**
     * How many non-empty pages one container may produce before its listing is abandoned as incomplete.
     *
     * <p>Not a scope setting, so not configurable: it is the point past which the connector is misbehaving
     * rather than the container being large. At the default page size that is a million entries in one
     * container.</p>
     */
    private static final int MAX_PAGES_PER_CONTAINER = 10_000;

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

    /**
     * What a connector's change feed reported, and how far through it the pass got.
     *
     * @param nodes         in-scope documents to ingest, in feed order
     * @param deletions     nodes to remove: what the feed reported gone, plus anything it reported changed
     *                      that the scope resolver no longer accepts. In an incremental pass the feed is
     *                      the only mechanism that can notice either, since no sweep runs
     * @param nextCursor    the furthest cursor the pass consumed, or {@code null} when it consumed none
     * @param cursorExpired the source rejected the cursor it was given, so nothing here is usable and the
     *                      caller has to forget its cursor and walk
     * @param drained       whether the feed reported no further pages. {@code false} means the window is
     *                      only partly consumed, which is not a failure: the next pass resumes from
     *                      {@code nextCursor}
     * @param outcome       the same completeness claim a walk makes. On an incremental pass
     *                      {@code complete} means the feed was drained, and the covered root paths are
     *                      empty because a feed does not enumerate a scope
     */
    public record ConnectorChanges(List<SourceNode> nodes,
                                   List<SourceTombstone> deletions,
                                   String nextCursor,
                                   boolean cursorExpired,
                                   boolean drained,
                                   DiscoveryOutcome outcome) {

        public ConnectorChanges {
            nodes = nodes == null ? List.of() : List.copyOf(nodes);
            deletions = deletions == null ? List.of() : List.copyOf(deletions);
        }

        /** The cursor is gone; the caller falls back to a walk in the same job. */
        static ConnectorChanges expired(String reason) {
            return new ConnectorChanges(List.of(), List.of(), null, true, false,
                    DiscoveryOutcome.incomplete(List.of(), List.of(reason)));
        }
    }

    /**
     * Reads a connector's change feed instead of walking it, up to {@code maxPages} pages.
     *
     * <p>Only the connector host does this, and only for a connector that answers
     * {@link ContentSourceClient#supportsChangeFeed()}. A feed page is a first-hand statement about the
     * nodes it names, so its deletions need none of the ratio guards a walk's inferred deletions do -- but
     * for the same reason a feed cannot notice anything it failed to report, which is what
     * {@code connector.change-feed.full-walk-every} exists to correct.</p>
     *
     * <p>Folders are dropped rather than ingested, matching the walk: a container has no content to
     * extract. A changed document the scope resolver rejects becomes an out-of-scope deletion, because a
     * node leaving scope is indistinguishable, from the index's side, from a node being removed, and no
     * sweep will run to notice.</p>
     *
     * @param cursor   where to resume; never {@code null} in practice, since a caller with no cursor walks
     * @param pageSize soft bound on the changes requested per page
     * @param maxPages how many pages one pass may consume before leaving the rest for the next one
     */
    public ConnectorChanges discoverIncremental(String cursor, int pageSize, int maxPages) {
        List<SourceNode> nodes = new ArrayList<>();
        List<SourceTombstone> deletions = new ArrayList<>();
        List<String> reasons = new ArrayList<>();
        int effectivePageSize = Math.max(1, pageSize);
        int pageBudget = Math.max(1, maxPages);

        String position = cursor;
        String consumed = null;
        boolean drained = false;

        for (int page = 0; page < pageBudget; page++) {
            SourceChangePage changes;
            try {
                changes = client.changesSince(position, effectivePageSize);
            } catch (Exception e) {
                // Contained like a failed container listing: what earlier pages reported is already correct,
                // and the pass simply did not finish.
                String reason = "Reading the change feed after cursor '" + position + "' failed: "
                        + e.getMessage();
                log.error("Connector change feed failed after {} page(s); keeping what it reported so far",
                        page, e);
                reasons.add(reason);
                break;
            }

            if (changes == null) {
                String reason = "The change feed returned no page for cursor '" + position + "'";
                log.error("Connector change feed returned null for cursor {}", position);
                reasons.add(reason);
                break;
            }
            if (changes.cursorExpired()) {
                log.warn("Connector change feed reports cursor '{}' expired after {} page(s); "
                        + "the cursor will be cleared and the source walked", position, page);
                return ConnectorChanges.expired("Cursor '" + position + "' expired at the source");
            }

            for (SourceNode node : changes.changed()) {
                if (node == null || node.nodeId() == null) {
                    continue;
                }
                if (node.folder()) {
                    continue;
                }
                if (scopeResolver.isInScope(node)) {
                    nodes.add(node);
                } else {
                    deletions.add(new SourceTombstone(node.nodeId(), node.modifiedAt(),
                            SourceTombstone.Reason.OUT_OF_SCOPE));
                }
            }
            for (SourceTombstone tombstone : changes.deleted()) {
                if (tombstone != null && tombstone.nodeId() != null && !tombstone.nodeId().isBlank()) {
                    deletions.add(tombstone);
                }
            }

            if (changes.nextCursor() != null && !changes.nextCursor().isBlank()) {
                consumed = changes.nextCursor();
                position = consumed;
            } else if (changes.moreAvailable()) {
                // More to come but nowhere to resume from: continuing would re-read the same window, and
                // the pass cannot claim the feed was drained.
                String reason = "The change feed reported more pages but no cursor to resume from";
                log.error("Connector change feed offered more pages without a next cursor; stopping");
                reasons.add(reason);
                break;
            }

            if (!changes.moreAvailable()) {
                drained = true;
                break;
            }
        }

        if (!drained && reasons.isEmpty()) {
            reasons.add("The change feed still had pages after the " + pageBudget
                    + "-page limit for one pass");
            log.info("Connector change feed stopped at the {}-page limit; the next pass resumes from the "
                    + "cursor it reached", pageBudget);
        }

        DiscoveryOutcome outcome = drained
                ? DiscoveryOutcome.complete(List.of())
                : DiscoveryOutcome.incomplete(List.of(), reasons);

        log.info("Connector change feed reported {} change(s) and {} deletion(s); feed {}",
                nodes.size(), deletions.size(), drained ? "drained" : "not drained");
        return new ConnectorChanges(nodes, deletions, consumed, false, drained, outcome);
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
        for (int page = 0; ; page++) {
            if (page >= MAX_PAGES_PER_CONTAINER) {
                // Only an empty page ends a container, so a connector that ignores skip and answers every
                // listing with the same non-empty page would spin here forever. The other walkers drive
                // in-tree clients that provably honour skip; this one drives whatever a jar implements.
                String reason = "Container '" + node.nodeId() + "' returned " + MAX_PAGES_PER_CONTAINER
                        + " non-empty pages without exhausting, so its listing was abandoned";
                log.error("Connector discovery abandoned container {} after {} pages; the connector may be "
                        + "ignoring the skip argument", node.nodeId(), MAX_PAGES_PER_CONTAINER);
                reasons.add(reason);
                return;
            }

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
            // A connector may return fewer than pageSize entries after dropping what it cannot represent
            // as a SourceNode, so a short page is not exhaustion. The cursor advances by what was asked
            // for rather than by what came back, or those drops would shift every later page window.
            skip += pageSize;
        }
    }
}
