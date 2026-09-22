package org.hyland.contentlake.pluginhost.batch.controller;

import lombok.extern.slf4j.Slf4j;
import org.hyland.contentlake.pluginhost.batch.config.ConnectorBatchProperties;
import org.hyland.contentlake.pluginhost.batch.config.SelectedConnector;
import org.hyland.contentlake.pluginhost.batch.service.ConnectorDiscoveryService;
import org.hyland.contentlake.service.IndexReconciliationService;
import org.hyland.contentlake.service.RootSelection;
import org.hyland.contentlake.service.RootSelectionStore;
import org.hyland.contentlake.spi.ContentSourceClient;
import org.hyland.contentlake.spi.ScopeResolver;
import org.hyland.contentlake.spi.SourceNode;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Reads a connector's tree, so an operator can see what there is before choosing what to sync.
 *
 * <p>Deliberately connector-agnostic: it needs no new SPI method, because {@code getNode} and
 * {@code getChildren} already exist and the host already holds the client. So a folder picker built on this
 * works for every connector rather than for the one it was written against.</p>
 *
 * <h3>Why this is on the plugin host and not in core</h3>
 * <p>Core is component-scanned by all six ingester applications, and five of them drive a client they were
 * compiled against rather than one from the registry. A browse endpoint there would be present and broken on
 * five services. Here it has exactly one client to read.</p>
 *
 * <h3>It annotates scope rather than filtering by it</h3>
 * <p>Out-of-scope entries are returned and marked. Filtering them out would be actively wrong for the screen
 * this exists to serve: a scope resolver deliberately descends into folders an include pattern does not match
 * (so {@code /Finance/Reports} stays reachable through {@code /Finance}), so filtering would make a legal
 * selection unreachable, and an operator trying to understand why something is excluded would be shown an
 * empty tree.</p>
 *
 * <p>Authenticated by the host's default-deny rule, with no exemption added. It reads content metadata, names
 * and paths, so it is not less sensitive than the sync trigger beside it.</p>
 */
@Slf4j
@RestController
@RequestMapping("/api/browse")
public class BrowseController {

    /**
     * A ceiling on one request, so a caller cannot ask a connector for a million children in one call and
     * neither the host nor the source has to survive it.
     */
    private static final int MAX_PAGE = 500;
    private static final int DEFAULT_PAGE = 100;

    private final SelectedConnector connector;
    private final ConnectorBatchProperties properties;
    private final ObjectProvider<RootSelectionStore> selectionStores;

    public BrowseController(SelectedConnector connector,
                            ConnectorBatchProperties properties,
                            ObjectProvider<RootSelectionStore> selectionStores) {
        this.connector = connector;
        this.properties = properties;
        this.selectionStores = selectionStores;
    }

    /**
     * Where a tree starts, and which layer of the precedence chain decided that.
     *
     * <p>{@code resolvedFrom} is there so the screen can say why: an operator looking at a tree rooted
     * somewhere unexpected needs to know whether that came from their own selection, from configuration, or
     * from the connector.</p>
     */
    @GetMapping("/roots")
    public ResponseEntity<?> roots() {
        RootSelectionStore store = selectionStores.getIfAvailable();
        Optional<RootSelection> selection = store == null
                ? Optional.empty()
                : store.load(IndexReconciliationService.qualifiedSourceId(connector.client()));

        String resolvedFrom;
        List<String> rootIds;
        try {
            rootIds = ConnectorDiscoveryService.resolveRoots(selection, properties.getRoots(), client());
            resolvedFrom = selection.isPresent() ? "selection"
                    : properties.getRoots().stream().anyMatch(root -> root != null && !root.isBlank())
                            ? "connector.roots"
                            : "connector";
        } catch (IllegalStateException e) {
            // Nothing anywhere names a root. That is a configuration state the screen has to be able to show,
            // not an error to hide behind a 500: the operator's next action is to choose one.
            return ResponseEntity.ok(new BrowseRoots(connector.sourceType(), "none", List.of(),
                    List.of(e.getMessage())));
        }

        List<BrowseNode> roots = new ArrayList<>();
        List<String> problems = new ArrayList<>();
        for (String rootId : rootIds) {
            try {
                SourceNode node = client().getNode(rootId);
                if (node == null) {
                    problems.add("Root '" + rootId + "' does not exist in the source");
                    continue;
                }
                roots.add(describe(node));
            } catch (RuntimeException e) {
                problems.add("Root '" + rootId + "' could not be read: " + e.getMessage());
            }
        }
        return ResponseEntity.ok(new BrowseRoots(connector.sourceType(), resolvedFrom, roots, problems));
    }

    /** One node, for a screen that has an id and wants its name, path and scope. */
    @GetMapping("/node")
    public ResponseEntity<?> node(@RequestParam String nodeId) {
        try {
            SourceNode node = client().getNode(nodeId);
            if (node == null) {
                return notFound(nodeId);
            }
            return ResponseEntity.ok(describe(node));
        } catch (RuntimeException e) {
            return upstream(nodeId, e);
        }
    }

    /**
     * One page of a container's children.
     *
     * <p>{@code nodeId} is a query parameter rather than a path variable on purpose. Node ids contain
     * slashes, colons and exclamation marks depending on the source, and a path variable carrying a slash
     * needs encoded-slash handling that Tomcat rejects by default and a proxy mangles. A query parameter
     * sidesteps the whole class of problem.</p>
     *
     * <p>The paging contract is the SPI's, and it is easy to get wrong in a UI: a page shorter than
     * {@code maxItems} does <em>not</em> mean the container is exhausted, because a connector may drop
     * entries it cannot represent. Only an empty page does. So {@code nextSkip} advances by the requested
     * page size rather than by the number of entries returned, and {@code endOfContainer} is set only on an
     * empty page. Advancing by the size received re-reads dropped entries or shifts every later window.</p>
     */
    @GetMapping("/children")
    public ResponseEntity<?> children(@RequestParam String nodeId,
                                      @RequestParam(defaultValue = "0") int skip,
                                      @RequestParam(defaultValue = "" + DEFAULT_PAGE) int maxItems) {
        int page = Math.min(Math.max(1, maxItems), MAX_PAGE);
        int from = Math.max(0, skip);

        try {
            List<SourceNode> children = client().getChildren(nodeId, from, page);
            List<BrowseNode> described = new ArrayList<>();
            for (SourceNode child : children) {
                if (child != null) {
                    described.add(describe(child));
                }
            }
            return ResponseEntity.ok(new BrowsePage(nodeId, from, page, from + page,
                    children.isEmpty(), described));
        } catch (RuntimeException e) {
            return upstream(nodeId, e);
        }
    }

    /**
     * A node as the screen needs it, and nothing more.
     *
     * <p>Read principals, deny principals and source properties are deliberately not echoed. They are an
     * access-control decision and a source's internal metadata, and this endpoint exists to draw a tree.</p>
     */
    private BrowseNode describe(SourceNode node) {
        ScopeResolver scope = connector.scopeResolver();
        return new BrowseNode(
                node.nodeId(),
                node.name(),
                node.path(),
                node.folder(),
                node.mimeType(),
                node.modifiedAt(),
                scope.isInScope(node),
                node.folder() && scope.shouldTraverse(node));
    }

    private ContentSourceClient client() {
        return connector.client();
    }

    /** A node the connector answered about, saying it is not there. */
    private ResponseEntity<Problem> notFound(String nodeId) {
        return ResponseEntity.status(HttpStatus.NOT_FOUND)
                .body(new Problem("No node '" + nodeId + "' in source '" + connector.sourceType() + "'"));
    }

    /**
     * The connector threw, which is a statement about the source rather than about the request.
     *
     * <p>502 rather than 500: the host is working and the thing behind it is not, and a screen showing "the
     * source could not be reached" is more use than one showing an internal error. The connector's own message
     * is passed through, because it is the only thing that names what failed.</p>
     */
    private ResponseEntity<Problem> upstream(String nodeId, RuntimeException e) {
        log.warn("Browsing '{}' failed in source '{}': {}", nodeId, connector.sourceType(), e.getMessage());
        return ResponseEntity.status(HttpStatus.BAD_GATEWAY)
                .body(new Problem("Source '" + connector.sourceType() + "' could not answer for '" + nodeId
                        + "': " + e.getMessage()));
    }

    /**
     * @param inScope     whether a sync would index this node, advisory only
     * @param traversable whether a walk would descend into it, which is not the same thing: a resolver
     *                    descends into a folder an include pattern does not match, so that a matching
     *                    descendant stays reachable
     */
    public record BrowseNode(String nodeId,
                             String name,
                             String path,
                             boolean folder,
                             String mimeType,
                             OffsetDateTime modifiedAt,
                             boolean inScope,
                             boolean traversable) {
    }

    /**
     * @param nextSkip       what to pass as {@code skip} for the following page, always advanced by the
     *                       requested page size
     * @param endOfContainer set only on an empty page, because a short page does not mean exhaustion
     */
    public record BrowsePage(String nodeId,
                             int skip,
                             int maxItems,
                             int nextSkip,
                             boolean endOfContainer,
                             List<BrowseNode> nodes) {
    }

    /**
     * @param resolvedFrom {@code selection}, {@code connector.roots}, {@code connector}, or {@code none}
     * @param problems     roots that are configured but unreadable, reported alongside the ones that worked
     *                     rather than failing the whole request
     */
    public record BrowseRoots(String sourceType,
                              String resolvedFrom,
                              List<BrowseNode> roots,
                              List<String> problems) {
    }

    public record Problem(String message) {
    }
}
