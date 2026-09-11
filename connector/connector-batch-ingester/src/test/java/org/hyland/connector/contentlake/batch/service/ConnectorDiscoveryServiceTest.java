package org.hyland.connector.contentlake.batch.service;

import org.hyland.connector.contentlake.batch.config.ConnectorBatchProperties;
import org.hyland.connector.contentlake.batch.config.SelectedConnector;
import org.hyland.contentlake.connector.DefaultScopeResolver;
import org.hyland.contentlake.spi.ContentSourceClient;
import org.hyland.contentlake.spi.ScopeResolver;
import org.hyland.contentlake.spi.SourceNode;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.core.io.Resource;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The generic container walk (#132).
 *
 * <p>The cases that matter are the ones where a connector behaves unlike a filesystem: a document filed
 * under two folders, a container that cannot be listed, a hierarchy that does not bottom out.</p>
 */
class ConnectorDiscoveryServiceTest {

    private static final String ROOT = "root";

    /** A container graph, so a test states a source's shape rather than stubbing call by call. */
    private static class FakeSource implements ContentSourceClient {

        private final Map<String, SourceNode> nodes = new LinkedHashMap<>();
        private final Map<String, List<String>> children = new LinkedHashMap<>();
        private final Set<String> unlistable = new java.util.LinkedHashSet<>();
        private final Set<String> unreadable = new java.util.LinkedHashSet<>();
        private final AtomicInteger listings = new AtomicInteger();
        private String ownRoot;

        FakeSource folder(String id, String... childIds) {
            nodes.put(id, node(id, true));
            children.put(id, List.of(childIds));
            return this;
        }

        FakeSource document(String id) {
            nodes.put(id, node(id, false));
            return this;
        }

        FakeSource unlistable(String id) {
            unlistable.add(id);
            return this;
        }

        /** A node the connector cannot even fetch, as opposed to one whose children it cannot list. */
        FakeSource unreadable(String id) {
            unreadable.add(id);
            return this;
        }

        FakeSource ownRoot(String id) {
            this.ownRoot = id;
            return this;
        }

        @Override
        public String getSourceId() {
            return "instance-1";
        }

        @Override
        public String getSourceType() {
            return "sample";
        }

        @Override
        public String getRootNodeId() {
            return ownRoot;
        }

        @Override
        public SourceNode getNode(String nodeId) {
            if (unreadable.contains(nodeId)) {
                throw new IllegalStateException("reading " + nodeId + " is not permitted");
            }
            return nodes.get(nodeId);
        }

        @Override
        public List<SourceNode> getChildren(String containerId, int skip, int maxItems) {
            listings.incrementAndGet();
            if (unlistable.contains(containerId)) {
                throw new IllegalStateException("listing " + containerId + " is not permitted");
            }
            List<String> ids = children.getOrDefault(containerId, List.of());
            List<SourceNode> page = new ArrayList<>();
            for (int i = skip; i < Math.min(ids.size(), skip + maxItems); i++) {
                page.add(nodes.get(ids.get(i)));
            }
            return page;
        }

        @Override
        public Resource downloadContent(String nodeId, String fileName) {
            return null;
        }

        @Override
        public byte[] getContent(String nodeId) {
            return new byte[0];
        }
    }

    @Test
    void walksNestedContainersInFull() {
        FakeSource source = new FakeSource()
                .folder(ROOT, "a.txt", "sub")
                .document("a.txt")
                .folder("sub", "b.txt")
                .document("b.txt");

        ConnectorDiscoveryService.ConnectorDiscovery discovery = discover(source, List.of(ROOT));

        assertThat(discovery.nodes()).extracting(SourceNode::nodeId).containsExactly("a.txt", "b.txt");
        assertThat(discovery.outcome().complete()).isTrue();
        assertThat(discovery.outcome().resolvedRootPaths()).containsExactly("/" + ROOT);
    }

    /** A short final page ends the loop; a full one asks for more. */
    @Test
    void pagesThroughAContainer() {
        FakeSource source = new FakeSource().folder(ROOT, "a", "b", "c", "d", "e");
        for (String id : List.of("a", "b", "c", "d", "e")) {
            source.document(id);
        }

        ConnectorDiscoveryService.ConnectorDiscovery discovery =
                discover(source, List.of(ROOT), props -> props.setPageSize(2));

        assertThat(discovery.nodes()).extracting(SourceNode::nodeId)
                .containsExactly("a", "b", "c", "d", "e");
        // 2 + 2 + 1: the short page ends it, so no fourth call.
        assertThat(source.listings).hasValue(3);
        assertThat(discovery.outcome().complete()).isTrue();
    }

    /**
     * The case a filesystem walk never has to handle. CMIS multi-filing puts one document in several
     * folders; without the visited set it is ingested once per parent, and the duplicate work is silent
     * because the pipeline is idempotent per node.
     */
    @Test
    void ingestsADocumentFiledUnderTwoContainersOnce() {
        FakeSource source = new FakeSource()
                .folder(ROOT, "one", "two")
                .folder("one", "shared.txt")
                .folder("two", "shared.txt")
                .document("shared.txt");

        ConnectorDiscoveryService.ConnectorDiscovery discovery = discover(source, List.of(ROOT));

        assertThat(discovery.nodes()).extracting(SourceNode::nodeId).containsExactly("shared.txt");
        // Multi-filing is normal, not a gap in coverage.
        assertThat(discovery.outcome().complete()).isTrue();
    }

    @Test
    void doesNotRevisitAContainerReachedFromASecondRoot() {
        FakeSource source = new FakeSource()
                .folder("root-a", "shared")
                .folder("root-b", "shared")
                .folder("shared", "x.txt")
                .document("x.txt");

        ConnectorDiscoveryService.ConnectorDiscovery discovery =
                discover(source, List.of("root-a", "root-b"));

        assertThat(discovery.nodes()).extracting(SourceNode::nodeId).containsExactly("x.txt");
        assertThat(discovery.outcome().resolvedRootPaths()).containsExactly("/root-a", "/root-b");
    }

    @Test
    void aContainerThatCannotBeListedCostsItsSubtreeAndMarksThePassIncomplete() {
        FakeSource source = new FakeSource()
                .folder(ROOT, "a.txt", "locked")
                .document("a.txt")
                .folder("locked", "hidden.txt")
                .document("hidden.txt")
                .unlistable("locked");

        ConnectorDiscoveryService.ConnectorDiscovery discovery = discover(source, List.of(ROOT));

        // What was found is still ingested.
        assertThat(discovery.nodes()).extracting(SourceNode::nodeId).containsExactly("a.txt");
        // And the sweep must not read the gap as "deleted at source".
        assertThat(discovery.outcome().complete()).isFalse();
        assertThat(discovery.outcome().reasonSummary()).contains("locked");
    }

    /** A pass that could not even fetch its root has nothing worth keeping, so that failure propagates. */
    @Test
    void aRootThatCannotBeFetchedFailsThePass() {
        FakeSource source = new FakeSource().folder(ROOT).unreadable(ROOT);

        assertThatThrownBy(() -> discover(source, List.of(ROOT)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("reading " + ROOT);
    }

    /**
     * A root whose listing fails is the containment case applied to a root: the pass returns, with nothing
     * found and its incompleteness on record, rather than failing the job.
     */
    @Test
    void aRootThatCannotBeListedYieldsAnIncompletePass() {
        FakeSource source = new FakeSource().folder(ROOT).unlistable(ROOT);

        ConnectorDiscoveryService.ConnectorDiscovery discovery = discover(source, List.of(ROOT));

        assertThat(discovery.nodes()).isEmpty();
        assertThat(discovery.outcome().complete()).isFalse();
    }

    /**
     * A root the connector says does not exist is not an error but is not authoritative either: nothing
     * under it was enumerated.
     */
    @Test
    void aMissingRootIsReportedAndTheOtherRootsAreStillWalked() {
        FakeSource source = new FakeSource().folder(ROOT, "a.txt").document("a.txt");

        ConnectorDiscoveryService.ConnectorDiscovery discovery =
                discover(source, List.of("gone", ROOT));

        assertThat(discovery.nodes()).extracting(SourceNode::nodeId).containsExactly("a.txt");
        assertThat(discovery.outcome().complete()).isFalse();
        assertThat(discovery.outcome().reasonSummary()).contains("gone");
        assertThat(discovery.outcome().resolvedRootPaths()).containsExactly("/" + ROOT);
    }

    /**
     * The visited set cannot help against a source that hands back a fresh id for the same container on
     * every listing, so the depth cap has to terminate the walk on its own.
     */
    @Test
    void theDepthCapTerminatesAnUnboundedHierarchy() {
        ContentSourceClient endless = new ContentSourceClient() {
            @Override
            public String getSourceId() {
                return "instance-1";
            }

            @Override
            public String getSourceType() {
                return "sample";
            }

            @Override
            public SourceNode getNode(String nodeId) {
                return node(nodeId, true);
            }

            @Override
            public List<SourceNode> getChildren(String containerId, int skip, int maxItems) {
                return skip > 0 ? List.of() : List.of(node(containerId + "/deeper", true));
            }

            @Override
            public Resource downloadContent(String nodeId, String fileName) {
                return null;
            }

            @Override
            public byte[] getContent(String nodeId) {
                return new byte[0];
            }
        };

        ConnectorDiscoveryService.ConnectorDiscovery discovery =
                discover(endless, List.of(ROOT), props -> props.setMaxDepth(3));

        assertThat(discovery.outcome().complete()).isFalse();
        assertThat(discovery.outcome().reasonSummary()).contains("Depth limit 3");
    }

    @Test
    void honoursTheScopeResolverForDocumentsAndContainers() {
        FakeSource source = new FakeSource()
                .folder(ROOT, "keep.txt", "drop.txt", "skipped")
                .document("keep.txt")
                .document("drop.txt")
                .folder("skipped", "buried.txt")
                .document("buried.txt");

        ScopeResolver selective = new ScopeResolver() {
            @Override
            public boolean isInScope(SourceNode node) {
                return !node.folder() && !"drop.txt".equals(node.nodeId());
            }

            @Override
            public boolean shouldTraverse(SourceNode node) {
                return node.folder() && !"skipped".equals(node.nodeId());
            }
        };

        ConnectorDiscoveryService service = new ConnectorDiscoveryService(
                selected(source, selective), List.of(ROOT), new ConnectorBatchProperties());

        // A pruned subtree is a scope decision, not a coverage gap.
        assertThat(service.discover()).extracting(SourceNode::nodeId).containsExactly("keep.txt");
    }

    @Test
    void configuredRootsWinOverTheConnectorsOwn() {
        FakeSource source = new FakeSource().ownRoot("its-own-root");

        assertThat(ConnectorDiscoveryService.resolveRoots(List.of("configured"), source))
                .containsExactly("configured");
    }

    @Test
    void blankAndDuplicateConfiguredRootsAreDropped() {
        FakeSource source = new FakeSource();

        assertThat(ConnectorDiscoveryService.resolveRoots(
                List.of("  a  ", "", "a", "b"), source))
                .containsExactly("a", "b");
    }

    @Test
    void fallsBackToTheConnectorsOwnRoot() {
        FakeSource source = new FakeSource().ownRoot("its-own-root");

        assertThat(ConnectorDiscoveryService.resolveRoots(List.of(), source))
                .containsExactly("its-own-root");
    }

    /** Failing here is what stops the service reporting an empty source on every run instead. */
    @Test
    void failsWhenNeitherConfigurationNorTheConnectorNamesARoot() {
        assertThatThrownBy(() -> ConnectorDiscoveryService.resolveRoots(null, new FakeSource()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("connector.roots")
                .hasMessageContaining("getRootNodeId");
    }

    private static ConnectorDiscoveryService.ConnectorDiscovery discover(ContentSourceClient client,
                                                                        List<String> roots) {
        return discover(client, roots, props -> {});
    }

    private static ConnectorDiscoveryService.ConnectorDiscovery discover(
            ContentSourceClient client,
            List<String> roots,
            java.util.function.Consumer<ConnectorBatchProperties> customiser) {
        ConnectorBatchProperties props = new ConnectorBatchProperties();
        customiser.accept(props);
        return new ConnectorDiscoveryService(selected(client, new DefaultScopeResolver()), roots, props)
                .discoverTallied();
    }

    private static SelectedConnector selected(ContentSourceClient client, ScopeResolver scopeResolver) {
        return new SelectedConnector("sample", "sample connector", "sample.jar", client, scopeResolver,
                Mockito.mock(org.hyland.contentlake.spi.TextExtractor.class), false);
    }

    private static SourceNode node(String nodeId, boolean folder) {
        return new SourceNode(nodeId, "instance-1", "sample", nodeId, "/" + nodeId,
                folder ? null : "text/plain", null, folder, Set.of("__Everyone__"), Set.of(), Map.of());
    }
}
