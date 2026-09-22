package org.hyland.contentlake.pluginhost.batch.service;

import org.hyland.contentlake.connector.DefaultScopeResolver;
import org.hyland.contentlake.pluginhost.batch.config.ConnectorBatchProperties;
import org.hyland.contentlake.pluginhost.batch.config.SelectedConnector;
import org.hyland.contentlake.service.RootSelection;
import org.hyland.contentlake.spi.ContentSourceClient;
import org.hyland.contentlake.spi.ScopeResolver;
import org.hyland.contentlake.spi.SourceNode;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.core.io.Resource;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Where a pass gets its roots from, and the one case that could destroy a corpus if it went the other way.
 */
class RootSelectionPrecedenceTest {

    private static final String CONNECTOR_ROOT = "connector-root";

    @Test
    void aSelectionWinsOverConfiguredRootsAndTheConnectorsOwn() {
        List<String> roots = ConnectorDiscoveryService.resolveRoots(
                Optional.of(RootSelection.of(List.of("chosen-a", "chosen-b"), "admin")),
                List.of("configured"),
                new StubClient());

        assertThat(roots).containsExactly("chosen-a", "chosen-b");
    }

    @Test
    void configuredRootsWinWhenNobodyHasChosen() {
        List<String> roots = ConnectorDiscoveryService.resolveRoots(
                Optional.empty(), List.of("configured"), new StubClient());

        assertThat(roots).containsExactly("configured");
    }

    @Test
    void fallsBackToTheConnectorsOwnRootWhenThereIsNeither() {
        // The stub names no plural roots, so this also covers the SPI default path: the host asks only the
        // plural question, and a connector overriding only the singular one is still honoured through it.
        List<String> roots = ConnectorDiscoveryService.resolveRoots(
                Optional.empty(), List.of(), new StubClient());

        assertThat(roots).containsExactly(CONNECTOR_ROOT);
    }

    @Test
    void anEmptySelectionIsNotAFallThrough() {
        // The whole point of the precedence chain. "Chosen, and nothing" must not reach configured roots, or
        // emptying a picker would silently re-ingest the entire source.
        List<String> roots = ConnectorDiscoveryService.resolveRoots(
                Optional.of(RootSelection.of(List.of(), "admin")),
                List.of("configured"),
                new StubClient());

        assertThat(roots).isEmpty();
    }

    @Test
    void stillFailsWhenNothingAnywhereNamesARoot() {
        // Preserved from before the selection store existed: a deployment that can supply no root at all is a
        // misconfiguration worth refusing, not an empty source to report on every run.
        StubClient nameless = new StubClient();
        nameless.root = null;

        assertThatThrownBy(() -> ConnectorDiscoveryService.resolveRoots(Optional.empty(), List.of(), nameless))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("connector.roots")
                .hasMessageContaining("selection API");
    }

    @Test
    void takesEveryRootAConnectorNamesRatherThanOnlyTheFirst() {
        // What #158 is for: a connector with several entry points no longer has to answer null and make the
        // operator write composite ids into connector.roots by hand.
        StubClient multi = new StubClient();
        multi.roots = List.of("drive-one:root", "drive-two:root");

        List<String> roots = ConnectorDiscoveryService.resolveRoots(Optional.empty(), List.of(), multi);

        assertThat(roots).containsExactly("drive-one:root", "drive-two:root");
    }

    @Test
    void blankAndDuplicateSelectedRootsAreDropped() {
        List<String> roots = ConnectorDiscoveryService.resolveRoots(
                Optional.of(RootSelection.of(List.of(" a ", "", "a", "b", "   "), "admin")),
                List.of(),
                new StubClient());

        assertThat(roots).containsExactly("a", "b");
    }

    @Test
    void anEmptyScopeReportsThePassIncompleteRatherThanComplete() {
        // The load-bearing assertion of this whole change. The reconciliation sweep deletes what an
        // authoritative enumeration did not mention, so a pass that walked nothing must never claim to be
        // complete: doing so would delete every document of the source the moment a selection was cleared.
        StubClient client = new StubClient();
        ConnectorDiscoveryService service = new ConnectorDiscoveryService(
                selected(client), () -> List.of(), new ConnectorBatchProperties());

        ConnectorDiscoveryService.ConnectorDiscovery discovery = service.discoverTallied();

        assertThat(discovery.nodes()).isEmpty();
        assertThat(discovery.outcome().complete()).isFalse();
        assertThat(discovery.outcome().reasonSummary()).contains("No root is selected");
    }

    @Test
    void resolvesRootsOncePerPassRatherThanOnceAtConstruction() {
        // What makes a scope change take effect without a restart, and what keeps a connector that resolves
        // its roots over the network off the startup path.
        StubClient client = new StubClient();
        AtomicReference<List<String>> scope = new AtomicReference<>(List.of(CONNECTOR_ROOT));

        ConnectorDiscoveryService service = new ConnectorDiscoveryService(
                selected(client), scope::get, new ConnectorBatchProperties());

        service.discoverTallied();
        assertThat(client.nodeLookups).isEqualTo(1);

        scope.set(List.of());
        assertThat(service.discoverTallied().outcome().complete()).isFalse();

        // The second pass read the new scope, so the supplier is consulted per pass and not cached.
        assertThat(client.nodeLookups).isEqualTo(1);
    }

    @Test
    void theSupplierIsNotCalledUntilAPassRuns() {
        // A supplier that threw at construction would reintroduce the boot loop this change removes.
        ConnectorDiscoveryService service = new ConnectorDiscoveryService(
                selected(new StubClient()),
                () -> {
                    throw new IllegalStateException("the source is unreachable");
                },
                new ConnectorBatchProperties());

        assertThatThrownBy(service::discoverTallied)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("unreachable");
    }

    private static SelectedConnector selected(ContentSourceClient client) {
        return selected(client, new DefaultScopeResolver());
    }

    private static SelectedConnector selected(ContentSourceClient client, ScopeResolver scopeResolver) {
        return new SelectedConnector("sample", "sample connector", "sample.jar", client, scopeResolver,
                Mockito.mock(org.hyland.contentlake.spi.TextExtractor.class), false);
    }

    /** Answers a single root and counts how often it was asked for a node. */
    private static class StubClient implements ContentSourceClient {

        private String root = CONNECTOR_ROOT;
        private List<String> roots;
        private int nodeLookups;

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
            return root;
        }

        @Override
        public List<String> getRootNodeIds() {
            return roots != null ? roots : ContentSourceClient.super.getRootNodeIds();
        }

        @Override
        public SourceNode getNode(String nodeId) {
            nodeLookups++;
            return new SourceNode(nodeId, "instance-1", "sample", nodeId, "/" + nodeId, null, null, true,
                    java.util.Set.of(), java.util.Set.of(), java.util.Map.of(), null);
        }

        @Override
        public List<SourceNode> getChildren(String containerId, int skip, int maxItems) {
            return List.of();
        }

        @Override
        public Resource downloadContent(String nodeId, String fileName) {
            throw new UnsupportedOperationException();
        }

        @Override
        public byte[] getContent(String nodeId) {
            throw new UnsupportedOperationException();
        }
    }
}
