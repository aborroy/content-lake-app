package org.hyland.contentlake.pluginhost.batch.controller;

import org.hyland.contentlake.connector.DefaultScopeResolver;
import org.hyland.contentlake.pluginhost.batch.config.ConnectorBatchProperties;
import org.hyland.contentlake.pluginhost.batch.config.SelectedConnector;
import org.hyland.contentlake.service.InMemoryRootSelectionStore;
import org.hyland.contentlake.service.RootSelection;
import org.hyland.contentlake.service.RootSelectionStore;
import org.hyland.contentlake.spi.ContentSourceClient;
import org.hyland.contentlake.spi.ScopeResolver;
import org.hyland.contentlake.spi.SourceNode;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.core.io.Resource;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The browse endpoint's contract with a folder picker.
 *
 * <p>Exercised directly rather than through MockMvc: what matters here is the paging arithmetic, the scope
 * annotation and the mapping of a connector's answers onto statuses, none of which involve the servlet
 * stack. Authentication is the host's default-deny rule and is covered by
 * {@code ConnectorBatchSecurityConfigTest}.</p>
 */
class BrowseControllerTest {

    private static final String ROOT = "root";

    @Test
    void namesTheRootsAndWhereTheyCameFrom() {
        FakeSource source = new FakeSource().folder(ROOT);

        BrowseController.BrowseRoots roots = (BrowseController.BrowseRoots)
                controller(source).roots().getBody();

        assertThat(roots.roots()).extracting(BrowseController.BrowseNode::nodeId).containsExactly(ROOT);
        assertThat(roots.resolvedFrom()).isEqualTo("connector");
        assertThat(roots.problems()).isEmpty();
    }

    @Test
    void saysWhenTheRootsCameFromASelectionRatherThanFromConfiguration() {
        // The screen has to be able to explain a tree rooted somewhere unexpected.
        FakeSource source = new FakeSource().folder(ROOT).folder("chosen");
        RootSelectionStore store = new InMemoryRootSelectionStore();
        store.save("sample:instance-1", RootSelection.of(List.of("chosen"), "admin"));

        BrowseController.BrowseRoots roots = (BrowseController.BrowseRoots)
                controller(source, store, new ConnectorBatchProperties()).roots().getBody();

        assertThat(roots.resolvedFrom()).isEqualTo("selection");
        assertThat(roots.roots()).extracting(BrowseController.BrowseNode::nodeId).containsExactly("chosen");
    }

    @Test
    void reportsAnUnreadableRootAlongsideTheOnesThatWorked() {
        // Failing the whole request would leave the operator with no tree at all because one configured root
        // is stale, which is precisely the state they are trying to fix.
        FakeSource source = new FakeSource().folder(ROOT);
        ConnectorBatchProperties props = new ConnectorBatchProperties();
        props.setRoots(new ArrayList<>(List.of(ROOT, "does-not-exist")));

        BrowseController.BrowseRoots roots = (BrowseController.BrowseRoots)
                controller(source, null, props).roots().getBody();

        assertThat(roots.roots()).extracting(BrowseController.BrowseNode::nodeId).containsExactly(ROOT);
        assertThat(roots.problems()).hasSize(1).first().asString().contains("does-not-exist");
    }

    @Test
    void answersWithAnEmptyTreeRatherThanAnErrorWhenNothingNamesARoot() {
        // A configuration state the screen must be able to show. The operator's next action is to choose one.
        FakeSource nameless = new FakeSource();
        nameless.namesNoRoot = true;

        ResponseEntity<?> response = controller(nameless).roots();

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        BrowseController.BrowseRoots roots = (BrowseController.BrowseRoots) response.getBody();
        assertThat(roots.resolvedFrom()).isEqualTo("none");
        assertThat(roots.roots()).isEmpty();
        assertThat(roots.problems()).isNotEmpty();
    }

    @Test
    void advancesTheCursorByThePageSizeAndNotByTheEntriesReturned() {
        // The SPI contract, and the one a UI gets wrong. A connector may drop entries it cannot represent, so
        // a short page is not exhaustion: advancing by the size received re-reads the dropped ones or shifts
        // every later window.
        FakeSource source = new FakeSource().folder(ROOT);
        source.shortPage = true;

        BrowseController.BrowsePage page = (BrowseController.BrowsePage)
                controller(source).children(ROOT, 0, 10).getBody();

        assertThat(page.nodes()).hasSize(3);
        assertThat(page.nextSkip()).isEqualTo(10);
        assertThat(page.endOfContainer()).isFalse();
    }

    @Test
    void marksEndOfContainerOnlyOnAnEmptyPage() {
        FakeSource source = new FakeSource().folder(ROOT);

        BrowseController.BrowsePage empty = (BrowseController.BrowsePage)
                controller(source).children(ROOT, 999, 10).getBody();

        assertThat(empty.nodes()).isEmpty();
        assertThat(empty.endOfContainer()).isTrue();
    }

    @Test
    void clampsAnAbsurdPageSizeRatherThanPassingItToTheConnector() {
        FakeSource source = new FakeSource().folder(ROOT);

        BrowseController.BrowsePage page = (BrowseController.BrowsePage)
                controller(source).children(ROOT, 0, 1_000_000).getBody();

        assertThat(page.maxItems()).isEqualTo(500);
        assertThat(source.lastMaxItems).isEqualTo(500);
    }

    @Test
    void treatsANegativeSkipAsZero() {
        FakeSource source = new FakeSource().folder(ROOT);

        BrowseController.BrowsePage page = (BrowseController.BrowsePage)
                controller(source).children(ROOT, -5, 10).getBody();

        assertThat(page.skip()).isZero();
    }

    @Test
    void returnsOutOfScopeEntriesAnnotatedRatherThanOmittingThem() {
        // Filtering would make a legal selection unreachable, because a resolver descends into a folder an
        // include pattern does not match so a matching descendant stays reachable. It would also show an empty
        // tree to an operator trying to understand an exclusion.
        FakeSource source = new FakeSource().folder(ROOT).child(ROOT, "keep.txt").child(ROOT, "skip.txt");
        ScopeResolver selective = new ScopeResolver() {
            @Override
            public boolean isInScope(SourceNode node) {
                return !"skip.txt".equals(node.nodeId());
            }

            @Override
            public boolean shouldTraverse(SourceNode node) {
                return true;
            }
        };

        BrowseController.BrowsePage page = (BrowseController.BrowsePage)
                controller(source, selective).children(ROOT, 0, 10).getBody();

        assertThat(page.nodes()).extracting(BrowseController.BrowseNode::nodeId)
                .containsExactly("keep.txt", "skip.txt");
        assertThat(page.nodes()).filteredOn(node -> node.nodeId().equals("skip.txt"))
                .first().extracting(BrowseController.BrowseNode::inScope).isEqualTo(false);
    }

    @Test
    void aNodeTheConnectorSaysIsAbsentIsA404() {
        ResponseEntity<?> response = controller(new FakeSource()).node("nowhere");

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
    }

    @Test
    void aConnectorThatThrowsIsABadGatewayNotAnInternalError() {
        // The host is working and the source is not, and a screen saying so is more use than one showing an
        // internal error. The connector's own message is the only thing that names what failed.
        FakeSource source = new FakeSource();
        source.unreadable = true;

        ResponseEntity<?> response = controller(source).node("anything");

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_GATEWAY);
        assertThat(((BrowseController.Problem) response.getBody()).message())
                .contains("the source is unreachable");
    }

    @Test
    void neverEchoesReadPrincipalsOrSourceProperties() {
        // This endpoint draws a tree. Access-control data and a source's internal metadata are neither its
        // business nor safe to hand to a browser.
        FakeSource source = new FakeSource().folder(ROOT);

        BrowseController.BrowseNode node = (BrowseController.BrowseNode) controller(source).node(ROOT).getBody();

        assertThat(node.toString())
                .doesNotContain("secret-principal")
                .doesNotContain("internal-property");
    }

    private static BrowseController controller(ContentSourceClient client) {
        return controller(client, new DefaultScopeResolver());
    }

    private static BrowseController controller(ContentSourceClient client, ScopeResolver scopeResolver) {
        return new BrowseController(selected(client, scopeResolver), new ConnectorBatchProperties(),
                provider(null));
    }

    private static BrowseController controller(ContentSourceClient client,
                                               RootSelectionStore store,
                                               ConnectorBatchProperties props) {
        return new BrowseController(selected(client, new DefaultScopeResolver()), props, provider(store));
    }

    private static SelectedConnector selected(ContentSourceClient client, ScopeResolver scopeResolver) {
        return new SelectedConnector("sample", "sample connector", "sample.jar", client, scopeResolver,
                Mockito.mock(org.hyland.contentlake.spi.TextExtractor.class), false);
    }

    @SuppressWarnings("unchecked")
    private static ObjectProvider<RootSelectionStore> provider(RootSelectionStore store) {
        ObjectProvider<RootSelectionStore> provider = Mockito.mock(ObjectProvider.class);
        Mockito.when(provider.getIfAvailable()).thenReturn(store);
        return provider;
    }

    /** A tree held in a map, which is all this controller needs from a connector. */
    private static final class FakeSource implements ContentSourceClient {

        private final Map<String, SourceNode> nodes = new LinkedHashMap<>();
        private final Map<String, List<String>> children = new LinkedHashMap<>();
        private boolean namesNoRoot;
        private boolean unreadable;
        private boolean shortPage;
        private int lastMaxItems;

        FakeSource folder(String id) {
            nodes.put(id, node(id, true));
            return this;
        }

        FakeSource child(String parent, String id) {
            nodes.put(id, node(id, false));
            children.computeIfAbsent(parent, key -> new ArrayList<>()).add(id);
            return this;
        }

        private static SourceNode node(String id, boolean folder) {
            return new SourceNode(id, "instance-1", "sample", id, "/" + id,
                    folder ? null : "text/plain", null, folder,
                    Set.of("secret-principal"), Set.of(),
                    Map.of("internal-property", "value"), null);
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
        public List<String> getRootNodeIds() {
            return namesNoRoot ? List.of() : nodes.keySet().stream().limit(1).toList();
        }

        @Override
        public SourceNode getNode(String nodeId) {
            if (unreadable) {
                throw new IllegalStateException("the source is unreachable");
            }
            return nodes.get(nodeId);
        }

        @Override
        public List<SourceNode> getChildren(String containerId, int skip, int maxItems) {
            if (unreadable) {
                throw new IllegalStateException("the source is unreachable");
            }
            lastMaxItems = maxItems;
            if (shortPage) {
                // A full window that arrives short, which is what the SPI warns about.
                return skip == 0
                        ? List.of(node("a", false), node("b", false), node("c", false))
                        : List.of();
            }
            List<String> ids = children.getOrDefault(containerId, List.of());
            return ids.stream().skip(skip).limit(maxItems).map(nodes::get).toList();
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
