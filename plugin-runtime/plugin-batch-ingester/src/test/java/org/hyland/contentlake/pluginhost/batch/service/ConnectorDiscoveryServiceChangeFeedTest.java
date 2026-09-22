package org.hyland.contentlake.pluginhost.batch.service;

import org.hyland.contentlake.pluginhost.batch.config.ConnectorBatchProperties;
import org.hyland.contentlake.pluginhost.batch.config.SelectedConnector;
import org.hyland.contentlake.connector.DefaultScopeResolver;
import org.hyland.contentlake.spi.ContentSourceClient;
import org.hyland.contentlake.spi.ScopeResolver;
import org.hyland.contentlake.spi.SourceChangePage;
import org.hyland.contentlake.spi.SourceNode;
import org.hyland.contentlake.spi.SourceTombstone;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.core.io.Resource;

import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Reading a connector's change feed instead of walking it (#144).
 *
 * <p>No connector in the tree implements a feed, so every case here drives a scripted one. The cases that
 * matter are the ones where the feed is not simply cooperative: it expires the cursor, it throws partway, it
 * offers more pages without saying where to resume, or it reports a document that has left scope.</p>
 */
class ConnectorDiscoveryServiceChangeFeedTest {

    private static final OffsetDateTime MODIFIED = OffsetDateTime.parse("2026-09-16T10:00:00Z");

    /** A scripted feed, so a test states what the source reports rather than stubbing call by call. */
    private static class FakeFeed implements ContentSourceClient {

        // LinkedList rather than ArrayDeque: one case queues a null page, which ArrayDeque rejects.
        private final Deque<Object> pages = new LinkedList<>();
        private final List<String> cursorsRequested = new ArrayList<>();
        private final List<Integer> sizesRequested = new ArrayList<>();

        /** Queues a page. An entry may also be a {@link RuntimeException} to be thrown in its turn. */
        FakeFeed page(Object page) {
            pages.add(page);
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
        public boolean supportsChangeFeed() {
            return true;
        }

        @Override
        public SourceChangePage changesSince(String cursor, int maxItems) {
            cursorsRequested.add(cursor);
            sizesRequested.add(maxItems);
            Object next = pages.poll();
            if (next instanceof RuntimeException e) {
                throw e;
            }
            return (SourceChangePage) next;
        }

        @Override
        public SourceNode getNode(String nodeId) {
            return null;
        }

        @Override
        public List<SourceNode> getChildren(String containerId, int skip, int maxItems) {
            return List.of();
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
    void collectsWhatOnePageReported() {
        FakeFeed feed = new FakeFeed().page(SourceChangePage.of(
                List.of(document("a.txt"), document("b.txt")),
                List.of(deleted("gone.txt")),
                "cursor-2", false));

        ConnectorDiscoveryService.ConnectorChanges changes = read(feed, "cursor-1");

        assertThat(changes.nodes()).extracting(SourceNode::nodeId).containsExactly("a.txt", "b.txt");
        assertThat(changes.deletions()).extracting(SourceTombstone::nodeId).containsExactly("gone.txt");
        assertThat(changes.nextCursor()).isEqualTo("cursor-2");
        assertThat(changes.cursorExpired()).isFalse();
        assertThat(changes.drained()).isTrue();
        assertThat(changes.outcome().complete()).isTrue();
        assertThat(feed.cursorsRequested).containsExactly("cursor-1");
    }

    /** A drained feed enumerated no scope, which is safe only because no sweep runs on such a pass. */
    @Test
    void claimsNoScopeEvenWhenTheFeedDrained() {
        FakeFeed feed = new FakeFeed().page(SourceChangePage.of(
                List.of(document("a.txt")), List.of(), "cursor-2", false));

        assertThat(read(feed, "cursor-1").outcome().resolvedRootPaths()).isEmpty();
    }

    @Test
    void resumesEachPageFromThePreviousCursor() {
        FakeFeed feed = new FakeFeed()
                .page(SourceChangePage.of(List.of(document("a.txt")), List.of(), "cursor-2", true))
                .page(SourceChangePage.of(List.of(document("b.txt")), List.of(), "cursor-3", true))
                .page(SourceChangePage.of(List.of(document("c.txt")), List.of(), "cursor-4", false));

        ConnectorDiscoveryService.ConnectorChanges changes = read(feed, "cursor-1");

        assertThat(changes.nodes()).extracting(SourceNode::nodeId)
                .containsExactly("a.txt", "b.txt", "c.txt");
        assertThat(feed.cursorsRequested).containsExactly("cursor-1", "cursor-2", "cursor-3");
        assertThat(changes.nextCursor()).isEqualTo("cursor-4");
        assertThat(changes.drained()).isTrue();
    }

    /**
     * Stopping at the page limit is not a failure, but it is not a drained feed either: the cursor reached
     * has to come back so the next pass resumes rather than re-reading the same pages forever.
     */
    @Test
    void stopsAtThePageLimitAndKeepsTheCursorItReached() {
        FakeFeed feed = new FakeFeed()
                .page(SourceChangePage.of(List.of(document("a.txt")), List.of(), "cursor-2", true))
                .page(SourceChangePage.of(List.of(document("b.txt")), List.of(), "cursor-3", true))
                .page(SourceChangePage.of(List.of(document("c.txt")), List.of(), "cursor-4", true));

        ConnectorDiscoveryService.ConnectorChanges changes = read(feed, "cursor-1", 200, 2);

        assertThat(changes.nodes()).extracting(SourceNode::nodeId).containsExactly("a.txt", "b.txt");
        assertThat(changes.nextCursor()).isEqualTo("cursor-3");
        assertThat(changes.drained()).isFalse();
        assertThat(changes.outcome().complete()).isFalse();
        assertThat(changes.outcome().reasonSummary()).contains("2-page limit");
    }

    /**
     * Expiry discards everything, including pages read before it was noticed. Keeping them would leave the
     * caller with a partial window it has no cursor to finish, which is exactly the state the fallback walk
     * exists to get out of.
     */
    @Test
    void anExpiredCursorCarriesNothingAndStopsTheFeed() {
        FakeFeed feed = new FakeFeed()
                .page(SourceChangePage.of(List.of(document("a.txt")), List.of(), "cursor-2", true))
                .page(SourceChangePage.expired())
                .page(SourceChangePage.of(List.of(document("never-read.txt")), List.of(), "cursor-9", false));

        ConnectorDiscoveryService.ConnectorChanges changes = read(feed, "cursor-1");

        assertThat(changes.cursorExpired()).isTrue();
        assertThat(changes.nodes()).isEmpty();
        assertThat(changes.deletions()).isEmpty();
        assertThat(changes.nextCursor()).isNull();
        assertThat(changes.drained()).isFalse();
        assertThat(changes.outcome().complete()).isFalse();
        assertThat(feed.cursorsRequested).containsExactly("cursor-1", "cursor-2");
    }

    /**
     * Contained like a failed container listing: what earlier pages reported is already true of those nodes,
     * so it is kept, and the pass says it did not finish.
     */
    @Test
    void aFeedThatThrowsKeepsWhatEarlierPagesReported() {
        FakeFeed feed = new FakeFeed()
                .page(SourceChangePage.of(List.of(document("a.txt")), List.of(), "cursor-2", true))
                .page(new IllegalStateException("the feed endpoint is down"));

        ConnectorDiscoveryService.ConnectorChanges changes = read(feed, "cursor-1");

        assertThat(changes.nodes()).extracting(SourceNode::nodeId).containsExactly("a.txt");
        assertThat(changes.nextCursor()).isEqualTo("cursor-2");
        assertThat(changes.drained()).isFalse();
        assertThat(changes.outcome().reasonSummary()).contains("the feed endpoint is down");
    }

    @Test
    void aFeedThatReturnsNoPageIsAnIncompletePass() {
        FakeFeed feed = new FakeFeed().page(null);

        ConnectorDiscoveryService.ConnectorChanges changes = read(feed, "cursor-1");

        assertThat(changes.nodes()).isEmpty();
        assertThat(changes.drained()).isFalse();
        assertThat(changes.outcome().reasonSummary()).contains("no page");
    }

    /**
     * More pages with nowhere to resume from would re-read the same window on every iteration, so the pass
     * stops and refuses to claim the feed was drained.
     */
    @Test
    void refusesToSpinOnAPageThatOffersMoreWithoutACursor() {
        FakeFeed feed = new FakeFeed()
                .page(SourceChangePage.of(List.of(document("a.txt")), List.of(), null, true))
                .page(SourceChangePage.of(List.of(document("never-read.txt")), List.of(), "cursor-9", false));

        ConnectorDiscoveryService.ConnectorChanges changes = read(feed, "cursor-1");

        assertThat(changes.nodes()).extracting(SourceNode::nodeId).containsExactly("a.txt");
        assertThat(changes.nextCursor()).isNull();
        assertThat(changes.outcome().reasonSummary()).contains("no cursor to resume from");
        assertThat(feed.cursorsRequested).containsExactly("cursor-1");
    }

    /** Matching the walk: a container has no content to extract, so it is dropped rather than ingested. */
    @Test
    void dropsContainersTheFeedReported() {
        FakeFeed feed = new FakeFeed().page(SourceChangePage.of(
                List.of(container("a-folder"), document("a.txt")), List.of(), "cursor-2", false));

        assertThat(read(feed, "cursor-1").nodes()).extracting(SourceNode::nodeId).containsExactly("a.txt");
    }

    /**
     * The reason an out-of-scope change is a deletion rather than something to ignore: from the index's
     * side a node leaving scope and a node being removed are the same thing, and with the sweep suspended
     * the feed is the only mechanism that can notice either.
     */
    @Test
    void aChangedDocumentThatLeftScopeBecomesADeletion() {
        FakeFeed feed = new FakeFeed().page(SourceChangePage.of(
                List.of(document("keep.txt"), document("private.txt")), List.of(), "cursor-2", false));

        ScopeResolver selective = new ScopeResolver() {
            @Override
            public boolean isInScope(SourceNode node) {
                return !"private.txt".equals(node.nodeId());
            }

            @Override
            public boolean shouldTraverse(SourceNode node) {
                return true;
            }
        };

        ConnectorDiscoveryService.ConnectorChanges changes =
                new ConnectorDiscoveryService(selected(feed, selective), () -> List.of("root"),
                        new ConnectorBatchProperties())
                        .discoverIncremental("cursor-1", 200, 100);

        assertThat(changes.nodes()).extracting(SourceNode::nodeId).containsExactly("keep.txt");
        assertThat(changes.deletions())
                .containsExactly(new SourceTombstone("private.txt", MODIFIED,
                        SourceTombstone.Reason.OUT_OF_SCOPE));
    }

    @Test
    void ignoresEntriesTheFeedCouldNotName() {
        FakeFeed feed = new FakeFeed().page(new SourceChangePage(
                List.of(document(null), document("a.txt")),
                List.of(deleted("  "), deleted("gone.txt")),
                "cursor-2", false, false));

        ConnectorDiscoveryService.ConnectorChanges changes = read(feed, "cursor-1");

        assertThat(changes.nodes()).extracting(SourceNode::nodeId).containsExactly("a.txt");
        assertThat(changes.deletions()).extracting(SourceTombstone::nodeId).containsExactly("gone.txt");
    }

    /** An empty page still ends the pass, and is the normal answer for a source with nothing new. */
    @Test
    void anEmptyPageDrainsTheFeed() {
        FakeFeed feed = new FakeFeed().page(SourceChangePage.empty("cursor-1"));

        ConnectorDiscoveryService.ConnectorChanges changes = read(feed, "cursor-1");

        assertThat(changes.nodes()).isEmpty();
        assertThat(changes.deletions()).isEmpty();
        assertThat(changes.drained()).isTrue();
        assertThat(changes.outcome().complete()).isTrue();
        assertThat(changes.nextCursor()).isEqualTo("cursor-1");
    }

    @Test
    void asksForAtLeastOneChangeAndOnePageHoweverItIsConfigured() {
        FakeFeed feed = new FakeFeed().page(SourceChangePage.of(List.of(), List.of(), "cursor-2", false));

        read(feed, "cursor-1", 0, 0);

        assertThat(feed.sizesRequested).containsExactly(1);
    }

    private static ConnectorDiscoveryService.ConnectorChanges read(ContentSourceClient client, String cursor) {
        return read(client, cursor, 200, 100);
    }

    private static ConnectorDiscoveryService.ConnectorChanges read(ContentSourceClient client,
                                                                  String cursor,
                                                                  int pageSize,
                                                                  int maxPages) {
        return new ConnectorDiscoveryService(selected(client, new DefaultScopeResolver()), () -> List.of("root"),
                new ConnectorBatchProperties())
                .discoverIncremental(cursor, pageSize, maxPages);
    }

    private static SelectedConnector selected(ContentSourceClient client, ScopeResolver scopeResolver) {
        return new SelectedConnector("sample", "sample connector", "sample.jar", client, scopeResolver,
                Mockito.mock(org.hyland.contentlake.spi.TextExtractor.class), false);
    }

    private static SourceTombstone deleted(String nodeId) {
        return new SourceTombstone(nodeId, MODIFIED, SourceTombstone.Reason.DELETED);
    }

    private static SourceNode document(String nodeId) {
        return node(nodeId, false);
    }

    private static SourceNode container(String nodeId) {
        return node(nodeId, true);
    }

    private static SourceNode node(String nodeId, boolean folder) {
        return new SourceNode(nodeId, "instance-1", "sample", nodeId, "/" + nodeId,
                folder ? null : "text/plain", MODIFIED, folder, Set.of("__Everyone__"), Set.of(), Map.of());
    }
}
