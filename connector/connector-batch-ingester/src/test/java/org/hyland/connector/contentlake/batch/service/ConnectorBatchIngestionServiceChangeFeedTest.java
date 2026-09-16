package org.hyland.connector.contentlake.batch.service;

import org.hyland.connector.contentlake.batch.config.ConnectorBatchProperties;
import org.hyland.connector.contentlake.batch.config.SelectedConnector;
import org.hyland.connector.contentlake.batch.model.IngestionJob;
import org.hyland.contentlake.service.DiscoveryOutcome;
import org.hyland.contentlake.service.InMemorySyncCursorStore;
import org.hyland.contentlake.service.IndexReconciliationService;
import org.hyland.contentlake.service.NodeSyncService;
import org.hyland.contentlake.service.SyncCursor;
import org.hyland.contentlake.service.SyncCursorStore;
import org.hyland.contentlake.spi.ContentSourceClient;
import org.hyland.contentlake.spi.ScopeResolver;
import org.hyland.contentlake.spi.SourceNode;
import org.hyland.contentlake.spi.SourceTombstone;
import org.hyland.contentlake.spi.TextExtractor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Which of the two mechanisms a job runs, and which of them owns that job's deletions (#144).
 *
 * <p>Every case here is about the branch rather than the ingestion: what makes a pass incremental, what
 * suspends the sweep, when a cursor is stored, and how a pass gets back to the authoritative walk when the
 * feed can no longer be trusted.</p>
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class ConnectorBatchIngestionServiceChangeFeedTest {

    private static final String ROOT_PATH = "/Sites/marketing";
    private static final String SOURCE = "sample:instance-1";

    @Mock
    private ConnectorDiscoveryService discoveryService;
    @Mock
    private NodeSyncService nodeSyncService;
    @Mock
    private IndexReconciliationService reconciliationService;
    @Mock
    private ContentSourceClient sourceClient;
    @Mock
    private ScopeResolver scopeResolver;
    @Mock
    private TextExtractor textExtractor;

    private ConnectorBatchProperties props;
    private SyncCursorStore cursorStore;
    private ConnectorBatchIngestionService service;

    /** Synchronous, so a test can assert on job state once the call returns. */
    private final Executor syncExecutor = Runnable::run;

    @BeforeEach
    void setUp() {
        props = new ConnectorBatchProperties();
        props.getChangeFeed().setEnabled(true);
        cursorStore = new InMemorySyncCursorStore();

        SelectedConnector connector = new SelectedConnector("sample", "sample connector", "sample.jar",
                sourceClient, scopeResolver, textExtractor, false);
        service = new ConnectorBatchIngestionService(discoveryService, nodeSyncService, syncExecutor,
                reconciliationService, cursorStore, connector, props);

        when(sourceClient.getSourceType()).thenReturn("sample");
        when(sourceClient.getSourceId()).thenReturn("instance-1");
        when(sourceClient.supportsChangeFeed()).thenReturn(true);
        when(sourceClient.initialCursor()).thenReturn("cursor-now");
        when(nodeSyncService.contentLakePathPrefix(anyString(), any())).thenReturn("/prefix" + ROOT_PATH);
        when(nodeSyncService.ingestMetadata(any())).thenReturn(new NodeSyncService.SyncResult(
                "hxpr-1", "node-1", "text/plain", "a.txt", ROOT_PATH, false, Map.of()));
        when(nodeSyncService.delete(any())).thenReturn(NodeSyncService.DeleteOutcome.DELETED);
        when(discoveryService.discoverTallied()).thenReturn(walkFound("node-1"));
    }

    @Nested
    class ChoosingTheMechanism {

        /** The feature is opt-in, so an untouched deployment walks exactly as it did before. */
        @Test
        void walksWhenTheFeatureIsOff() {
            props.getChangeFeed().setEnabled(false);
            cursorStore.save(SOURCE, SyncCursor.seeded("cursor-1"));

            service.startConfiguredSync();

            verify(discoveryService).discoverTallied();
            verify(discoveryService, never()).discoverIncremental(anyString(), anyInt(), anyInt());
        }

        /** Turning the feature on for a connector without a feed changes nothing about how it is ingested. */
        @Test
        void walksAConnectorThatDeclaresNoFeed() {
            when(sourceClient.supportsChangeFeed()).thenReturn(false);
            cursorStore.save(SOURCE, SyncCursor.seeded("cursor-1"));

            service.startConfiguredSync();

            verify(discoveryService).discoverTallied();
            verify(discoveryService, never()).discoverIncremental(anyString(), anyInt(), anyInt());
        }

        /**
         * The first pass has to walk: nothing has established that the index reflects the source, and a
         * feed opened now would never mention what already exists.
         */
        @Test
        void walksWhenNoCursorIsStoredYet() {
            service.startConfiguredSync();

            verify(discoveryService).discoverTallied();
            verify(discoveryService, never()).discoverIncremental(anyString(), anyInt(), anyInt());
        }

        @Test
        void readsTheFeedFromTheStoredCursor() {
            cursorStore.save(SOURCE, SyncCursor.seeded("cursor-1"));
            when(discoveryService.discoverIncremental(anyString(), anyInt(), anyInt()))
                    .thenReturn(feedReported(List.of(node("node-1")), List.of(), "cursor-2"));

            IngestionJob job = service.startConfiguredSync();

            verify(discoveryService).discoverIncremental("cursor-1", 200, 100);
            verify(discoveryService, never()).discoverTallied();
            assertThat(job.getStatus()).isEqualTo(IngestionJob.JobStatus.COMPLETED);
            assertThat(job.getSyncedCountValue()).isEqualTo(1);
        }

        @Test
        void passesTheConfiguredPageAndPageLimitThrough() {
            props.getChangeFeed().setPageSize(25);
            props.getChangeFeed().setMaxPages(3);
            cursorStore.save(SOURCE, SyncCursor.seeded("cursor-1"));
            when(discoveryService.discoverIncremental(anyString(), anyInt(), anyInt()))
                    .thenReturn(feedReported(List.of(), List.of(), "cursor-2"));

            service.startConfiguredSync();

            verify(discoveryService).discoverIncremental("cursor-1", 25, 3);
        }

        /**
         * The drift guard, and the only mechanism that catches a deletion the feed never reported: a walk
         * plus its sweep every Nth pass.
         */
        @Test
        void walksOnceTheCursorHasDrivenFullWalkEveryPasses() {
            props.getChangeFeed().setFullWalkEvery(3);
            cursorStore.save(SOURCE, new SyncCursor("cursor-1", OffsetDateTime.now(), 3L));

            service.startConfiguredSync();

            verify(discoveryService).discoverTallied();
            verify(discoveryService, never()).discoverIncremental(anyString(), anyInt(), anyInt());
        }

        @Test
        void staysIncrementalBelowTheFullWalkLimit() {
            props.getChangeFeed().setFullWalkEvery(3);
            cursorStore.save(SOURCE, new SyncCursor("cursor-1", OffsetDateTime.now(), 2L));
            when(discoveryService.discoverIncremental(anyString(), anyInt(), anyInt()))
                    .thenReturn(feedReported(List.of(), List.of(), "cursor-2"));

            service.startConfiguredSync();

            verify(discoveryService).discoverIncremental("cursor-1", 200, 100);
        }

        /** The default: an incremental schedule keeps running incrementally until told otherwise. */
        @Test
        void neverForcesAWalkWhenFullWalkEveryIsZero() {
            assertThat(props.getChangeFeed().getFullWalkEvery()).isZero();
            cursorStore.save(SOURCE, new SyncCursor("cursor-1", OffsetDateTime.now(), 900L));
            when(discoveryService.discoverIncremental(anyString(), anyInt(), anyInt()))
                    .thenReturn(feedReported(List.of(), List.of(), "cursor-2"));

            service.startConfiguredSync();

            verify(discoveryService).discoverIncremental("cursor-1", 200, 100);
        }
    }

    @Nested
    class Deletions {

        @Test
        void appliesWhatTheFeedReportedGone() {
            props.getReconcile().setEnabled(true);
            cursorStore.save(SOURCE, SyncCursor.seeded("cursor-1"));
            SourceTombstone tombstone = SourceTombstone.deleted("gone.txt");
            when(discoveryService.discoverIncremental(anyString(), anyInt(), anyInt()))
                    .thenReturn(feedReported(List.of(), List.of(tombstone), "cursor-2"));

            IngestionJob job = service.startConfiguredSync();

            verify(nodeSyncService).delete(tombstone);
            assertThat(job.getDeletedCountValue()).isEqualTo(1);
            assertThat(job.getFailedCountValue()).isZero();
        }

        /** Already absent is the desired end state, reached by someone else or by an earlier pass. */
        @Test
        void countsAnAlreadyAbsentDocumentAsDeleted() {
            cursorStore.save(SOURCE, SyncCursor.seeded("cursor-1"));
            when(nodeSyncService.delete(any())).thenReturn(NodeSyncService.DeleteOutcome.NOT_FOUND);
            when(discoveryService.discoverIncremental(anyString(), anyInt(), anyInt())).thenReturn(
                    feedReported(List.of(), List.of(SourceTombstone.deleted("gone.txt")), "cursor-2"));

            assertThat(service.startConfiguredSync().getDeletedCountValue()).isEqualTo(1);
        }

        /**
         * A failed delete counts as a node failure, which is what stops the cursor advancing over a window
         * the pass did not fully apply.
         */
        @Test
        void aFailedDeleteHoldsTheCursorWhereItWas() {
            cursorStore.save(SOURCE, SyncCursor.seeded("cursor-1"));
            when(nodeSyncService.delete(any())).thenReturn(NodeSyncService.DeleteOutcome.FAILED);
            when(discoveryService.discoverIncremental(anyString(), anyInt(), anyInt())).thenReturn(
                    feedReported(List.of(), List.of(SourceTombstone.deleted("gone.txt")), "cursor-2"));

            IngestionJob job = service.startConfiguredSync();

            assertThat(job.getFailedCountValue()).isEqualTo(1);
            assertThat(job.getDeletedCountValue()).isZero();
            assertThat(cursorStore.load(SOURCE)).map(SyncCursor::value).contains("cursor-1");
        }

        /** A delete that throws must not fail the whole job: the nodes already ingested still stand. */
        @Test
        void aDeleteThatThrowsIsCountedRatherThanFatal() {
            cursorStore.save(SOURCE, SyncCursor.seeded("cursor-1"));
            when(nodeSyncService.delete(any())).thenThrow(new RuntimeException("hxpr unavailable"));
            when(discoveryService.discoverIncremental(anyString(), anyInt(), anyInt())).thenReturn(
                    feedReported(List.of(), List.of(SourceTombstone.deleted("gone.txt")), "cursor-2"));

            IngestionJob job = service.startConfiguredSync();

            assertThat(job.getStatus()).isEqualTo(IngestionJob.JobStatus.COMPLETED);
            assertThat(job.getFailedCountValue()).isEqualTo(1);
        }
    }

    @Nested
    class TheSweep {

        /**
         * The single-owner rule. An incremental pass enumerated nothing, so there is nothing for the sweep
         * to compare the index against, and running it would delete every document the feed did not mention.
         */
        @Test
        void doesNotRunOnAnIncrementalPassEvenWhenEnabled() {
            props.getReconcile().setEnabled(true);
            cursorStore.save(SOURCE, SyncCursor.seeded("cursor-1"));
            when(discoveryService.discoverIncremental(anyString(), anyInt(), anyInt()))
                    .thenReturn(feedReported(List.of(node("node-1")), List.of(), "cursor-2"));

            service.startConfiguredSync();

            verify(reconciliationService, never()).reconcile(any(), any(), anyInt(), any(), any());
        }

        /**
         * Reported as its own status rather than as DISABLED, which an operator would read as a
         * misconfiguration instead of as the designed behaviour of an incremental pass.
         */
        @Test
        void saysWhyItDidNotRun() {
            props.getReconcile().setEnabled(true);
            cursorStore.save(SOURCE, SyncCursor.seeded("cursor-1"));
            when(discoveryService.discoverIncremental(anyString(), anyInt(), anyInt()))
                    .thenReturn(feedReported(List.of(node("node-1")), List.of(), "cursor-2"));

            IngestionJob job = service.startConfiguredSync();

            assertThat(job.getReconciliation().status())
                    .isEqualTo(IndexReconciliationService.Status.SKIPPED_INCREMENTAL_RUN);
            assertThat(job.getReconciliation().detail()).contains("change feed");
        }

        @Test
        void runsOnTheWalkThatFollowsAnExpiredCursor() {
            props.getReconcile().setEnabled(true);
            cursorStore.save(SOURCE, SyncCursor.seeded("cursor-1"));
            when(discoveryService.discoverIncremental(anyString(), anyInt(), anyInt()))
                    .thenReturn(ConnectorDiscoveryService.ConnectorChanges.expired("expired"));
            when(reconciliationService.reconcile(any(), any(), anyInt(), any(), any()))
                    .thenReturn(new IndexReconciliationService.Report(
                            IndexReconciliationService.Status.COMPLETED, 1, 1, 0, 0, 0, 0.0, "ok"));

            IngestionJob job = service.startConfiguredSync();

            verify(reconciliationService).reconcile(any(), any(), anyInt(), any(), any());
            assertThat(job.getReconciliation().status())
                    .isEqualTo(IndexReconciliationService.Status.COMPLETED);
        }
    }

    @Nested
    class Cursors {

        @Test
        void advancesToTheFurthestCursorTheFeedGave() {
            cursorStore.save(SOURCE, SyncCursor.seeded("cursor-1"));
            when(discoveryService.discoverIncremental(anyString(), anyInt(), anyInt()))
                    .thenReturn(feedReported(List.of(node("node-1")), List.of(), "cursor-2"));

            service.startConfiguredSync();

            assertThat(cursorStore.load(SOURCE)).hasValueSatisfying(cursor -> {
                assertThat(cursor.value()).isEqualTo("cursor-2");
                assertThat(cursor.generation()).isEqualTo(1L);
            });
        }

        /**
         * Mirrors the sweep's own node-failure guard: a pass that could not read some of what the feed named
         * must not claim to have consumed the window. Re-reading it costs nothing, since ingestion is
         * idempotent per node.
         */
        @Test
        void doesNotAdvanceAfterANodeFailure() {
            cursorStore.save(SOURCE, SyncCursor.seeded("cursor-1"));
            when(nodeSyncService.ingestMetadata(any())).thenThrow(new RuntimeException("read failed"));
            when(discoveryService.discoverIncremental(anyString(), anyInt(), anyInt()))
                    .thenReturn(feedReported(List.of(node("node-1")), List.of(), "cursor-2"));

            IngestionJob job = service.startConfiguredSync();

            assertThat(job.getFailedCountValue()).isEqualTo(1);
            assertThat(cursorStore.load(SOURCE)).map(SyncCursor::value).contains("cursor-1");
        }

        /**
         * A pass stopped by the page limit still saves what it reached. Discarding it would make every pass
         * re-read the same first pages and never catch up with a busy source.
         */
        @Test
        void keepsProgressWhenThePageLimitStoppedThePass() {
            cursorStore.save(SOURCE, SyncCursor.seeded("cursor-1"));
            when(discoveryService.discoverIncremental(anyString(), anyInt(), anyInt())).thenReturn(
                    new ConnectorDiscoveryService.ConnectorChanges(
                            List.of(node("node-1")), List.of(), "cursor-2", false, false,
                            DiscoveryOutcome.incomplete(List.of(), List.of("page limit"))));

            service.startConfiguredSync();

            assertThat(cursorStore.load(SOURCE)).map(SyncCursor::value).contains("cursor-2");
        }

        @Test
        void staysPutWhenTheFeedConsumedNothing() {
            cursorStore.save(SOURCE, SyncCursor.seeded("cursor-1"));
            when(discoveryService.discoverIncremental(anyString(), anyInt(), anyInt()))
                    .thenReturn(feedReported(List.of(), List.of(), null));

            service.startConfiguredSync();

            assertThat(cursorStore.load(SOURCE)).map(SyncCursor::value).contains("cursor-1");
        }

        /**
         * Read before the walk and saved after it, so a change made while the walk ran is replayed by the
         * next incremental pass rather than falling between the two mechanisms.
         */
        @Test
        void aSuccessfulWalkSeedsTheCursorWithThePositionItStartedFrom() {
            service.startConfiguredSync();

            assertThat(cursorStore.load(SOURCE)).hasValueSatisfying(cursor -> {
                assertThat(cursor.value()).isEqualTo("cursor-now");
                // Restarts the drift count: full-walk-every counts incremental passes since a walk.
                assertThat(cursor.generation()).isZero();
            });
        }

        @Test
        void aConnectorThatNamesNoPositionIsWalkedEveryPass() {
            when(sourceClient.initialCursor()).thenReturn(null);

            service.startConfiguredSync();

            assertThat(cursorStore.load(SOURCE)).isEmpty();
        }

        /** Same reasoning as the sweep's guards: an unfinished walk has not established anything to resume from. */
        @Test
        void anIncompleteWalkSeedsNothing() {
            when(discoveryService.discoverTallied()).thenReturn(
                    new ConnectorDiscoveryService.ConnectorDiscovery(
                            List.of(node("node-1")),
                            DiscoveryOutcome.incomplete(List.of(ROOT_PATH), List.of("a container failed"))));

            service.startConfiguredSync();

            assertThat(cursorStore.load(SOURCE)).isEmpty();
        }

        @Test
        void aWalkWithAFailedNodeSeedsNothing() {
            when(nodeSyncService.ingestMetadata(any())).thenThrow(new RuntimeException("read failed"));

            service.startConfiguredSync();

            assertThat(cursorStore.load(SOURCE)).isEmpty();
        }

        /** Expiry is the designed bridge back to the walk, so the stale cursor must not survive the job. */
        @Test
        void anExpiredCursorIsReplacedByTheFollowingWalksSeed() {
            cursorStore.save(SOURCE, SyncCursor.seeded("cursor-stale"));
            when(discoveryService.discoverIncremental(anyString(), anyInt(), anyInt()))
                    .thenReturn(ConnectorDiscoveryService.ConnectorChanges.expired("expired"));

            service.startConfiguredSync();

            verify(discoveryService).discoverTallied();
            assertThat(cursorStore.load(SOURCE)).map(SyncCursor::value).contains("cursor-now");
        }

        /** A cursor store that cannot be read is not a reason to skip the pass, only to walk it. */
        @Test
        void walksWhenTheCursorCannotBeRead() {
            service = new ConnectorBatchIngestionService(discoveryService, nodeSyncService, syncExecutor,
                    reconciliationService, new UnreadableCursorStore(),
                    new SelectedConnector("sample", "sample connector", "sample.jar",
                            sourceClient, scopeResolver, textExtractor, false),
                    props);

            IngestionJob job = service.startConfiguredSync();

            assertThat(job.getStatus()).isEqualTo(IngestionJob.JobStatus.COMPLETED);
            verify(discoveryService).discoverTallied();
        }
    }

    /** Stands in for hxpr being unreachable, or a state file that will not parse. */
    private static class UnreadableCursorStore implements SyncCursorStore {
        @Override
        public java.util.Optional<SyncCursor> load(String qualifiedSourceId) {
            throw new IllegalStateException("the cursor store is unreachable");
        }

        @Override
        public void save(String qualifiedSourceId, SyncCursor cursor) {
            throw new IllegalStateException("the cursor store is unreachable");
        }

        @Override
        public void clear(String qualifiedSourceId) {
            throw new IllegalStateException("the cursor store is unreachable");
        }
    }

    private static ConnectorDiscoveryService.ConnectorChanges feedReported(List<SourceNode> nodes,
                                                                          List<SourceTombstone> deletions,
                                                                          String nextCursor) {
        return new ConnectorDiscoveryService.ConnectorChanges(nodes, deletions, nextCursor, false, true,
                DiscoveryOutcome.complete(List.of()));
    }

    private static ConnectorDiscoveryService.ConnectorDiscovery walkFound(String... nodeIds) {
        return new ConnectorDiscoveryService.ConnectorDiscovery(
                List.of(nodeIds).stream().map(ConnectorBatchIngestionServiceChangeFeedTest::node).toList(),
                DiscoveryOutcome.complete(List.of(ROOT_PATH)));
    }

    private static SourceNode node(String nodeId) {
        return new SourceNode(nodeId, "instance-1", "sample", nodeId + ".txt", ROOT_PATH, "text/plain",
                OffsetDateTime.parse("2026-09-01T10:00:00Z"), false,
                Set.of("__Everyone__"), Set.of(), Map.of());
    }
}
