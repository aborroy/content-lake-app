package org.hyland.contentlake.pluginhost.batch.service;

import lombok.extern.slf4j.Slf4j;
import org.hyland.contentlake.pluginhost.batch.config.ConnectorBatchProperties;
import org.hyland.contentlake.pluginhost.batch.config.SelectedConnector;
import org.hyland.contentlake.pluginhost.batch.model.IngestionJob;
import org.hyland.contentlake.service.DiscoveryOutcome;
import org.hyland.contentlake.service.IndexReconciliationService;
import org.hyland.contentlake.service.NodeSyncService;
import org.hyland.contentlake.service.ReconcileConfig;
import org.hyland.contentlake.service.SeenSet;
import org.hyland.contentlake.service.SyncCursor;
import org.hyland.contentlake.service.SyncCursorStore;
import org.hyland.contentlake.spi.SourceNode;
import org.hyland.contentlake.spi.SourceTombstone;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * Runs and tracks batch jobs over the connector this host loaded.
 *
 * <p>Identical in shape to the other batch ingestion services: discover, sync each node through
 * {@code NodeSyncService}, then sweep. Everything source-specific it needs comes from
 * {@link SelectedConnector}.</p>
 *
 * <h3>Two ways to run one job, and one owner of deletions</h3>
 * <p>A job either walks the source or reads its change feed, never both at once, and whichever mechanism
 * ran owns that job's deletions:</p>
 * <ul>
 *   <li>A <strong>walk</strong> enumerates the source, so the reconciliation sweep can compare that
 *       enumeration against the index and delete what is missing. It infers deletions, which is why it is
 *       hedged with a completeness claim, a ratio guard and an absolute cap.</li>
 *   <li>An <strong>incremental pass</strong> enumerates nothing, so the sweep is suspended for that run and
 *       the feed's own tombstones are applied directly. A connector naming a node id is a first-hand
 *       statement about that node; the guards exist to catch a walk that came back suspiciously empty,
 *       which is a failure a feed cannot have.</li>
 * </ul>
 * <p>The cost of suspending the sweep is that a deletion the feed never reported is never noticed, so
 * {@code connector.change-feed.full-walk-every} forces a walk, sweep included, every Nth pass.</p>
 */
@Slf4j
public class ConnectorBatchIngestionService {

    private static final int MAX_RETAINED_JOBS = 100;

    private final ConnectorDiscoveryService discoveryService;
    private final NodeSyncService nodeSyncService;
    private final Executor batchExecutor;
    private final IndexReconciliationService reconciliationService;
    private final SyncCursorStore cursorStore;
    private final SelectedConnector connector;
    private final ConnectorBatchProperties props;
    private final Map<String, IngestionJob> jobsById = Collections.synchronizedMap(
            new LinkedHashMap<>(MAX_RETAINED_JOBS, 0.75f, false) {
                @Override
                protected boolean removeEldestEntry(Map.Entry<String, IngestionJob> eldest) {
                    return size() > MAX_RETAINED_JOBS;
                }
            });

    public ConnectorBatchIngestionService(ConnectorDiscoveryService discoveryService,
                                         NodeSyncService nodeSyncService,
                                         Executor batchExecutor,
                                         IndexReconciliationService reconciliationService,
                                         SyncCursorStore cursorStore,
                                         SelectedConnector connector,
                                         ConnectorBatchProperties props) {
        this.discoveryService = discoveryService;
        this.nodeSyncService = nodeSyncService;
        this.batchExecutor = batchExecutor;
        this.reconciliationService = reconciliationService;
        this.cursorStore = cursorStore;
        this.connector = connector;
        this.props = props;
    }

    public IngestionJob startConfiguredSync() {
        IngestionJob job = createJob();
        CompletableFuture.runAsync(() -> runJob(job), batchExecutor)
                .whenComplete((ignored, thrown) -> failIfEscaped(job, thrown));
        return job;
    }

    /**
     * The last line of defence for a job whose task did not finish it.
     *
     * <p>A future nobody observes captures whatever escaped its task and discards it, so a throwable that
     * missed the catch in {@link #runJob} left the job {@code RUNNING} with {@code completedAt} null, for ever,
     * and printed nothing at all. A caller polling for a terminal status waited for ever, and a stalled job was
     * indistinguishable from a slow one. This is the only place that sees such a throwable.</p>
     *
     * <p>{@code thrown} is non-null only when something escaped the task, so this cannot overwrite the status
     * of a job that finished. It can run twice for one throwable, because {@link #runJob} marks an {@code Error}
     * and rethrows it; {@link IngestionJob#fail()} is not idempotent, but both calls write the same terminal
     * status and the only difference is a {@code completedAt} microseconds later.</p>
     */
    private void failIfEscaped(IngestionJob job, Throwable thrown) {
        if (thrown == null) {
            return;
        }
        job.fail();
        log.error("Connector batch job {} ended on a throwable its task did not handle", job.getJobId(), thrown);
    }

    public IngestionJob getJob(String jobId) {
        return jobsById.get(jobId);
    }

    public Map<String, IngestionJob> getAllJobs() {
        synchronized (jobsById) {
            return Collections.unmodifiableMap(new LinkedHashMap<>(jobsById));
        }
    }

    private IngestionJob createJob() {
        String jobId = UUID.randomUUID().toString();
        IngestionJob job = new IngestionJob(jobId, connector.sourceType());
        jobsById.put(jobId, job);
        log.info("Starting connector batch job {} for '{}' (from {})",
                jobId, connector.sourceType(), connector.origin());
        return job;
    }

    private void runJob(IngestionJob job) {
        try {
            String source = IndexReconciliationService.qualifiedSourceId(connector.client());

            SyncCursor resumeFrom = cursorToResumeFrom(source);
            if (resumeFrom == null || !ingestFromFeed(job, source, resumeFrom)) {
                walk(job, source);
            }
        } catch (Throwable t) {
            // Throwable rather than Exception: one unreadable file threw something outside Exception from the
            // extraction chain, and the job stayed RUNNING for ever because nothing here caught it. Marking the
            // job at the point of failure is what keeps the source context in the log line; an Error is rethrown
            // afterwards, because swallowing one here would hide a JVM-level problem from the executor.
            job.fail();
            log.error("Connector batch job {} failed", job.getJobId(), t);
            if (t instanceof Error error) {
                throw error;
            }
        }
    }

    /**
     * The cursor this job should read the feed from, or {@code null} when it must walk instead.
     *
     * <p>Every condition fails towards walking, which is the authoritative mechanism: the feature has to be
     * on, the connector has to claim a feed, a cursor has to already be stored, and the drift counter has to
     * be below its limit.</p>
     */
    private SyncCursor cursorToResumeFrom(String source) {
        ConnectorBatchProperties.ChangeFeed feed = props.getChangeFeed();
        if (!feed.isEnabled() || !connector.client().supportsChangeFeed()) {
            return null;
        }

        SyncCursor stored;
        try {
            stored = cursorStore.load(source).orElse(null);
        } catch (Exception e) {
            log.warn("Could not read the stored change-feed cursor for {}; walking this source instead",
                    source, e);
            return null;
        }
        if (stored == null) {
            // Either the first pass for this source or a cursor the last pass cleared. Both mean the index
            // cannot be assumed to reflect the source, so the walk has to establish that first.
            log.info("No change-feed cursor stored for {}; this pass walks the source and seeds one", source);
            return null;
        }
        if (feed.getFullWalkEvery() > 0 && stored.generation() >= feed.getFullWalkEvery()) {
            log.info("Change-feed cursor for {} has driven {} incremental pass(es), reaching "
                    + "full-walk-every={}; this pass walks and sweeps instead",
                    source, stored.generation(), feed.getFullWalkEvery());
            return null;
        }
        return stored;
    }

    /**
     * Reads the connector's change feed and applies what it reported.
     *
     * @return {@code true} when the feed stood for this job, {@code false} when the caller must walk
     *         because the cursor had expired
     */
    private boolean ingestFromFeed(IngestionJob job, String source, SyncCursor resumeFrom) {
        ConnectorBatchProperties.ChangeFeed feed = props.getChangeFeed();
        ConnectorDiscoveryService.ConnectorChanges changes = discoveryService.discoverIncremental(
                resumeFrom.value(), feed.getPageSize(), feed.getMaxPages());

        if (changes.cursorExpired()) {
            // Not an error, and the designed bridge back to the authoritative mechanism: forget the cursor
            // and let this same job walk, with the sweep as configured.
            log.warn("Change-feed cursor for {} expired at the source; it has been cleared and this job "
                    + "falls back to a full walk with the reconciliation sweep as configured", source);
            clearCursor(source);
            return false;
        }

        changes.nodes().forEach(node -> syncNode(node, job, null));
        changes.deletions().forEach(tombstone -> applyTombstone(tombstone, job));

        job.complete();
        completionLog(job, "the change feed");

        job.recordReconciliation(IndexReconciliationService.Report.incrementalRun(
                "This pass read the connector's change feed, which reported its own deletions, so there was "
                        + "no enumeration of the source for a sweep to compare the index against"));

        saveCursor(job, source, resumeFrom, changes);
        return true;
    }

    /** The pass that enumerates the source, and the only one a reconciliation sweep can follow. */
    private void walk(IngestionJob job, String source) {
        // Read before the walk starts and saved after it finishes, so a change made while it ran is
        // replayed by the next incremental pass rather than falling between the two mechanisms.
        String positionBeforeWalk = feedPositionBeforeWalk();

        ReconcileConfig config = props.getReconcile().toConfig();
        SeenSet seen = new SeenSet(config.maxSeenIds());

        ConnectorDiscoveryService.ConnectorDiscovery discovered = discoveryService.discoverTallied();
        discovered.nodes().forEach(node -> syncNode(node, job, seen));

        job.complete();
        completionLog(job, "a full walk");

        sweep(job, discovered.outcome(), seen, config);
        seedCursor(job, source, positionBeforeWalk, discovered.outcome());
    }

    private void completionLog(IngestionJob job, String mode) {
        log.info("Connector sync job {} completed via {}. Discovered: {}, Synced: {}, Skipped: {}, "
                + "Deleted: {}, Failed: {}",
                job.getJobId(), mode,
                job.getDiscoveredCountValue(),
                job.getSyncedCountValue(),
                job.getSkippedCountValue(),
                job.getDeletedCountValue(),
                job.getFailedCountValue());
    }

    /**
     * The feed position to seed a cursor with after this walk, or {@code null} when there is none to take.
     *
     * <p>A connector that answers {@code null} is walked on every pass, which is the behaviour of a source
     * with no feed at all.</p>
     */
    private String feedPositionBeforeWalk() {
        if (!props.getChangeFeed().isEnabled() || !connector.client().supportsChangeFeed()) {
            return null;
        }
        try {
            String position = connector.client().initialCursor();
            return position == null || position.isBlank() ? null : position;
        } catch (Exception e) {
            log.warn("Connector '{}' could not name its current change-feed position; this walk seeds no "
                    + "cursor and the next pass walks again", connector.sourceType(), e);
            return null;
        }
    }

    /**
     * Stores the position read before a successful walk, so the next pass can go incremental.
     *
     * <p>Generation restarts at zero: {@code full-walk-every} counts incremental passes since the last
     * walk, and this walk is that one.</p>
     */
    private void seedCursor(IngestionJob job, String source, String position, DiscoveryOutcome outcome) {
        if (position == null) {
            return;
        }
        if (!outcome.complete() || job.getFailedCountValue() > 0) {
            // The same reasoning as the sweep's own guards: a walk that did not cover its scope, or that
            // could not read some nodes, has not established that the index reflects the source, so a
            // cursor taken from it would make the gap permanent.
            log.info("Walk of {} did not complete cleanly, so no change-feed cursor was seeded; the next "
                    + "pass walks again", source);
            return;
        }
        try {
            cursorStore.save(source, SyncCursor.seeded(position));
            log.info("Seeded the change-feed cursor for {} from the position read before this walk", source);
        } catch (Exception e) {
            log.warn("Could not store the change-feed cursor for {}; the next pass walks again", source, e);
        }
    }

    /**
     * Advances the stored cursor after an incremental pass.
     *
     * <p>Only after a pass with no node failures, mirroring the sweep's own {@code SKIPPED_NODE_FAILURES}
     * guard: a pass that could not read some of what the feed named must not claim to have consumed the
     * window. Re-reading a window costs nothing correctness-wise, because ingestion is idempotent per node
     * and a delete of an already-absent document reports {@code NOT_FOUND} rather than failing.</p>
     *
     * <p>A pass stopped by {@code max-pages} still saves, and must: the cursor it reached is a real position
     * in the feed, and discarding it would make every pass re-read the same first pages and never catch
     * up.</p>
     */
    private void saveCursor(IngestionJob job,
                            String source,
                            SyncCursor resumeFrom,
                            ConnectorDiscoveryService.ConnectorChanges changes) {
        if (job.getFailedCountValue() > 0) {
            log.warn("{} node(s) failed during the incremental pass for {}, so the cursor stays where it "
                    + "was and the next pass re-reads the same window", job.getFailedCountValue(), source);
            return;
        }
        if (changes.nextCursor() == null) {
            // Nothing was consumed: the feed reported no position beyond the one we started from.
            return;
        }
        try {
            cursorStore.save(source, resumeFrom.advancedTo(changes.nextCursor()));
        } catch (Exception e) {
            log.warn("Could not store the advanced change-feed cursor for {}; the next pass re-reads the "
                    + "same window", source, e);
        }
    }

    private void clearCursor(String source) {
        try {
            cursorStore.clear(source);
        } catch (Exception e) {
            // The walk still runs, and it is the authoritative pass; the stale cursor is retried next time.
            log.warn("Could not clear the expired change-feed cursor for {}", source, e);
        }
    }

    /**
     * Removes a document the feed reported gone, or reported changed into something out of scope.
     *
     * <p>No ratio guard, no absolute cap and no seen set: those exist to second-guess deletions a walk
     * <em>inferred</em> from an enumeration that may have been partial, and a tombstone naming one node id
     * is not an inference.</p>
     */
    private void applyTombstone(SourceTombstone tombstone, IngestionJob job) {
        try {
            switch (nodeSyncService.delete(tombstone)) {
                // NOT_FOUND is the desired end state, reached by someone else or by an earlier pass.
                case DELETED, NOT_FOUND -> job.incrementDeleted();
                case SKIPPED_NEWER -> log.info("Kept indexed document for {} node {}: it is newer than the "
                        + "change the feed reported", connector.sourceType(), tombstone.nodeId());
                case FAILED -> {
                    job.incrementFailed();
                    log.error("Failed to delete {} node {} reported by the change feed",
                            connector.sourceType(), tombstone.nodeId());
                }
            }
        } catch (Exception e) {
            job.incrementFailed();
            log.error("Failed to delete {} node {} reported by the change feed",
                    connector.sourceType(), tombstone.nodeId(), e);
        }
    }

    /**
     * Deletes indexed documents this sync's discovery did not see.
     *
     * <p>Runs after {@code job.complete()}: the sweep is a distinct optional phase and a sweep failure
     * must not turn a successful ingestion into a failed job.</p>
     */
    private void sweep(IngestionJob job, DiscoveryOutcome outcome, SeenSet seen, ReconcileConfig config) {
        if (!config.enabled()) {
            return;
        }
        try {
            List<String> prefixes = outcome.resolvedRootPaths().stream()
                    .map(path -> nodeSyncService.contentLakePathPrefix(
                            connector.client().getSourceId(), path))
                    .toList();

            if (prefixes.isEmpty()) {
                // No resolved scope means the predicate would match nothing, which reconcile() would
                // then report as "index matches the source" -- a silent no-op dressed as success.
                log.warn("Reconciliation skipped for job {}: no scope path could be resolved from "
                        + "discovery, so the sweep has nothing it can safely own.", job.getJobId());
                return;
            }

            job.recordReconciliation(reconciliationService.reconcile(
                    seen, outcome, job.getFailedCountValue(),
                    IndexReconciliationService.underAnyPath(prefixes), config));
        } catch (Throwable t) {
            // Throwable, and deliberately not rethrown, so the invariant above survives the widening of
            // runJob's catch: the job is already COMPLETED with correct ingestion counters, and letting a sweep
            // throwable reach runJob would flip it to FAILED. Logged rather than swallowed, which is the whole
            // point -- nothing here may end without a log line.
            log.error("Reconciliation sweep for job {} failed; ingestion is unaffected", job.getJobId(), t);
        }
    }

    /**
     * @param seen the walk's enumeration, or {@code null} on an incremental pass, where there is no sweep to
     *             inform and a feed page is not an enumeration of anything
     */
    private void syncNode(SourceNode node, IngestionJob job, SeenSet seen) {
        job.incrementDiscovered();
        if (seen != null) {
            // Recorded before the sync attempt: the set means "the source has this node", which holds
            // whether or not syncing it succeeded. A failed node blocks the sweep separately.
            seen.add(node.nodeId());
        }
        try {
            NodeSyncService.SyncResult metadata = nodeSyncService.ingestMetadata(node);
            if (metadata.skipped()) {
                job.incrementSkipped();
                return;
            }
            nodeSyncService.processContent(
                    metadata.hxprDocId(),
                    metadata.ingestProperties(),
                    metadata.nodeId(),
                    metadata.mimeType(),
                    metadata.documentName(),
                    metadata.documentPath());
            job.incrementSynced();
        } catch (Exception e) {
            job.incrementFailed();
            log.error("Failed to sync {} node {}", connector.sourceType(), node.nodeId(), e);
        }
    }
}
