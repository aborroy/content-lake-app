package org.hyland.contentlake.service;

import lombok.extern.slf4j.Slf4j;
import org.hyland.contentlake.client.HxprService;
import org.hyland.contentlake.model.HxprDocument;
import org.hyland.contentlake.spi.ContentSourceClient;
import org.hyland.contentlake.spi.SourceTombstone;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Predicate;

/**
 * Deletes indexed documents that a completed discovery pass did not see in the source.
 *
 * <p>Deletion otherwise depends entirely on a delete event arriving. With the live ingester down, a
 * dropped broker message, or a node removed while only the batch ingester runs, the document outlives
 * its source indefinitely and search returns it as a phantom result, with nothing to notice.</p>
 *
 * <h3>Guards, and why each exists</h3>
 * <p>A sweep that gets its input wrong empties the index, so every precondition fails towards doing
 * nothing:</p>
 * <ul>
 *   <li><b>Discovery must report itself complete.</b> Several discovery paths return a partial result
 *       and log a warning rather than failing, so completeness is asserted by discovery, never
 *       inferred here. See {@link DiscoveryOutcome}.</li>
 *   <li><b>No node-level failures.</b> Any node that failed during ingestion may or may not have been
 *       enumerated, so the seen set is not trustworthy. No tolerance is configurable, because a
 *       partial-failure threshold is not a number an operator can calibrate.</li>
 *   <li><b>An empty discovery never deletes.</b> Zero nodes is exactly what a broken discovery
 *       returns.</li>
 *   <li><b>The seen set must not have overflowed</b>, or ids discovery did see would look missing.</li>
 *   <li><b>A maximum deletion ratio and an absolute cap</b>, as the backstop against a partial that
 *       was never reported.</li>
 *   <li><b>Scope.</b> Only documents under the roots discovery actually covered are candidates, so a
 *       folder-scoped sync cannot delete documents another sync owns.</li>
 * </ul>
 *
 * <p>Not a Spring component: core classes are hand-wired, so each batch ingester declares its own
 * bean.</p>
 */
@Slf4j
public class IndexReconciliationService {

    private final HxprService hxprService;
    private final NodeSyncService nodeSyncService;
    private final ContentSourceClient sourceClient;

    public IndexReconciliationService(HxprService hxprService,
                                      NodeSyncService nodeSyncService,
                                      ContentSourceClient sourceClient) {
        this.hxprService = hxprService;
        this.nodeSyncService = nodeSyncService;
        this.sourceClient = sourceClient;
    }

    /** Why a sweep did what it did. Every non-{@code COMPLETED} value means nothing was deleted. */
    public enum Status {
        /** Reconciliation is turned off. */
        DISABLED,
        /** Discovery reported that it did not enumerate its whole scope. */
        SKIPPED_INCOMPLETE_DISCOVERY,
        /** At least one node failed during ingestion, so the seen set may be missing ids. */
        SKIPPED_NODE_FAILURES,
        /** Discovery saw nothing, which is indistinguishable from discovery being broken. */
        SKIPPED_EMPTY_DISCOVERY,
        /**
         * The pass consumed a change feed instead of walking, so the feed owned its own deletions and a
         * sweep would have had no enumeration of the source to compare against. Distinct from
         * {@link #DISABLED}, which would read as a misconfiguration rather than as the designed
         * behaviour of an incremental pass.
         */
        SKIPPED_INCREMENTAL_RUN,
        /** The seen set hit its bound, so ids discovery saw would look missing. */
        ABORTED_SEEN_SET_OVERFLOW,
        /** The scan of the index failed partway, so the candidate set is incomplete. */
        ABORTED_SCAN_FAILED,
        /** The proportion of documents to delete exceeded the configured maximum. */
        ABORTED_RATIO,
        /** The number of documents to delete exceeded the absolute cap. */
        ABORTED_ABSOLUTE_CAP,
        /** The sweep ran. Deletions, if any, were applied. */
        COMPLETED
    }

    /**
     * What a sweep did, attached to the ingester's job so it is visible through the sync status API.
     *
     * @param status     the outcome; anything other than {@link Status#COMPLETED} means no deletions
     * @param indexed    documents scanned for this source
     * @param inScope    of those, how many fell under the roots discovery covered
     * @param candidates in-scope documents discovery did not see
     * @param deleted    documents actually removed
     * @param failed     candidates that could not be removed
     * @param ratio      {@code candidates / inScope}, the figure the ratio guard tested
     * @param detail     human-readable explanation, always populated for a non-completed status
     */
    public record Report(
            Status status,
            int indexed,
            int inScope,
            int candidates,
            int deleted,
            int failed,
            double ratio,
            String detail
    ) {
        static Report skipped(Status status, String detail) {
            return new Report(status, 0, 0, 0, 0, 0, 0.0, detail);
        }

        /**
         * The report an ingester attaches when its pass consumed a change feed rather than walking, so
         * the sweep was never asked to run.
         */
        public static Report incrementalRun(String detail) {
            return skipped(Status.SKIPPED_INCREMENTAL_RUN, detail);
        }
    }

    /**
     * Matches documents whose {@code cin_paths} sits at or under any of the given prefixes.
     *
     * <p>The prefixes are {@code cin_paths} values, not source paths: use
     * {@link NodeSyncService#contentLakePathPrefix} to convert. A document with no paths never
     * matches, so it is never deleted.</p>
     */
    public static Predicate<HxprDocument> underAnyPath(Collection<String> cinPathPrefixes) {
        List<String> prefixes = cinPathPrefixes == null ? List.of() : List.copyOf(cinPathPrefixes);
        return document -> {
            List<String> paths = document.getCinPaths();
            if (paths == null || paths.isEmpty() || prefixes.isEmpty()) {
                return false;
            }
            for (String path : paths) {
                if (path == null) {
                    continue;
                }
                for (String prefix : prefixes) {
                    if (path.equals(prefix) || path.startsWith(prefix.endsWith("/") ? prefix : prefix + "/")) {
                        return true;
                    }
                }
            }
            return false;
        };
    }

    /**
     * Deletes in-scope indexed documents that discovery did not see.
     *
     * @param seen        node ids discovery enumerated
     * @param outcome     discovery's own account of its completeness
     * @param nodeFailures per-node failures during this sync
     * @param inScope     which indexed documents this sweep owns
     * @param config      tunables
     * @return what the sweep did
     */
    public Report reconcile(SeenSet seen,
                            DiscoveryOutcome outcome,
                            int nodeFailures,
                            Predicate<HxprDocument> inScope,
                            ReconcileConfig config) {

        if (config == null || !config.enabled()) {
            return Report.skipped(Status.DISABLED, "Reconciliation is disabled");
        }
        if (outcome == null || !outcome.complete()) {
            String reasons = outcome == null ? "no discovery outcome reported" : outcome.reasonSummary();
            log.info("Reconciliation skipped: discovery did not complete ({}). Nothing was deleted.", reasons);
            return Report.skipped(Status.SKIPPED_INCOMPLETE_DISCOVERY,
                    "Discovery did not complete: " + reasons);
        }
        if (nodeFailures > 0) {
            log.info("Reconciliation skipped: {} node(s) failed during this sync, so the discovered set "
                    + "may be incomplete. Nothing was deleted; the next successful sync will reconcile.",
                    nodeFailures);
            return Report.skipped(Status.SKIPPED_NODE_FAILURES,
                    nodeFailures + " node(s) failed during this sync");
        }
        if (seen == null || seen.overflowed()) {
            log.warn("Reconciliation ABORTED: the discovered-id set exceeded "
                    + "max-seen-ids={}. NOTHING was deleted. Raise max-seen-ids for a corpus this "
                    + "large, or narrow the configured scope.", config.maxSeenIds());
            return Report.skipped(Status.ABORTED_SEEN_SET_OVERFLOW,
                    "Discovered-id set exceeded max-seen-ids=" + config.maxSeenIds());
        }
        if (seen.isEmpty()) {
            log.info("Reconciliation skipped: discovery saw no nodes, which is indistinguishable from a "
                    + "broken discovery. Nothing was deleted.");
            return Report.skipped(Status.SKIPPED_EMPTY_DISCOVERY, "Discovery saw no nodes");
        }

        String sourceId = qualifiedSourceId();
        List<HxprDocument> candidates = new ArrayList<>();
        int[] counters = new int[2]; // 0 = indexed, 1 = in scope

        try {
            hxprService.forEachDocumentOfSource(sourceId, config.pageSize(), document -> {
                counters[0]++;

                // A SysEmbeddings child carries no cin_sourceId so it cannot be matched, but deleting
                // one as if it were a parent would silently strip a document's vectors.
                String name = document.getSysName();
                if (name != null && name.startsWith("_e_")) {
                    return;
                }
                if (!inScope.test(document)) {
                    return;
                }
                counters[1]++;

                String nodeId = document.getCinId();
                if (nodeId != null && !nodeId.isBlank() && !seen.contains(nodeId)) {
                    candidates.add(document);
                }
            });
        } catch (Exception e) {
            log.warn("Reconciliation ABORTED: the index scan for {} failed ({}). NOTHING was deleted, "
                    + "because a partial scan cannot distinguish a missing document from an unscanned one.",
                    sourceId, e.getMessage());
            return Report.skipped(Status.ABORTED_SCAN_FAILED, "Index scan failed: " + e.getMessage());
        }

        int indexed = counters[0];
        int inScopeCount = counters[1];
        double ratio = inScopeCount == 0 ? 0.0 : (double) candidates.size() / inScopeCount;

        if (candidates.isEmpty()) {
            log.info("Reconciliation complete for {}: {} indexed, {} in scope, nothing to delete.",
                    sourceId, indexed, inScopeCount);
            return new Report(Status.COMPLETED, indexed, inScopeCount, 0, 0, 0, 0.0,
                    "Index matches the source");
        }

        if (candidates.size() > config.maxDeletes()) {
            log.warn("Reconciliation ABORTED for {}: {} of {} in-scope documents would be deleted, "
                    + "exceeding max-deletes={}. NOTHING was deleted. This is the expected result of a "
                    + "partial discovery, or of the first sweep on a corpus that has accumulated drift. "
                    + "Inspect the source, then raise the cap for one run if the deletions are genuine.",
                    sourceId, candidates.size(), inScopeCount, config.maxDeletes());
            return new Report(Status.ABORTED_ABSOLUTE_CAP, indexed, inScopeCount, candidates.size(),
                    0, 0, ratio,
                    candidates.size() + " deletions exceed max-deletes=" + config.maxDeletes());
        }

        if (ratio > config.maxDeleteRatio()) {
            log.warn("Reconciliation ABORTED for {}: {} of {} in-scope documents ({}) exceed "
                    + "max-delete-ratio={}. NOTHING was deleted. This is the expected result of a "
                    + "partial discovery, or of the first sweep on a corpus that has accumulated drift. "
                    + "Inspect the source, then raise the ratio for one run if the deletions are genuine.",
                    sourceId, candidates.size(), inScopeCount, String.format("%.3f", ratio),
                    config.maxDeleteRatio());
            return new Report(Status.ABORTED_RATIO, indexed, inScopeCount, candidates.size(),
                    0, 0, ratio,
                    String.format("Deletion ratio %.3f exceeds max-delete-ratio %.3f",
                            ratio, config.maxDeleteRatio()));
        }

        int deleted = 0;
        int failed = 0;
        for (HxprDocument document : candidates) {
            NodeSyncService.DeleteOutcome result =
                    nodeSyncService.delete(SourceTombstone.missingAtReconcile(document.getCinId()));
            switch (result) {
                case DELETED -> deleted++;
                // NOT_FOUND means a concurrent delete got there first, which is the desired end state.
                case NOT_FOUND -> deleted++;
                case SKIPPED_NEWER, FAILED -> failed++;
            }
        }

        log.info("Reconciliation complete for {}: {} indexed, {} in scope, {} not seen by discovery, "
                + "{} deleted, {} failed (ratio {}).",
                sourceId, indexed, inScopeCount, candidates.size(), deleted, failed,
                String.format("%.3f", ratio));

        return new Report(Status.COMPLETED, indexed, inScopeCount, candidates.size(), deleted, failed,
                ratio, deleted + " document(s) deleted");
    }

    private String qualifiedSourceId() {
        return qualifiedSourceId(sourceClient);
    }

    /**
     * The {@code "<sourceType>:<sourceId>"} string this sweep scans the index by.
     *
     * <p>Public because it is also the key a {@link SyncCursorStore} stores a source's position under: a
     * cursor and the sweep that would otherwise delete that source's documents have to name the source
     * identically, and a second implementation of the same rule would eventually disagree with this one.
     * An id that already carries a type prefix is returned as it is.</p>
     */
    public static String qualifiedSourceId(ContentSourceClient client) {
        String type = client.getSourceType();
        String id = client.getSourceId();
        if (id == null || id.isBlank() || type == null || type.isBlank() || id.contains(":")) {
            return id;
        }
        return type + ":" + id;
    }
}
