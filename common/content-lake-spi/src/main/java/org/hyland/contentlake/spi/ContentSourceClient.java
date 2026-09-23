package org.hyland.contentlake.spi;

import org.springframework.core.io.Resource;

import java.util.List;

/**
 * Contract for reading content and metadata from a source system.
 *
 * <p>Implementations are source-specific (Alfresco, Nuxeo, …) and live in
 * their respective adapter modules. The shared sync pipeline depends only on
 * this interface.</p>
 */
public interface ContentSourceClient {

    /** Unique identifier for this source system instance (e.g. repository UUID). */
    String getSourceId();

    /** Short source type label stored as the prefix of {@code cin_sourceId} (e.g. {@code "alfresco"}). */
    String getSourceType();

    /**
     * The configuration this connector needs, so a deployment can be checked before it runs and
     * operator tooling can describe a source without hardcoding a form for it.
     *
     * <p>Defaults to an empty schema, which declares nothing and validates trivially, so an adapter
     * written before this is unaffected. An implementation should describe its own connection and scope
     * settings only; see {@link ConnectorSchema} for what is deliberately out of scope.</p>
     */
    default ConnectorSchema connectorSchema() {
        return ConnectorSchema.empty(getSourceType());
    }

    /**
     * The container a batch discovery pass starts from, or {@code null} when the connector does not know.
     *
     * <p>Every other method here answers a question about a node the caller already has; this is the one
     * that says where to begin. A host driving a connector it was not compiled against has no other way to
     * find out, and the alternative -- requiring the entry point in configuration -- makes a connector that
     * has exactly one possible root (a mounted directory, a single repository) impossible to configure
     * wrongly only by luck.</p>
     *
     * <p>Defaults to {@code null}, so an adapter written before this is unaffected and a connector with
     * several equally valid roots, or none it can name, simply does not answer. A host is expected to take
     * configured roots in preference to this and to refuse to start when it has neither.</p>
     */
    default String getRootNodeId() {
        return null;
    }

    /**
     * Every container a batch discovery pass should start from, or empty when the connector does not know.
     *
     * <p>This exists because {@link #getRootNodeId()} can only answer for a connector with exactly one root.
     * A source with several equally valid entry points -- two SharePoint document libraries, a set of chosen
     * folders -- could previously only answer {@code null}, which pushed the work onto the operator: the host
     * then required {@code connector.roots} to be set by hand, with ids in whatever composite form that
     * connector happens to use.</p>
     *
     * <p>Defaults to the singleton of {@link #getRootNodeId()}, or an empty list when that answers
     * {@code null}, so every existing connector and every connector written against the older method keeps
     * working unchanged and only the ones with something more to say override this.</p>
     *
     * <p>Override this <em>or</em> {@link #getRootNodeId()}, not both: overriding both means two answers that
     * can disagree, and the host reads this one.</p>
     *
     * <h3>It may be called on every pass, so it must be cheap or memoised</h3>
     * <p>A host resolves roots per pass rather than at startup, which is what lets an operator change a scope
     * without a restart. A connector that answers this by calling its source must therefore memoise the
     * result: doing the call per pass is wasteful, and doing it in a constructor instead would put it on the
     * host's startup path, where a transient source outage becomes a container that will not boot.</p>
     *
     * @return the roots, never {@code null}. An empty list means "I do not know", which leaves the host to
     *         fall back to its configuration.
     */
    default List<String> getRootNodeIds() {
        String single = getRootNodeId();
        return single == null || single.isBlank() ? List.of() : List.of(single.trim());
    }

    /**
     * Fetches a single node by its source-system identifier.
     *
     * @param nodeId source-system node identifier
     * @return the node as a {@link SourceNode}, or {@code null} when not found
     */
    SourceNode getNode(String nodeId);

    /**
     * Lists direct children of a container node.
     *
     * <p><strong>A page shorter than {@code maxItems} does not mean the container is exhausted.</strong>
     * Only an empty page does. An implementation is free to return fewer entries than asked for: several
     * here apply the source's own page window and then drop what they cannot represent as a
     * {@link SourceNode} -- a CMIS relationship, a directory entry whose attributes will not read -- so
     * a full window can arrive as a short page with more still to come.</p>
     *
     * <p>Callers must therefore page until they get an empty list, and must advance the cursor by
     * {@code maxItems} rather than by the size of the page they received. Advancing by the size received
     * re-reads the entries that were dropped, which either loops forever or shifts every subsequent page
     * window. Terminating on a short page silently truncates the container, which is worse than it sounds:
     * a discovery pass that reports itself complete hands the reconciliation sweep a list the sweep then
     * treats as authoritative, so the dropped tail is deleted from the index rather than merely missed.</p>
     *
     * @param containerId source-system identifier of the parent container
     * @param skip        number of entries to skip (for pagination); callers advance it by {@code maxItems}
     * @param maxItems    maximum number of entries to return; fewer is allowed, more is not
     * @return list of child nodes; empty list when there is nothing left at or beyond {@code skip}
     */
    List<SourceNode> getChildren(String containerId, int skip, int maxItems);

    /**
     * Whether this source can report what changed since a previous pass, instead of being walked.
     *
     * <p>This is the capability gate, and the only thing a host checks before taking the incremental
     * path. Defaults to {@code false}, so a connector written before this -- or one whose source has no
     * change feed -- is walked exactly as it is today. Not implementing a feed is a supported choice, not
     * a degraded one: the walk plus the reconciliation sweep is the authoritative mechanism, and a feed
     * is an optimisation over it.</p>
     */
    default boolean supportsChangeFeed() {
        return false;
    }

    /**
     * The source's current feed position, for a host that has no cursor stored yet, or {@code null} when
     * the source cannot name one.
     *
     * <p>This is a position, not a licence to skip anything. A host with no cursor reads this, walks the
     * source in full, and only then saves the value it read: the walk is what indexes the nodes that
     * already exist, and a feed opened at "now" would never mention them. Reading the position first and
     * saving it second means a change made while the walk ran is replayed by the next incremental pass
     * rather than falling between the two mechanisms.</p>
     *
     * <p>{@code null} is a supported answer and means every pass walks, which is the behaviour of a source
     * with no feed at all. It is also the right answer for a source whose only token is start-of-time:
     * nothing here distinguishes such a token from a current one, so a host that took one for the other
     * would either replay the whole history on every pass or index nothing.</p>
     */
    default String initialCursor() {
        return null;
    }

    /**
     * Reads one page of the source's change feed.
     *
     * <p>Throws by default rather than returning an empty page. An empty page is indistinguishable from
     * "nothing changed", and a host that skipped the walk on that basis would index nothing and treat the
     * empty deletion list as authoritative; silence in the dangerous direction is the wrong default. The
     * gate is {@link #supportsChangeFeed()}, so this is only ever called on a source that said yes.</p>
     *
     * <p>Implementations must return {@link SourceChangePage#expired()} rather than throwing when the
     * cursor is no longer usable, must not parse or reorder the cursor they were given, and may return
     * fewer than {@code maxItems} changes with {@code moreAvailable} still true.</p>
     *
     * @param cursor   the token from the previous page's {@code nextCursor}, or the value
     *                 {@link #initialCursor()} gave the host before its seeding walk. Never {@code null} in
     *                 practice: a host with no stored cursor walks instead of reading the feed
     * @param maxItems soft upper bound on the number of changed nodes in the page
     * @return the page; never {@code null}
     * @throws UnsupportedOperationException when the source has no change feed
     */
    default SourceChangePage changesSince(String cursor, int maxItems) {
        throw new UnsupportedOperationException(
                "Source '" + getSourceType() + "' has no change feed; discovery must walk it");
    }

    /**
     * Downloads the primary content blob to a temporary {@link Resource}.
     *
     * <p>Callers are responsible for deleting any temporary files after use.</p>
     *
     * @param nodeId   source-system node identifier
     * @param fileName preferred file name used as a suffix for the temp resource
     * @return resource containing the downloaded content
     */
    Resource downloadContent(String nodeId, String fileName);

    /**
     * Downloads the primary content blob as a byte array.
     *
     * @param nodeId source-system node identifier
     * @return content bytes
     */
    byte[] getContent(String nodeId);

    /**
     * Writes the sync status back to the source node so the status API can read it
     * without a secondary hxpr query. Default implementation is a no-op; Alfresco
     * overrides this to persist {@code cl:syncStatusValue} and {@code cl:syncError}.
     *
     * <p>Implementations must be best-effort: any failure must be swallowed and logged
     * rather than propagated, to avoid disrupting the ingestion pipeline.</p>
     *
     * @param nodeId source-system node identifier
     * @param status status value ({@code PENDING}, {@code INDEXED}, or {@code FAILED})
     * @param error  error message, or {@code null} to clear any previous error
     */
    default void writeSyncStatus(String nodeId, String status, String error) {}

    /**
     * Clears any sync-status state previously written by {@link #writeSyncStatus} from
     * the source node. Used during subtree tear-down (e.g. {@code cl:indexed} removed
     * from an ancestor folder) so the source no longer advertises a stale ingestion
     * status.
     *
     * <p>Default implementation is a no-op. Implementations must be best-effort: any
     * failure must be swallowed and logged rather than propagated.</p>
     *
     * @param nodeId source-system node identifier
     */
    default void clearSyncStatus(String nodeId) {}

    /**
     * How this source is authenticating, or {@code null} when there is nothing worth showing an operator.
     *
     * <p>{@code null} is the right answer for most sources and is the default, so nothing has to opt out. A
     * source whose credential is deployment configuration has no state that changes: it works, or the container
     * failed to start. The sources that need this are the ones holding a credential that can lapse while
     * everything else stays healthy, because there the failure arrives as a job that stopped working for
     * reasons the log explains in terms of a file the operator has never seen.</p>
     *
     * <p>Two obligations on an implementation, and both are about what a caller is. This is read by a status
     * endpoint that may be polled, so it <strong>must not</strong> acquire a token, call the source, or
     * otherwise block: report {@code null} in a field rather than making a status response wait on a directory.
     * And every value reaches a browser, so it must carry no token, no secret, and no path to a file holding
     * either -- see {@link SourceAuthState} for why the cache path in particular is excluded.</p>
     *
     * @return the current authentication state, or {@code null} when this source has none to report
     */
    default SourceAuthState authState() {
        return null;
    }
}
