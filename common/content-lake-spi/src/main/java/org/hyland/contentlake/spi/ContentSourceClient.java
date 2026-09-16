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
}
