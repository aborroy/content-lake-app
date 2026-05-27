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
     * Fetches a single node by its source-system identifier.
     *
     * @param nodeId source-system node identifier
     * @return the node as a {@link SourceNode}, or {@code null} when not found
     */
    SourceNode getNode(String nodeId);

    /**
     * Lists direct children of a container node.
     *
     * @param containerId source-system identifier of the parent container
     * @param skip        number of entries to skip (for pagination)
     * @param maxItems    maximum number of entries to return
     * @return list of child nodes; empty list when the container has no children
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
