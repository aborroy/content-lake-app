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
}
