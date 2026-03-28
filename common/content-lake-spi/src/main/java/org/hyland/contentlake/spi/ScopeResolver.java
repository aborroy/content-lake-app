package org.hyland.contentlake.spi;

/**
 * Determines whether a source node belongs in the Content Lake.
 *
 * <p>Alfresco uses {@code cl:indexed} aspects and path exclusions.
 * Nuxeo uses document types, lifecycle state, and config-driven path rules.
 * Each source adapter provides its own implementation.</p>
 */
public interface ScopeResolver {

    /**
     * Returns {@code true} when the node should be ingested into the Content Lake.
     *
     * @param node candidate node
     * @return {@code true} if in scope for ingestion
     */
    boolean isInScope(SourceNode node);

    /**
     * Returns {@code true} when a folder node should be traversed during batch discovery.
     *
     * @param node folder node to evaluate
     * @return {@code true} if the folder's children should be visited
     */
    boolean shouldTraverse(SourceNode node);
}
