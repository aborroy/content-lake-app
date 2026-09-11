package org.hyland.contentlake.connector;

import org.hyland.contentlake.spi.ScopeResolver;
import org.hyland.contentlake.spi.SourceNode;

/**
 * The host's default scope: ingest every document, walk every container.
 *
 * <p>{@link org.hyland.contentlake.spi.ConnectorPlugin#createScopeResolver} may return {@code null} to
 * "leave the decision to the host's default". This is that default, so a plugin with no scope rules of its
 * own does not have to write one.</p>
 *
 * <p>Permissive on purpose, and safe to be: scope decides what is <em>worth</em> ingesting, not who may
 * read it. Read access comes from {@link SourceNode#readPrincipals()}, which the connector supplies per
 * node, so a broad scope cannot widen visibility. A connector that needs to exclude parts of its source
 * implements its own resolver, which is why the Alfresco ({@code cl:indexed}) and filesystem (path and
 * extension patterns) adapters have theirs.</p>
 */
public class DefaultScopeResolver implements ScopeResolver {

    /**
     * Every non-container node is in scope. A container is not: a folder carries no content to extract,
     * and discovery reaches its documents through {@link #shouldTraverse} instead.
     */
    @Override
    public boolean isInScope(SourceNode node) {
        return node != null && !node.folder();
    }

    /** Every container is walked. */
    @Override
    public boolean shouldTraverse(SourceNode node) {
        return node != null && node.folder();
    }
}
