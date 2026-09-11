package org.hyland.contentlake.connector;

import org.hyland.contentlake.spi.SourceNode;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The default a host applies when a plugin returns no {@code ScopeResolver} (#132).
 */
class DefaultScopeResolverTest {

    private final DefaultScopeResolver resolver = new DefaultScopeResolver();

    @Test
    void aDocumentIsInScope() {
        assertThat(resolver.isInScope(node("doc-1", false))).isTrue();
    }

    /**
     * A container is not in scope even though it is traversed: it carries no content to extract, and
     * ingesting it would put an empty document in the index for every folder in the source.
     */
    @Test
    void aContainerIsNotInScopeButIsTraversed() {
        SourceNode folder = node("folder-1", true);

        assertThat(resolver.isInScope(folder)).isFalse();
        assertThat(resolver.shouldTraverse(folder)).isTrue();
    }

    @Test
    void aDocumentIsNotTraversed() {
        assertThat(resolver.shouldTraverse(node("doc-1", false))).isFalse();
    }

    /** Neither method may throw on a null node: discovery calls them on whatever a connector returns. */
    @Test
    void aNullNodeIsNeitherInScopeNorTraversed() {
        assertThat(resolver.isInScope(null)).isFalse();
        assertThat(resolver.shouldTraverse(null)).isFalse();
    }

    private static SourceNode node(String nodeId, boolean folder) {
        return new SourceNode(nodeId, "src", "sample", nodeId, "/", folder ? null : "text/plain",
                null, folder, Set.of(), Set.of(), Map.of());
    }
}
