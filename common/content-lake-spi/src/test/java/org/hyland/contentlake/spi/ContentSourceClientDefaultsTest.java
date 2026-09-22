package org.hyland.contentlake.spi;

import org.junit.jupiter.api.Test;
import org.springframework.core.io.Resource;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * What an adapter gets without implementing the optional methods.
 *
 * <p>The point of {@link ContentSourceClient#getRootNodeId()} defaulting to {@code null} rather than
 * throwing is that a host can ask any connector where to start and act on "it does not say" (#132). A
 * default that threw would make the question unaskable.</p>
 *
 * <p>{@link ContentSourceClient#changesSince} defaults the other way, and the contrast is the point:
 * there, a default that answered would be answered wrongly, because an empty page reads as "nothing
 * changed" and would let a host skip the walk and then trust an empty deletion list.</p>
 */
class ContentSourceClientDefaultsTest {

    /** A connector that implements only the four methods the pipeline requires. */
    private static class MinimalClient implements ContentSourceClient {

        @Override
        public String getSourceId() {
            return "instance-1";
        }

        @Override
        public String getSourceType() {
            return "sample";
        }

        @Override
        public SourceNode getNode(String nodeId) {
            return null;
        }

        @Override
        public List<SourceNode> getChildren(String containerId, int skip, int maxItems) {
            return List.of();
        }

        @Override
        public Resource downloadContent(String nodeId, String fileName) {
            return null;
        }

        @Override
        public byte[] getContent(String nodeId) {
            return new byte[0];
        }
    }

    private final ContentSourceClient client = new MinimalClient();

    @Test
    void namesNoRootByDefault() {
        assertThat(client.getRootNodeId()).isNull();
    }

    @Test
    void namesNoRootsByDefaultEither() {
        // Empty rather than a singleton of null, so a host can treat "I do not know" as a list it can iterate
        // without a null check, and fall back to its own configuration.
        assertThat(client.getRootNodeIds()).isEmpty();
    }

    @Test
    void aConnectorOverridingOnlyTheSingularMethodStillAnswersThePluralOne() {
        // The whole point of the default: every connector written before the plural method keeps working, and
        // a host may ask only the plural question.
        ContentSourceClient single = new MinimalClient() {
            @Override
            public String getRootNodeId() {
                return "  the-only-root  ";
            }
        };

        assertThat(single.getRootNodeIds()).containsExactly("the-only-root");
    }

    @Test
    void aBlankSingularRootIsNotPromotedToAListOfOne() {
        // A connector that answers a blank string means the same as one that answers null: it does not know.
        ContentSourceClient blank = new MinimalClient() {
            @Override
            public String getRootNodeId() {
                return "   ";
            }
        };

        assertThat(blank.getRootNodeIds()).isEmpty();
    }

    @Test
    void publishesAnEmptySchemaByDefault() {
        assertThat(client.connectorSchema().fields()).isEmpty();
        assertThat(client.connectorSchema().sourceType()).isEqualTo("sample");
    }

    /** Both status hooks are no-ops rather than failures, so a source with no place to write one is fine. */
    @Test
    void syncStatusWritesAreNoOps() {
        client.writeSyncStatus("node-1", "INDEXED", null);
        client.clearSyncStatus("node-1");
    }

    @Test
    void declaresNoChangeFeedByDefault() {
        assertThat(client.supportsChangeFeed()).isFalse();
    }

    /** No initial cursor means the host seeds one with a walk, which is what it would have done anyway. */
    @Test
    void namesNoInitialCursorByDefault() {
        assertThat(client.initialCursor()).isNull();
    }

    /** The message names the source, so a host that called it despite the gate says which connector. */
    @Test
    void refusesToReadAChangeFeedByDefault() {
        assertThatThrownBy(() -> client.changesSince("cursor-1", 100))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("sample");
    }
}
