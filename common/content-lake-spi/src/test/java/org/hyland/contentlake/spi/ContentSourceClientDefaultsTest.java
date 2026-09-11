package org.hyland.contentlake.spi;

import org.junit.jupiter.api.Test;
import org.springframework.core.io.Resource;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * What an adapter gets without implementing the optional methods.
 *
 * <p>The point of {@link ContentSourceClient#getRootNodeId()} defaulting to {@code null} rather than
 * throwing is that a host can ask any connector where to start and act on "it does not say" (#132). A
 * default that threw would make the question unaskable.</p>
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
}
