package org.hyland.contentlake.connector;

import org.hyland.contentlake.spi.ConnectorSchema;
import org.hyland.contentlake.spi.ContentSourceClient;
import org.hyland.contentlake.spi.SourceNode;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.Resource;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * What a host can ask about its plugin connectors (#124).
 */
class ConnectorRegistryTest {

    @Test
    void anEmptyRegistryIsWhatEveryDefaultDeploymentHas() {
        ConnectorRegistry registry = ConnectorRegistry.empty();

        assertThat(registry.isEmpty()).isTrue();
        assertThat(registry.connectors()).isEmpty();
        assertThat(registry.single()).isEmpty();
        assertThat(registry.problems()).isEmpty();
        assertThat(registry.bySourceType("cmis")).isEmpty();
    }

    @Test
    void findsAConnectorBySourceType() {
        ConnectorRegistry registry = registry(connector("cmis"), connector("sharepoint"));

        assertThat(registry.bySourceType("cmis")).get()
                .extracting(LoadedConnector::sourceType).isEqualTo("cmis");
        assertThat(registry.bySourceType("nuxeo")).isEmpty();
    }

    /**
     * {@code single()} is how a host driven by a plugin connector finds it: one mounted jar is
     * unambiguous, and with two the host has to be told which it means rather than picking one.
     */
    @Test
    void singleOnlyAnswersWhenThereIsExactlyOne() {
        assertThat(registry(connector("cmis")).single()).isPresent();
        assertThat(registry(connector("cmis"), connector("sharepoint")).single()).isEmpty();
        assertThat(ConnectorRegistry.empty().single()).isEmpty();
    }

    @Test
    void keepsLoadOrder() {
        ConnectorRegistry registry = registry(connector("b"), connector("a"), connector("c"));

        assertThat(registry.connectors()).extracting(LoadedConnector::sourceType)
                .containsExactly("b", "a", "c");
    }

    /** A jar that failed to load has to be reportable after startup, not only at the moment it failed. */
    @Test
    void carriesTheLoadProblems() {
        ConnectorRegistry registry = new ConnectorRegistry(List.of(),
                List.of("Connector plugin from broken.jar failed to build"));

        assertThat(registry.problems()).hasSize(1);
        assertThat(registry.isEmpty()).isTrue();
    }

    private static ConnectorRegistry registry(LoadedConnector... connectors) {
        return new ConnectorRegistry(List.of(connectors), List.of());
    }

    private static LoadedConnector connector(String sourceType) {
        return new LoadedConnector(sourceType, sourceType, sourceType + ".jar",
                ConnectorSchema.empty(sourceType), new StubClient(sourceType), null, null);
    }

    private record StubClient(String sourceType) implements ContentSourceClient {

        @Override
        public String getSourceId() {
            return sourceType;
        }

        @Override
        public String getSourceType() {
            return sourceType;
        }

        @Override
        public SourceNode getNode(String nodeId) {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<SourceNode> getChildren(String containerId, int skip, int maxItems) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Resource downloadContent(String nodeId, String fileName) {
            throw new UnsupportedOperationException();
        }

        @Override
        public byte[] getContent(String nodeId) {
            throw new UnsupportedOperationException();
        }
    }
}
