package org.hyland.connector.contentlake.batch.config;

import org.hyland.connector.contentlake.batch.service.ConnectorBatchIngestionService;
import org.hyland.connector.contentlake.batch.service.ConnectorDiscoveryService;
import org.hyland.contentlake.connector.ConnectorRegistry;
import org.hyland.contentlake.connector.LoadedConnector;
import org.hyland.contentlake.service.NodeSyncService;
import org.hyland.contentlake.spi.ConnectorSchema;
import org.hyland.contentlake.spi.ContentSourceClient;
import org.hyland.contentlake.spi.SourceNode;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.ai.embedding.EmbeddingModel;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.autoconfigure.context.ConfigurationPropertiesAutoConfiguration;
import org.springframework.boot.autoconfigure.context.PropertyPlaceholderAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.core.io.Resource;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * That the pipeline actually assembles.
 *
 * <p>Worth its own test because the failure mode is a container that will not start, and the two ways this
 * host could get there are invisible in unit tests of the pieces: a plugin's client published as a
 * {@code ContentSourceClient} bean would be a cycle through core's registry, and a connector resolved too
 * eagerly would be needed before the registry exists.</p>
 */
class AppConfigWiringTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(
                    ConfigurationPropertiesAutoConfiguration.class,
                    PropertyPlaceholderAutoConfiguration.class))
            .withUserConfiguration(AppConfig.class)
            .withBean(EmbeddingModel.class, () -> Mockito.mock(EmbeddingModel.class))
            .withPropertyValues(
                    "hxpr.url=http://localhost:8080",
                    "hxpr.repository-id=default",
                    "hxpr.target-path=/connector-sync",
                    "hxpr.username=admin",
                    "hxpr.password=password",
                    "connector.embedding.model-name=ai/mxbai-embed-large");

    @Test
    void buildsTheWholePipelineFromOneLoadedConnector() {
        runner.withBean(ConnectorRegistry.class, () -> registry("sample", "root-1"))
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context).hasSingleBean(SelectedConnector.class);
                    assertThat(context).hasSingleBean(ConnectorDiscoveryService.class);
                    assertThat(context).hasSingleBean(ConnectorBatchIngestionService.class);
                    assertThat(context).hasSingleBean(NodeSyncService.class);
                    assertThat(context.getBean(SelectedConnector.class).sourceType()).isEqualTo("sample");
                });
    }

    /**
     * The plugin's client must not become a {@code ContentSourceClient} bean. Core builds the registry from
     * an {@code ObjectProvider<ContentSourceClient>}, so one sourced from the registry would be a cycle, and
     * the listing at {@code /api/connectors} would report the same connector twice.
     */
    @Test
    void doesNotPublishThePluginClientAsAnSpiBean() {
        runner.withBean(ConnectorRegistry.class, () -> registry("sample", "root-1"))
                .run(context -> assertThat(context).doesNotHaveBean(ContentSourceClient.class));
    }

    /** An empty registry has to stop the container, not produce a service that ingests nothing. */
    @Test
    void failsToStartWithNoConnector() {
        runner.withBean(ConnectorRegistry.class, ConnectorRegistry::empty)
                .run(context -> assertThat(context).getFailure()
                        .hasMessageContaining("No connector plugin was loaded"));
    }

    /** Nor when the connector cannot say where a batch pass should start. */
    @Test
    void failsToStartWhenNoRootCanBeResolved() {
        runner.withBean(ConnectorRegistry.class, () -> registry("sample", null))
                .run(context -> assertThat(context).getFailure()
                        .hasMessageContaining("connector.roots"));
    }

    @Test
    void startsWhenTheRootComesFromConfigurationInsteadOfTheConnector() {
        runner.withBean(ConnectorRegistry.class, () -> registry("sample", null))
                .withPropertyValues("connector.roots=configured-root")
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context).hasSingleBean(ConnectorDiscoveryService.class);
                });
    }

    private static ConnectorRegistry registry(String sourceType, String rootNodeId) {
        LoadedConnector connector = new LoadedConnector(sourceType, sourceType + " connector",
                sourceType + ".jar", ConnectorSchema.empty(sourceType),
                new StubClient(sourceType, rootNodeId), null, null);
        return new ConnectorRegistry(List.of(connector), List.of());
    }

    private record StubClient(String sourceType, String rootNodeId) implements ContentSourceClient {

        @Override
        public String getSourceId() {
            return sourceType + "-instance";
        }

        @Override
        public String getSourceType() {
            return sourceType;
        }

        @Override
        public String getRootNodeId() {
            return rootNodeId;
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
}
