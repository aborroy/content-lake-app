package org.hyland.contentlake.connector;

import org.hyland.contentlake.spi.ConnectorSchema;
import org.hyland.contentlake.spi.ContentSourceClient;
import org.hyland.contentlake.spi.SourceNode;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.Resource;

import java.lang.reflect.RecordComponent;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@code GET /api/connectors} and {@code GET /api/connectors/schema} (#123, #124). Between them they answer
 * the two questions a deployment asks: what is loaded and what does it need. Neither may carry a value.
 */
class ConnectorSchemaControllerTest {

    private static final ConnectorSchema NUXEO_SCHEMA = ConnectorSchema.builder("nuxeo")
            .required("nuxeo.base-url", ConnectorSchema.FieldType.URL, "Instance URL")
            .secret("nuxeo.password", "Password for the account", true)
            .build();

    // ------------------------------------------------------------------
    // The schema endpoint
    // ------------------------------------------------------------------

    @Test
    void publishesOneSchemaPerConnector() {
        ConnectorSchemaController controller = controller(
                ConnectorRegistry.empty(),
                new StubSource("alfresco", ConnectorSchema.builder("alfresco")
                        .required("content.service.url", ConnectorSchema.FieldType.URL, "Repository URL")
                        .build()),
                new StubSource("nuxeo", NUXEO_SCHEMA));

        assertThat(controller.schemas()).extracting(ConnectorSchema::sourceType)
                .containsExactly("alfresco", "nuxeo");
    }

    /** A plugin connector's schema is published alongside the in-tree ones. */
    @Test
    void publishesPluginSchemasToo() {
        ConnectorSchemaController controller = controller(
                registryWith(pluginConnector("cmis", "cmis-connector.jar")),
                new StubSource("nuxeo", NUXEO_SCHEMA));

        assertThat(controller.schemas()).extracting(ConnectorSchema::sourceType)
                .containsExactly("nuxeo", "cmis");
    }

    /**
     * A schema carries descriptors, never values, so the response cannot leak a password however the field
     * is marked. The secret flag is still published, because tooling needs to know to mask its input.
     */
    @Test
    void carriesNoValues_onlyDescriptors() {
        ConnectorSchemaController controller =
                controller(ConnectorRegistry.empty(), new StubSource("nuxeo", NUXEO_SCHEMA));

        ConnectorSchema.Field secret = controller.schemas().getFirst().fields().get(1);

        assertThat(secret.name()).isEqualTo("nuxeo.password");
        assertThat(secret.secret()).isTrue();
        // Field has exactly six components, none of which is a value.
        assertThat(ConnectorSchema.Field.class.getRecordComponents()).extracting(RecordComponent::getName)
                .containsExactly("name", "type", "description", "required", "secret", "allowedValues");
    }

    @Test
    void anApplicationWithNoConnectorReturnsAnEmptyList() {
        assertThat(controller(ConnectorRegistry.empty()).schemas()).isEmpty();
    }

    /** A source that publishes nothing still appears, so the response lists every connector present. */
    @Test
    void aSourceWithoutASchemaStillAppears() {
        ConnectorSchemaController controller =
                controller(ConnectorRegistry.empty(), new StubSource("legacy", null));

        assertThat(controller.schemas()).singleElement().satisfies(schema -> {
            assertThat(schema.sourceType()).isEqualTo("legacy");
            assertThat(schema.fields()).isEmpty();
        });
    }

    // ------------------------------------------------------------------
    // The listing endpoint
    // ------------------------------------------------------------------

    /**
     * Whether the jar was picked up is the first question about a plugin deployment, and reading the
     * container's log is a poor way to answer it.
     */
    @Test
    void listsInTreeAndPluginConnectorsWithTheirOrigin() {
        ConnectorSchemaController controller = controller(
                registryWith(pluginConnector("cmis", "cmis-connector.jar")),
                new StubSource("nuxeo", NUXEO_SCHEMA));

        assertThat(controller.connectors().connectors())
                .extracting(ConnectorSchemaController.ConnectorInfo::sourceType,
                        ConnectorSchemaController.ConnectorInfo::origin,
                        ConnectorSchemaController.ConnectorInfo::settings)
                .containsExactly(
                        org.assertj.core.groups.Tuple.tuple("nuxeo", "in-tree", 2),
                        org.assertj.core.groups.Tuple.tuple("cmis", "cmis-connector.jar", 1));
    }

    @Test
    void namesTheImplementingClass_whichIsWhatIdentifiesAPluginsCode() {
        ConnectorSchemaController controller =
                controller(registryWith(pluginConnector("cmis", "cmis-connector.jar")));

        assertThat(controller.connectors().connectors()).singleElement()
                .extracting(ConnectorSchemaController.ConnectorInfo::implementation)
                .isEqualTo(StubSource.class.getName());
    }

    /** A jar that failed to load is a fact the running service has to be able to report. */
    @Test
    void reportsWhatFailedToLoad() {
        ConnectorRegistry registry = new ConnectorRegistry(List.of(),
                List.of("Connector plugin from broken.jar failed to build: IllegalStateException"));

        assertThat(controller(registry).connectors().problems())
                .singleElement(org.assertj.core.api.InstanceOfAssertFactories.STRING)
                .contains("broken.jar");
    }

    /** A sliced test, or any application that never built a registry, reports no plugins rather than failing. */
    @Test
    void withoutARegistryOnlyInTreeConnectorsAreListed() {
        ConnectorSchemaController controller = new ConnectorSchemaController(
                TestProviders.of(new StubSource("nuxeo", NUXEO_SCHEMA)), TestProviders.none());

        assertThat(controller.connectors().connectors()).hasSize(1);
        assertThat(controller.connectors().problems()).isEmpty();
    }

    // ------------------------------------------------------------------
    // Fixtures
    // ------------------------------------------------------------------

    private static ConnectorSchemaController controller(ConnectorRegistry registry,
                                                        ContentSourceClient... clients) {
        return new ConnectorSchemaController(TestProviders.of(clients), TestProviders.of(registry));
    }

    private static ConnectorRegistry registryWith(LoadedConnector... connectors) {
        return new ConnectorRegistry(List.of(connectors), List.of());
    }

    private static LoadedConnector pluginConnector(String sourceType, String origin) {
        ConnectorSchema schema = ConnectorSchema.builder(sourceType)
                .required(sourceType + ".url", ConnectorSchema.FieldType.URL, "Where the source lives")
                .build();
        return new LoadedConnector(sourceType, sourceType + " connector", origin, schema,
                new StubSource(sourceType, schema), null, null);
    }

    private record StubSource(String sourceType, ConnectorSchema schema) implements ContentSourceClient {

        @Override
        public String getSourceId() {
            return sourceType;
        }

        @Override
        public String getSourceType() {
            return sourceType;
        }

        @Override
        public ConnectorSchema connectorSchema() {
            return schema;
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
