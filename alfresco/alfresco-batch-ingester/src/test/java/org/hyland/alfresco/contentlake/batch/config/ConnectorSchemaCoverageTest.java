package org.hyland.alfresco.contentlake.batch.config;

import org.hyland.alfresco.contentlake.client.AlfrescoClient;
import org.hyland.contentlake.spi.ConnectorSchema;
import org.junit.jupiter.api.Test;
import org.springframework.boot.env.YamlPropertySourceLoader;
import org.springframework.core.env.PropertySource;
import org.springframework.core.io.ClassPathResource;

import java.io.IOException;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The Alfresco connector's schema has to describe the settings this ingester actually reads (#123). A
 * field naming a property the configuration does not define would make startup validation report a
 * missing required setting that nothing uses.
 *
 * <p>Placeholders in {@code application.yml} are left unresolved: presence is what is being checked, not
 * the value.</p>
 */
class ConnectorSchemaCoverageTest {

    /** The schema is static, so the client needs no wired API handlers to publish it. */
    private final ConnectorSchema schema = new AlfrescoClient(null, null).connectorSchema();

    @Test
    void everySchemaFieldIsDefinedInApplicationYml() throws IOException {
        PropertySource<?> yml = applicationYml();

        assertThat(schema.fields()).allSatisfy(field ->
                assertThat(yml.containsProperty(field.name()) || yml.containsProperty(field.name() + "[0]"))
                        .as("application.yml defines %s", field.name())
                        .isTrue());
    }

    /**
     * The repository URL and the service account: without any of the three the connector cannot read the
     * repository at all. The transform service is optional because extraction degrades to Tika.
     */
    @Test
    void theRepositoryEndpointAndServiceAccountAreRequired() {
        assertThat(schema.fields())
                .filteredOn(ConnectorSchema.Field::required)
                .extracting(ConnectorSchema.Field::name)
                .containsExactly(
                        "content.service.url",
                        "content.service.security.basicAuth.username",
                        "content.service.security.basicAuth.password");
    }

    @Test
    void thePasswordIsTheOnlySecret() {
        assertThat(schema.fields())
                .filteredOn(ConnectorSchema.Field::secret)
                .extracting(ConnectorSchema.Field::name)
                .containsExactly("content.service.security.basicAuth.password");
    }

    /**
     * What belongs to the ingester rather than to the source stays out: the ActiveMQ broker the live
     * ingester listens on, the discovery roots, the batch executor and the shared hxpr and embedding
     * settings. Listing them here would repeat the same fields in every connector's schema.
     */
    @Test
    void ingesterAndPipelineSettingsAreNotInTheSchema() {
        assertThat(schema.fields()).extracting(ConnectorSchema.Field::name)
                .noneMatch(name -> name.startsWith("hxpr.")
                        || name.startsWith("ingestion.")
                        || name.startsWith("spring.")
                        || name.startsWith("extraction."));
    }

    private static PropertySource<?> applicationYml() throws IOException {
        List<PropertySource<?>> sources =
                new YamlPropertySourceLoader().load("application", new ClassPathResource("application.yml"));
        assertThat(sources).isNotEmpty();
        return sources.getFirst();
    }
}
