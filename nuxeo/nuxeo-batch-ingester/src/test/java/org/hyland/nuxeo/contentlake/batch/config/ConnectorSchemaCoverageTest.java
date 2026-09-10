package org.hyland.nuxeo.contentlake.batch.config;

import org.hyland.contentlake.spi.ConnectorSchema;
import org.hyland.nuxeo.contentlake.client.NuxeoClient;
import org.hyland.nuxeo.contentlake.config.NuxeoProperties;
import org.junit.jupiter.api.Test;
import org.springframework.boot.env.YamlPropertySourceLoader;
import org.springframework.core.env.PropertySource;
import org.springframework.core.io.ClassPathResource;

import java.io.IOException;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The Nuxeo connector's schema has to describe the settings this ingester actually reads (#123).
 *
 * <p>Placeholders in {@code application.yml} are left unresolved -- presence is what is being checked --
 * and the scope lists are YAML sequences, which flatten to {@code name[0]}.</p>
 */
class ConnectorSchemaCoverageTest {

    private final ConnectorSchema schema = new NuxeoClient(new NuxeoProperties()).connectorSchema();

    @Test
    void everySchemaFieldIsDefinedInApplicationYml() throws IOException {
        PropertySource<?> yml = applicationYml();

        assertThat(schema.fields()).allSatisfy(field ->
                assertThat(yml.containsProperty(field.name()) || yml.containsProperty(field.name() + "[0]"))
                        .as("application.yml defines %s", field.name())
                        .isTrue());
    }

    @Test
    void theInstanceUrlAndAccountAreRequired() {
        assertThat(schema.fields())
                .filteredOn(ConnectorSchema.Field::required)
                .extracting(ConnectorSchema.Field::name)
                .containsExactly("nuxeo.base-url", "nuxeo.username", "nuxeo.password");
    }

    @Test
    void thePasswordIsTheOnlySecret() {
        assertThat(schema.fields())
                .filteredOn(ConnectorSchema.Field::secret)
                .extracting(ConnectorSchema.Field::name)
                .containsExactly("nuxeo.password");
    }

    /**
     * Scope is part of the Nuxeo connector, unlike Alfresco's: Nuxeo has no in-repository marker aspect,
     * so what to ingest is configuration rather than repository state.
     */
    @Test
    void scopeIsPartOfTheSchema() {
        assertThat(schema.fields()).extracting(ConnectorSchema.Field::name)
                .contains("nuxeo.scope.included-roots",
                        "nuxeo.scope.included-types",
                        "nuxeo.scope.excluded-lifecycle-states");
    }

    /** The discovery mode is an enum, so a typo is caught at startup rather than at the first query. */
    @Test
    void theDiscoveryModeDeclaresItsAllowedValues() {
        assertThat(schema.fields())
                .filteredOn(field -> field.name().equals("nuxeo.discovery.mode"))
                .singleElement()
                .satisfies(field -> {
                    assertThat(field.type()).isEqualTo(ConnectorSchema.FieldType.ENUM);
                    assertThat(field.allowedValues())
                            .containsExactlyInAnyOrderElementsOf(
                                    java.util.Arrays.stream(NuxeoProperties.Mode.values())
                                            .map(Enum::name)
                                            .toList());
                });
    }

    /** The audit cursor and polling interval belong to the live ingester, not to the source. */
    @Test
    void liveIngesterSettingsAreNotInTheSchema() {
        assertThat(schema.fields()).extracting(ConnectorSchema.Field::name)
                .noneMatch(name -> name.startsWith("nuxeo.live.") || name.startsWith("hxpr."));
    }

    private static PropertySource<?> applicationYml() throws IOException {
        List<PropertySource<?>> sources =
                new YamlPropertySourceLoader().load("application", new ClassPathResource("application.yml"));
        assertThat(sources).isNotEmpty();
        return sources.getFirst();
    }
}
