package org.hyland.filesystem.contentlake.batch.config;

import org.hyland.contentlake.spi.ConnectorSchema;
import org.hyland.filesystem.contentlake.client.FileSystemSourceClient;
import org.hyland.filesystem.contentlake.config.FileSystemProperties;
import org.junit.jupiter.api.Test;
import org.springframework.boot.env.YamlPropertySourceLoader;
import org.springframework.core.env.PropertySource;
import org.springframework.core.io.ClassPathResource;

import java.io.IOException;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The schema has to describe the settings this ingester actually reads (#123). A schema naming a
 * property the configuration does not define is worse than no schema: startup validation would report a
 * missing required setting that nothing uses, and operator tooling would ask for a value that goes
 * nowhere.
 *
 * <p>So every field is checked against the ingester's own {@code application.yml}. Placeholders are left
 * unresolved -- presence is what matters here, not the value -- and a YAML sequence flattens to
 * {@code name[0]}, which counts as the property being defined.</p>
 */
class ConnectorSchemaCoverageTest {

    private final ConnectorSchema schema =
            new FileSystemSourceClient(new FileSystemProperties()).connectorSchema();

    @Test
    void everySchemaFieldIsDefinedInApplicationYml() throws IOException {
        PropertySource<?> yml = applicationYml();

        assertThat(schema.fields()).allSatisfy(field ->
                assertThat(defines(yml, field.name()))
                        .as("application.yml defines %s", field.name())
                        .isTrue());
    }

    @Test
    void theRootPathIsTheOneRequiredSetting() {
        assertThat(schema.fields())
                .filteredOn(ConnectorSchema.Field::required)
                .extracting(ConnectorSchema.Field::name)
                .containsExactly("filesystem.root-path");
    }

    /**
     * A filesystem ingester pointed at an unmounted path ingests nothing and reads as an empty source,
     * so the root is validated as a directory that has to exist rather than as a non-blank string.
     */
    @Test
    void theRootPathIsValidatedAsADirectory() {
        assertThat(schema.fields().getFirst().type()).isEqualTo(ConnectorSchema.FieldType.DIRECTORY);
    }

    /** The filesystem has no ACL model, so this is the only thing deciding who can retrieve the content. */
    @Test
    void readPrincipalsIsPublished() {
        assertThat(schema.fields()).extracting(ConnectorSchema.Field::name)
                .contains("filesystem.read-principals");
    }

    private static boolean defines(PropertySource<?> yml, String name) {
        return yml.containsProperty(name) || yml.containsProperty(name + "[0]");
    }

    private static PropertySource<?> applicationYml() throws IOException {
        List<PropertySource<?>> sources =
                new YamlPropertySourceLoader().load("application", new ClassPathResource("application.yml"));
        assertThat(sources).isNotEmpty();
        return sources.getFirst();
    }
}
