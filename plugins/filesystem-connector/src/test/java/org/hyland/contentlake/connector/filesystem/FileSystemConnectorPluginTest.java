package org.hyland.contentlake.connector.filesystem;

import org.hyland.contentlake.spi.ConnectorContext;
import org.hyland.contentlake.spi.ConnectorSchema;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class FileSystemConnectorPluginTest {

    /** Records what the plugin asked for, which is how the schema is checked against the code. */
    private static final class RecordingContext implements ConnectorContext {
        private final Map<String, String> values;
        private final Set<String> requested = new LinkedHashSet<>();

        RecordingContext(Map<String, String> values) {
            this.values = values;
        }

        @Override
        public String property(String name) {
            requested.add(name);
            return values.get(name);
        }
    }

    @Test
    void declaresTheSourceTypeThatPrefixesEveryDocumentsSourceId() {
        FileSystemConnectorPlugin plugin = new FileSystemConnectorPlugin();

        assertThat(plugin.sourceType()).isEqualTo("filesystem");
        assertThat(plugin.displayName()).isEqualTo("Filesystem connector");
    }

    @Test
    void everySchemaFieldIsActuallyRead() {
        RecordingContext context = new RecordingContext(Map.of("filesystem.root-path", "/tmp"));
        FileSystemConnectorPlugin plugin = new FileSystemConnectorPlugin();

        plugin.settingsFrom(context);

        // The in-tree module this replaced checked the schema against its own application.yml, on the
        // grounds that a schema naming a setting nothing reads is worse than no schema: startup validation
        // would demand a value that goes nowhere. A plugin has no application.yml, so the equivalent check
        // is against the code that reads the context.
        List<String> declared = plugin.schema().fields().stream().map(ConnectorSchema.Field::name).toList();
        assertThat(context.requested).containsAll(declared);
    }

    @Test
    void requiresOnlyTheRootPath() {
        List<String> problems = new FileSystemConnectorPlugin().schema().validate(
                name -> "filesystem.root-path".equals(name) ? "/tmp" : null);

        // Everything else has a usable default, and a filesystem with no configured principals is readable
        // by everyone, which the schema says out loud rather than leaving to be discovered.
        assertThat(problems).isEmpty();
    }

    @Test
    void refusesARootPathThatIsNotADirectory() {
        List<String> problems = new FileSystemConnectorPlugin().schema().validate(
                name -> "filesystem.root-path".equals(name) ? "/definitely/not/mounted" : null);

        // The one startup check with real teeth: an ingester pointed at an unmounted path reports zero
        // documents and reads as an empty source rather than as a misconfiguration.
        assertThat(problems).isNotEmpty();
        assertThat(String.join(" ", problems)).contains("filesystem.root-path");
    }

    @Test
    void defaultsToEveryoneBecauseAFilesystemHasNoPermissionsToMap() {
        FileSystemConnectorSettings settings = new FileSystemConnectorPlugin()
                .settingsFrom(new RecordingContext(Map.of("filesystem.root-path", "/tmp")));

        assertThat(settings.readPrincipals()).containsExactly("__Everyone__");
        assertThat(settings.sourceId()).isEqualTo("filesystem");
        assertThat(settings.pageSize()).isEqualTo(100);
    }

    @Test
    void lowerCasesTheConfiguredExtensionsSoMatchingIsPredictable() {
        FileSystemConnectorSettings settings = new FileSystemConnectorPlugin().settingsFrom(
                new RecordingContext(Map.of(
                        "filesystem.root-path", "/tmp",
                        "filesystem.include-extensions", "PDF, DocX ")));

        assertThat(settings.includeExtensions()).containsExactly("pdf", "docx");
    }

    @Test
    void suppliesAScopeResolverAndNoTextExtractor() {
        RecordingContext context = new RecordingContext(Map.of("filesystem.root-path", "/tmp"));
        FileSystemConnectorPlugin plugin = new FileSystemConnectorPlugin();

        assertThat(plugin.createScopeResolver(context, null)).isInstanceOf(FileSystemScopeResolver.class);
        // There is nothing on the far side of a filesystem to ask for a conversion.
        assertThat(plugin.createTextExtractor(context)).isNull();
    }

    @Test
    void isDiscoverableThroughTheServiceLoaderTheHostUses() {
        List<String> found = java.util.ServiceLoader
                .load(org.hyland.contentlake.spi.ConnectorPlugin.class).stream()
                .map(provider -> provider.get().sourceType())
                .toList();

        // A missing or misspelled service file is the difference between a working jar and an inert one.
        assertThat(found).contains("filesystem");
    }
}
