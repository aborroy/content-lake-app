package org.hyland.contentlake.connector;

import org.hyland.contentlake.spi.ConnectorContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.InstanceOfAssertFactories.STRING;

/**
 * Loading a connector from a jar that was never part of this build (#124).
 *
 * <p>Every jar here is compiled and packaged during the test, so what is exercised is the real path: a
 * class the application classpath has never seen, discovered through {@link java.util.ServiceLoader} from a
 * file in a directory, resolving the SPI types to the host's own.</p>
 */
class ConnectorPluginLoaderTest {

    @TempDir
    Path pluginDirectory;

    private final Map<String, String> config = new HashMap<>();

    private final ConnectorContext context = config::get;

    private ConnectorPluginLoader loader() {
        return new ConnectorPluginLoader(context, ConnectorConfigurationValidator.Mode.FAIL);
    }

    // ------------------------------------------------------------------
    // Plugin sources, compiled during the test
    // ------------------------------------------------------------------

    /**
     * A connector needing one setting. Deliberately written the way a real plugin would be: against the SPI
     * only, with a public no-argument constructor.
     */
    private static String pluginSource(String className, String sourceType, boolean throwOnCreate) {
        int lastDot = className.lastIndexOf('.');
        String simpleName = className.substring(lastDot + 1);
        return """
                package %s;

                import org.hyland.contentlake.spi.*;
                import org.springframework.core.io.Resource;
                import java.util.List;

                public class %s implements ConnectorPlugin {

                    @Override
                    public String sourceType() {
                        return "%s";
                    }

                    @Override
                    public String displayName() {
                        return "%s connector";
                    }

                    @Override
                    public ConnectorSchema schema() {
                        return ConnectorSchema.builder("%s")
                                .required("%s.url", ConnectorSchema.FieldType.URL, "Where the source lives")
                                .build();
                    }

                    @Override
                    public ContentSourceClient createClient(ConnectorContext context) {
                        %s
                    }

                    @Override
                    public ScopeResolver createScopeResolver(ConnectorContext context,
                                                             ContentSourceClient client) {
                        return new ScopeResolver() {
                            @Override public boolean isInScope(SourceNode node) { return true; }
                            @Override public boolean shouldTraverse(SourceNode node) { return true; }
                        };
                    }

                    public static final class Client implements ContentSourceClient {

                        private final String url;

                        Client(String url) {
                            this.url = url;
                        }

                        @Override public String getSourceId() { return url; }
                        @Override public String getSourceType() { return "%s"; }
                        @Override public SourceNode getNode(String nodeId) { return null; }
                        @Override public List<SourceNode> getChildren(String c, int s, int m) {
                            return List.of();
                        }
                        @Override public Resource downloadContent(String nodeId, String fileName) {
                            return null;
                        }
                        @Override public byte[] getContent(String nodeId) { return new byte[0]; }

                        @Override
                        public ConnectorSchema connectorSchema() {
                            return ConnectorSchema.builder("%s")
                                    .required("%s.url", ConnectorSchema.FieldType.URL,
                                            "Where the source lives")
                                    .build();
                        }
                    }
                }
                """.formatted(
                className.substring(0, lastDot), simpleName, sourceType, sourceType, sourceType, sourceType,
                throwOnCreate
                        ? "throw new IllegalStateException(\"cannot reach the repository\");"
                        : "return new Client(context.property(\"" + sourceType + ".url\"));",
                sourceType, sourceType, sourceType);
    }

    private void writeWorkingPlugin(String jarName, String sourceType) {
        PluginJars.writePluginJar(pluginDirectory, jarName,
                "test.plugin." + sourceType + ".Plugin",
                pluginSource("test.plugin." + sourceType + ".Plugin", sourceType, false));
    }

    // ------------------------------------------------------------------
    // The happy path
    // ------------------------------------------------------------------

    @Test
    void loadsAConnectorFromAJarThatWasNeverOnTheClasspath() {
        writeWorkingPlugin("cmis-connector.jar", "cmis");
        config.put("cmis.url", "http://repo:8080/cmis");

        ConnectorPluginLoader.LoadResult result = loader().load(pluginDirectory.toString(), List.of());

        assertThat(result.problems()).isEmpty();
        assertThat(result.connectors()).singleElement().satisfies(connector -> {
            assertThat(connector.sourceType()).isEqualTo("cmis");
            assertThat(connector.displayName()).isEqualTo("cmis connector");
            // The jar the connector came from, which is the first thing to know about a broken plugin.
            assertThat(connector.origin()).isEqualTo("cmis-connector.jar");
            assertThat(connector.client().getSourceType()).isEqualTo("cmis");
            // Configured from the context, so the plugin read the host's configuration.
            assertThat(connector.client().getSourceId()).isEqualTo("http://repo:8080/cmis");
            assertThat(connector.scopeResolver()).isNotNull();
            assertThat(connector.textExtractor()).as("no extractor: the host's chain applies").isNull();
            assertThat(connector.schema().fields()).hasSize(1);
        });
    }

    @Test
    void loadsSeveralConnectorsFromSeveralJars() {
        writeWorkingPlugin("cmis-connector.jar", "cmis");
        writeWorkingPlugin("sharepoint-connector.jar", "sharepoint");
        config.put("cmis.url", "http://repo:8080/cmis");
        config.put("sharepoint.url", "http://sharepoint:8080");

        ConnectorPluginLoader.LoadResult result = loader().load(pluginDirectory.toString(), List.of());

        assertThat(result.connectors()).extracting(LoadedConnector::sourceType)
                .containsExactlyInAnyOrder("cmis", "sharepoint");
    }

    // ------------------------------------------------------------------
    // Nothing to load
    // ------------------------------------------------------------------

    @Test
    void anEmptyDirectoryLoadsNothingAndReportsNoProblem() {
        ConnectorPluginLoader.LoadResult result = loader().load(pluginDirectory.toString(), List.of());

        assertThat(result.connectors()).isEmpty();
        assertThat(result.problems()).isEmpty();
    }

    /** The normal deployment: no connectors mounted, so startup must be quiet. */
    @Test
    void aMissingDirectoryIsNotAProblem() {
        ConnectorPluginLoader.LoadResult result =
                loader().load(pluginDirectory.resolve("not-mounted").toString(), List.of());

        assertThat(result.connectors()).isEmpty();
        assertThat(result.problems()).isEmpty();
    }

    @Test
    void noDirectoryConfiguredScansTheClasspathOnly() {
        assertThatCode(() -> loader().load(null, List.of())).doesNotThrowAnyException();
        assertThat(loader().load("  ", List.of()).connectors()).isEmpty();
    }

    // ------------------------------------------------------------------
    // Broken plugins are isolated
    // ------------------------------------------------------------------

    /**
     * The acceptance criterion that matters most: one bad jar must not cost an ingester the connectors that
     * do work.
     */
    @Test
    void aCorruptJarIsReportedAndTheWorkingOneStillLoads() {
        PluginJars.writeCorruptJar(pluginDirectory, "truncated-connector.jar");
        writeWorkingPlugin("cmis-connector.jar", "cmis");
        config.put("cmis.url", "http://repo:8080/cmis");

        ConnectorPluginLoader.LoadResult result = loader().load(pluginDirectory.toString(), List.of());

        assertThat(result.connectors()).extracting(LoadedConnector::sourceType).containsExactly("cmis");
    }

    @Test
    void aServiceEntryNamingAMissingClassIsReportedAndTheWorkingOneStillLoads() {
        PluginJars.writeJarDeclaringMissingClass(pluginDirectory, "stale-connector.jar",
                "test.plugin.gone.Plugin");
        writeWorkingPlugin("cmis-connector.jar", "cmis");
        config.put("cmis.url", "http://repo:8080/cmis");

        ConnectorPluginLoader.LoadResult result = loader().load(pluginDirectory.toString(), List.of());

        assertThat(result.connectors()).extracting(LoadedConnector::sourceType).containsExactly("cmis");
        assertThat(result.problems()).isNotEmpty();
    }

    @Test
    void aPluginThatFailsToBuildIsReportedByJarAndSourceType() {
        PluginJars.writePluginJar(pluginDirectory, "broken-connector.jar", "test.plugin.broken.Plugin",
                pluginSource("test.plugin.broken.Plugin", "broken", true));
        config.put("broken.url", "http://repo:8080");

        ConnectorPluginLoader.LoadResult result = loader().load(pluginDirectory.toString(), List.of());

        assertThat(result.connectors()).isEmpty();
        assertThat(result.problems()).singleElement(STRING)
                .contains("broken")
                .contains("broken-connector.jar")
                .contains("cannot reach the repository");
    }

    // ------------------------------------------------------------------
    // Configuration and identity
    // ------------------------------------------------------------------

    @Test
    void aPluginWhoseRequiredSettingIsMissingIsRefusedByName() {
        writeWorkingPlugin("cmis-connector.jar", "cmis");
        // cmis.url is not configured.

        ConnectorPluginLoader.LoadResult result = loader().load(pluginDirectory.toString(), List.of());

        assertThat(result.connectors()).isEmpty();
        assertThat(result.problems()).singleElement(STRING).contains("cmis.url");
    }

    @Test
    void aPluginWhoseSettingIsMalformedIsRefused() {
        writeWorkingPlugin("cmis-connector.jar", "cmis");
        config.put("cmis.url", "repo:8080");

        ConnectorPluginLoader.LoadResult result = loader().load(pluginDirectory.toString(), List.of());

        assertThat(result.connectors()).isEmpty();
        assertThat(result.problems()).singleElement(STRING).contains("absolute URL");
    }

    /** With validation off, the host builds whatever the plugin says it can build. */
    @Test
    void withValidationOffAnUnconfiguredPluginIsStillBuilt() {
        writeWorkingPlugin("cmis-connector.jar", "cmis");
        ConnectorPluginLoader lenient =
                new ConnectorPluginLoader(context, ConnectorConfigurationValidator.Mode.OFF);

        ConnectorPluginLoader.LoadResult result = lenient.load(pluginDirectory.toString(), List.of());

        assertThat(result.connectors()).hasSize(1);
        assertThat(result.problems()).isEmpty();
    }

    /**
     * The source type is the prefix of {@code cin_sourceId}, so a plugin claiming one an in-tree connector
     * already uses would make their documents indistinguishable in the index.
     */
    @Test
    void aPluginCannotClaimAnInTreeSourceType() {
        writeWorkingPlugin("fake-alfresco.jar", "alfresco");
        config.put("alfresco.url", "http://evil:8080");

        ConnectorPluginLoader.LoadResult result =
                loader().load(pluginDirectory.toString(), List.of("alfresco"));

        assertThat(result.connectors()).isEmpty();
        assertThat(result.problems()).singleElement(STRING)
                .contains("alfresco")
                .contains("already provided by in-tree");
    }

    @Test
    void twoPluginsClaimingTheSameSourceTypeKeepTheFirstAndReportTheSecond() {
        PluginJars.writePluginJar(pluginDirectory, "a-cmis.jar", "test.plugin.first.Plugin",
                pluginSource("test.plugin.first.Plugin", "cmis", false));
        PluginJars.writePluginJar(pluginDirectory, "b-cmis.jar", "test.plugin.second.Plugin",
                pluginSource("test.plugin.second.Plugin", "cmis", false));
        config.put("cmis.url", "http://repo:8080/cmis");

        ConnectorPluginLoader.LoadResult result = loader().load(pluginDirectory.toString(), List.of());

        assertThat(result.connectors()).hasSize(1);
        assertThat(result.problems()).singleElement(STRING).contains("already provided by");
    }
}
