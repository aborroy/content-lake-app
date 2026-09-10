package org.hyland.contentlake.connector;

import lombok.extern.slf4j.Slf4j;
import org.hyland.contentlake.spi.ContentSourceClient;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

import java.util.ArrayList;
import java.util.List;

/**
 * Loads the connector plugins for whichever application this is (#124).
 *
 * <p>In core, so every ingester gets it by component scan and none has to wire it. The RAG service gets it
 * too and will load nothing useful, because a plugin connector is an ingestion concern -- but it costs a
 * directory listing at startup and keeps the mechanism in one place.</p>
 */
@Slf4j
@Configuration
public class ConnectorPluginConfiguration {

    /**
     * Where connector jars are mounted. The default is a path rather than empty so a deployment only has to
     * mount a volume, with no environment variable to remember; a missing directory is the normal case and
     * loads nothing.
     */
    public static final String DEFAULT_PLUGIN_DIRECTORY = "/opt/content-lake/connectors";

    /**
     * The registry, built eagerly at startup.
     *
     * <p>{@code ObjectProvider<ContentSourceClient>} is asked only for the source types already present
     * in-tree, so a plugin cannot claim {@code alfresco} and make its documents indistinguishable from the
     * real ones. Resolving those beans here is safe: this bean is not a dependency of any of them.</p>
     *
     * <p>Failure to load a plugin does not fail this bean. A configuration problem does, in {@code fail}
     * mode, by the same rule that applies to an in-tree connector.</p>
     */
    @Bean
    public ConnectorRegistry connectorRegistry(
            Environment environment,
            ObjectProvider<ContentSourceClient> sourceClients,
            @Value("${content-lake.connector.plugin-directory:" + DEFAULT_PLUGIN_DIRECTORY + "}")
            String pluginDirectory,
            @Value("${content-lake.connector.validation:fail}") String validationMode) {

        ConnectorConfigurationValidator.Mode mode = ConnectorConfigurationValidator.Mode.parse(validationMode);
        ConnectorPluginLoader loader = new ConnectorPluginLoader(
                new EnvironmentConnectorContext(environment), mode);

        ConnectorPluginLoader.LoadResult result = loader.load(pluginDirectory, inTreeSourceTypes(sourceClients));
        ConnectorRegistry registry = new ConnectorRegistry(result.connectors(), result.problems());

        if (!result.problems().isEmpty() && mode == ConnectorConfigurationValidator.Mode.FAIL) {
            throw new IllegalStateException(
                    "Connector plugins could not be loaded:"
                            + String.join("\n  - ", prefixed(result.problems()))
                            + "\nRemove the jar, fix the settings, or set "
                            + "content-lake.connector.validation=warn to start anyway.");
        }
        return registry;
    }

    private static List<String> prefixed(List<String> problems) {
        List<String> lines = new ArrayList<>(problems.size() + 1);
        lines.add("");
        lines.addAll(problems);
        return lines;
    }

    private static List<String> inTreeSourceTypes(ObjectProvider<ContentSourceClient> sourceClients) {
        List<String> types = new ArrayList<>();
        for (ContentSourceClient client : sourceClients) {
            try {
                String sourceType = client.getSourceType();
                if (sourceType != null && !sourceType.isBlank()) {
                    types.add(sourceType);
                }
            } catch (Exception e) {
                log.debug("Could not read a source type while loading plugins: {}", e.getMessage());
            }
        }
        return types;
    }
}
