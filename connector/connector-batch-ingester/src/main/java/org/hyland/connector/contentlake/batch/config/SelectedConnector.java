package org.hyland.connector.contentlake.batch.config;

import org.hyland.contentlake.connector.ConnectorRegistry;
import org.hyland.contentlake.connector.DefaultScopeResolver;
import org.hyland.contentlake.connector.LoadedConnector;
import org.hyland.contentlake.spi.ContentSourceClient;
import org.hyland.contentlake.spi.ScopeResolver;
import org.hyland.contentlake.spi.TextExtractor;

import java.util.List;
import java.util.function.Supplier;

/**
 * The one connector this ingester runs, with the host's defaults filled in.
 *
 * <p>Resolution and defaulting live here rather than in {@code AppConfig} so both are testable without a
 * Spring context: what happens with no connector, with several, with one that supplies no scope rules, and
 * with one that supplies its own extractor are the questions this host is made of.</p>
 *
 * <p>Deliberately not an SPI type, and the client is deliberately not published as a
 * {@link ContentSourceClient} bean. Two reasons. Ambiguity is the documented one: an SPI bean coming from a
 * plugin breaks injection by type in any application that has one of its own. The other is mechanical and
 * would bite immediately here -- core builds {@link ConnectorRegistry} from an
 * {@code ObjectProvider<ContentSourceClient>}, so a client bean sourced from the registry would be a
 * dependency cycle.</p>
 *
 * @param sourceType    the connector's {@code cin_sourceId} prefix
 * @param displayName   what to call it in logs and status
 * @param origin        the jar it came from, or {@code classpath}
 * @param client        the connector's client
 * @param scopeResolver the connector's scope rules, or the host default when it supplied none
 * @param textExtractor the connector's extractor, or the host's extraction chain when it supplied none
 * @param ownExtractor  whether {@code textExtractor} came from the connector, for logging and tests
 */
public record SelectedConnector(String sourceType,
                                String displayName,
                                String origin,
                                ContentSourceClient client,
                                ScopeResolver scopeResolver,
                                TextExtractor textExtractor,
                                boolean ownExtractor) {

    /**
     * Picks the connector to run and fills in the host's defaults.
     *
     * @param registry          what the loader produced
     * @param requestedType     {@code connector.source-type}, or {@code null} to take the only connector
     * @param pluginDirectory   where jars are mounted, for the message when there are none
     * @param hostExtractor     builds the host's extraction chain; called only when the connector has no
     *                          extractor of its own, so a connector that extracts from node references
     *                          does not pay for a chain it will not use
     * @throws IllegalStateException when there is no connector to run, or more than one and no choice made.
     *                               Both fail startup: this service does nothing else, so an ingester that
     *                               cannot name its connector is misconfigured rather than idle.
     */
    public static SelectedConnector from(ConnectorRegistry registry,
                                         String requestedType,
                                         String pluginDirectory,
                                         Supplier<TextExtractor> hostExtractor) {
        LoadedConnector loaded = pick(registry, requestedType, pluginDirectory);

        ScopeResolver scopeResolver = loaded.scopeResolver() != null
                ? loaded.scopeResolver()
                : new DefaultScopeResolver();
        boolean ownExtractor = loaded.textExtractor() != null;
        TextExtractor textExtractor = ownExtractor ? loaded.textExtractor() : hostExtractor.get();

        return new SelectedConnector(
                loaded.sourceType(),
                loaded.displayName(),
                loaded.origin(),
                loaded.client(),
                scopeResolver,
                textExtractor,
                ownExtractor);
    }

    private static LoadedConnector pick(ConnectorRegistry registry,
                                        String requestedType,
                                        String pluginDirectory) {
        if (registry == null || registry.isEmpty()) {
            throw new IllegalStateException(
                    "No connector plugin was loaded, so this ingester has nothing to ingest from. Mount a "
                            + "connector jar in " + describe(pluginDirectory) + " and restart."
                            + problemSuffix(registry));
        }

        if (requestedType != null && !requestedType.isBlank()) {
            String wanted = requestedType.trim();
            return registry.bySourceType(wanted).orElseThrow(() -> new IllegalStateException(
                    "connector.source-type is '" + wanted + "', which no loaded connector provides. "
                            + "Loaded: " + types(registry) + "." + problemSuffix(registry)));
        }

        return registry.single().orElseThrow(() -> new IllegalStateException(
                "Several connectors are loaded (" + types(registry) + ") and connector.source-type does not "
                        + "say which to ingest with. Set it, or mount one connector."));
    }

    private static String describe(String pluginDirectory) {
        return pluginDirectory == null || pluginDirectory.isBlank()
                ? "the connector plugin directory"
                : pluginDirectory.trim();
    }

    private static List<String> types(ConnectorRegistry registry) {
        return registry.connectors().stream().map(LoadedConnector::sourceType).toList();
    }

    /**
     * Load problems appended to the failure, because "no connector was loaded" and "a connector was
     * refused" have the same symptom and completely different fixes.
     */
    private static String problemSuffix(ConnectorRegistry registry) {
        if (registry == null || registry.problems().isEmpty()) {
            return "";
        }
        return " Problems while loading: " + String.join("; ", registry.problems());
    }
}
