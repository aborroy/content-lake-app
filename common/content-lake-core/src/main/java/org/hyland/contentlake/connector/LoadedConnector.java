package org.hyland.contentlake.connector;

import org.hyland.contentlake.spi.ConnectorSchema;
import org.hyland.contentlake.spi.ContentSourceClient;
import org.hyland.contentlake.spi.ScopeResolver;
import org.hyland.contentlake.spi.TextExtractor;

/**
 * A connector the host built from a plugin (#124), with where it came from.
 *
 * <p>{@code origin} is the jar file name, or {@code classpath} for a plugin that shipped with the
 * application. It is carried because the first question about a misbehaving plugin connector is which jar
 * produced it, and a stack trace from inside a plugin does not say.</p>
 *
 * @param sourceType    the {@code cin_sourceId} prefix, unique across the host's connectors
 * @param displayName   what to call it in logs and listings
 * @param origin        jar file name, or {@code classpath}
 * @param schema        the configuration it declared, already validated
 * @param client        the connector itself
 * @param scopeResolver its scope rules, or {@code null} to use the host's default
 * @param textExtractor its own extractor, or {@code null} to use the host's extraction chain
 */
public record LoadedConnector(String sourceType,
                              String displayName,
                              String origin,
                              ConnectorSchema schema,
                              ContentSourceClient client,
                              ScopeResolver scopeResolver,
                              TextExtractor textExtractor) {

    /** Origin of a plugin that shipped on the application classpath rather than in the plugin directory. */
    public static final String CLASSPATH_ORIGIN = "classpath";
}
