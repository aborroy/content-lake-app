package org.hyland.contentlake.spi;

/**
 * A source connector shipped as a jar rather than as a module of this build (#124).
 *
 * <p>Adding a source in-tree means creating a Maven module, registering it in an intermediate POM and
 * adding a COPY line to every service Dockerfile, because each one parses the whole reactor. Omitting that
 * line from any single Dockerfile breaks that service's build even when it has nothing to do with the new
 * module. A plugin sidesteps all of it: the jar is built against {@code content-lake-spi} alone, dropped
 * into the ingester's plugin directory, and discovered at startup.</p>
 *
 * <h3>What an implementation has to provide</h3>
 * <p>A public no-argument constructor, because the JDK {@link java.util.ServiceLoader} instantiates it, and
 * a {@code META-INF/services/org.hyland.contentlake.spi.ConnectorPlugin} entry naming the class. Nothing
 * else: no Spring annotations, no dependency on core.</p>
 *
 * <p>This is a factory rather than the connector itself precisely because of that no-argument constructor:
 * the plugin declares what configuration it needs through {@link #schema()}, the host validates it and
 * hands back a {@link ConnectorContext}, and only then is the client built. A connector whose settings are
 * missing or malformed is therefore reported by name before anything tries to use it.</p>
 *
 * <h3>What the host does with it</h3>
 * <ol>
 *   <li>Validates the configuration against {@link #schema()}. Problems are reported per connector and,
 *       depending on the host's validation mode, either abort startup or skip the plugin.</li>
 *   <li>Calls {@link #createClient(ConnectorContext)}, then the two optional factory methods.</li>
 *   <li>Publishes the result: the connector appears in {@code GET /api/connectors} alongside the in-tree
 *       ones, and its beans are available to an ingester that resolves its pipeline by SPI type.</li>
 * </ol>
 *
 * <p>A plugin that throws from any of these is logged with the jar it came from and skipped. A broken
 * connector must not stop an ingester that has other work to do.</p>
 */
public interface ConnectorPlugin {

    /**
     * Short label stored as the prefix of {@code cin_sourceId}, e.g. {@code "cmis"}.
     *
     * <p>Must match {@link ContentSourceClient#getSourceType()} of the client this plugin builds, and must
     * not collide with a connector the host already has: the prefix is how a document's source is
     * identified in the index, so two connectors sharing one would be indistinguishable. The host refuses
     * the later of two plugins claiming the same type.</p>
     */
    String sourceType();

    /**
     * The configuration this connector needs, validated before {@link #createClient} is called.
     *
     * <p>Use {@link ConnectorSchema#builder(String)}. An empty schema is legitimate for a connector with
     * nothing to configure, and means nothing is checked.</p>
     */
    ConnectorSchema schema();

    /**
     * Builds the client, reading its settings from {@code context}.
     *
     * @throws RuntimeException if the connector cannot be built. The host logs it against this plugin's jar
     *                          and carries on without this connector.
     */
    ContentSourceClient createClient(ConnectorContext context);

    /**
     * Which nodes are in scope, or {@code null} to leave the decision to the host's default.
     *
     * <p>Takes the client because scope usually needs to read the source: whether a node's ancestor carries
     * a marker, what type it is, where it sits.</p>
     */
    default ScopeResolver createScopeResolver(ConnectorContext context, ContentSourceClient client) {
        return null;
    }

    /**
     * A source-specific text extractor, or {@code null} to use the host's extraction chain.
     *
     * <p>Worth providing only when the source can convert its own content, the way Nuxeo's
     * {@code ConversionService} does: extracting from a node reference avoids downloading the binary at
     * all. Everything else is better served by the host's chain, which already handles engine fallback and
     * in-process Tika.</p>
     */
    default TextExtractor createTextExtractor(ConnectorContext context) {
        return null;
    }

    /** Human-readable name for logs and the connector listing. Defaults to the source type. */
    default String displayName() {
        return sourceType();
    }
}
