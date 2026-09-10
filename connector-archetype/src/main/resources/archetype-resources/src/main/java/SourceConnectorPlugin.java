package ${package};

import org.hyland.contentlake.spi.ConnectorContext;
import org.hyland.contentlake.spi.ConnectorPlugin;
import org.hyland.contentlake.spi.ConnectorSchema;
import org.hyland.contentlake.spi.ContentSourceClient;
import org.hyland.contentlake.spi.ScopeResolver;
import org.hyland.contentlake.spi.SourceNode;

import java.util.List;

/**
 * Content Lake connector for ${sourceType}.
 *
 * <p>Discovered by the JDK ServiceLoader through
 * {@code src/main/resources/META-INF/services/org.hyland.contentlake.spi.ConnectorPlugin}, so this class
 * needs a public no-argument constructor and nothing else. Build the jar and drop it into an ingester's
 * plugin directory:</p>
 *
 * <pre>
 * mvn package
 * cp target/${artifactId}-${version}.jar /path/to/deployment/connectors/
 * </pre>
 *
 * <p>Then check it was picked up: {@code GET /api/connectors} on the ingester lists every connector, the
 * jar each came from, and anything that failed to load.</p>
 */
public class SourceConnectorPlugin implements ConnectorPlugin {

    /** Prefix of {@code cin_sourceId} for every document this connector ingests. */
    private static final String SOURCE_TYPE = "${sourceType}";

    /** Settings this connector reads. Names are what an operator sets and what validation reports. */
    private static final String URL_SETTING = SOURCE_TYPE + ".url";
    private static final String USERNAME_SETTING = SOURCE_TYPE + ".username";
    private static final String PASSWORD_SETTING = SOURCE_TYPE + ".password";
    private static final String PAGE_SIZE_SETTING = SOURCE_TYPE + ".page-size";

    private static final int DEFAULT_PAGE_SIZE = 100;

    @Override
    public String sourceType() {
        return SOURCE_TYPE;
    }

    @Override
    public String displayName() {
        return "${sourceType} connector";
    }

    /**
     * What this connector needs to be configured.
     *
     * <p>The host validates this before {@link #createClient} is called, so a missing or malformed setting
     * is reported by name at startup rather than surfacing as a failure on the first request. Mark a
     * credential as {@code secret} and its value stays out of messages, logs and the schema endpoint.</p>
     */
    @Override
    public ConnectorSchema schema() {
        return ConnectorSchema.builder(SOURCE_TYPE)
                .required(URL_SETTING, ConnectorSchema.FieldType.URL,
                        "Base URL of the ${sourceType} repository")
                .required(USERNAME_SETTING, ConnectorSchema.FieldType.STRING,
                        "Account the ingester reads the repository with")
                .secret(PASSWORD_SETTING, "Password for that account", true)
                .optional(PAGE_SIZE_SETTING, ConnectorSchema.FieldType.INTEGER,
                        "Nodes fetched per listing")
                .build();
    }

    @Override
    public ContentSourceClient createClient(ConnectorContext context) {
        return new SourceConnectorClient(
                context.property(URL_SETTING),
                context.property(USERNAME_SETTING),
                context.property(PASSWORD_SETTING),
                context.intProperty(PAGE_SIZE_SETTING, DEFAULT_PAGE_SIZE));
    }

    /**
     * Which nodes to ingest. Returning {@code null} leaves the decision to the host's default; implement it
     * when the source needs its own rule, such as a path prefix or a document type.
     */
    @Override
    public ScopeResolver createScopeResolver(ConnectorContext context, ContentSourceClient client) {
        return new ScopeResolver() {

            @Override
            public boolean isInScope(SourceNode node) {
                // TODO: decide what is worth ingesting. Everything, to start with.
                return true;
            }

            @Override
            public boolean shouldTraverse(SourceNode node) {
                // TODO: decide which containers to walk into.
                return node.folder();
            }
        };
    }

    /**
     * The settings this connector declares, for a test to assert against without repeating the strings.
     */
    static List<String> settingNames() {
        return List.of(URL_SETTING, USERNAME_SETTING, PASSWORD_SETTING, PAGE_SIZE_SETTING);
    }
}
