package org.hyland.example.connector.directory;

import org.hyland.contentlake.spi.ConnectorContext;
import org.hyland.contentlake.spi.ConnectorPlugin;
import org.hyland.contentlake.spi.ConnectorSchema;
import org.hyland.contentlake.spi.ContentSourceClient;

import java.util.List;
import java.util.Set;

/**
 * A complete, working connector plugin: it ingests the files in a mounted directory.
 *
 * <p>This exists because the archetype generates a skeleton whose {@code getChildren} returns nothing, so a
 * jar built from it loads and then discovers zero documents, which proves the plugin mechanism but not an
 * ingestion. A directory is the cheapest real source: no service to stand up, and the expected document set
 * is whatever is in the folder.</p>
 *
 * <p>It is a worked example, not a supported source. The in-tree filesystem connector
 * ({@code filesystem-batch-ingester}) is what a deployment should use for a mounted directory: it has scope
 * patterns, exclusions and real ACL configuration. This one is deliberately about a hundred lines.</p>
 *
 * <h3>Running it</h3>
 * <pre>
 * mvn package
 * cp target/sample-directory-connector-1.0.0.jar &lt;deployment&gt;/connectors/
 * SAMPLE_DIRECTORY_ROOT_PATH=/data/sample docker compose --profile alfresco --profile connector up -d
 * curl -u admin:admin http://localhost:9096/api/connectors
 * curl -u admin:admin -X POST http://localhost:9096/api/sync/configured
 * </pre>
 */
public class DirectoryConnectorPlugin implements ConnectorPlugin {

    /** Prefix of {@code cin_sourceId} for every document this connector ingests. */
    static final String SOURCE_TYPE = "sample-directory";

    static final String ROOT_PATH_SETTING = SOURCE_TYPE + ".root-path";
    static final String SOURCE_ID_SETTING = SOURCE_TYPE + ".source-id";
    static final String READ_PRINCIPALS_SETTING = SOURCE_TYPE + ".read-principals";
    static final String PAGE_SIZE_SETTING = SOURCE_TYPE + ".page-size";

    private static final int DEFAULT_PAGE_SIZE = 100;
    private static final String DEFAULT_PRINCIPAL = "__Everyone__";

    @Override
    public String sourceType() {
        return SOURCE_TYPE;
    }

    @Override
    public String displayName() {
        return "Sample directory connector";
    }

    /**
     * What this connector needs.
     *
     * <p>{@code root-path} is a {@code DIRECTORY} rather than a {@code STRING} so the host refuses to start
     * when the volume is not mounted. Without that, an unmounted path reads as an empty source: the ingester
     * runs, reports zero documents, and nothing says why.</p>
     *
     * <p>Note that every setting here is hyphenated and every one is settable as
     * {@code SAMPLE_DIRECTORY_ROOT_PATH} and friends. That mapping is the host's, not this connector's; a
     * connector only declares the names.</p>
     */
    @Override
    public ConnectorSchema schema() {
        return ConnectorSchema.builder(SOURCE_TYPE)
                .required(ROOT_PATH_SETTING, ConnectorSchema.FieldType.DIRECTORY,
                        "Directory to ingest, as seen from inside the container")
                .optional(SOURCE_ID_SETTING, ConnectorSchema.FieldType.STRING,
                        "Source alias stored as the second half of cin_sourceId")
                .optional(READ_PRINCIPALS_SETTING, ConnectorSchema.FieldType.LIST,
                        "Principals granted read access to every ingested file; defaults to everyone")
                .optional(PAGE_SIZE_SETTING, ConnectorSchema.FieldType.INTEGER,
                        "Entries returned per directory listing")
                .build();
    }

    @Override
    public ContentSourceClient createClient(ConnectorContext context) {
        List<String> principals = context.listProperty(READ_PRINCIPALS_SETTING);
        return new DirectoryConnectorClient(
                context.property(ROOT_PATH_SETTING),
                context.property(SOURCE_ID_SETTING, SOURCE_TYPE),
                principals.isEmpty() ? Set.of(DEFAULT_PRINCIPAL) : Set.copyOf(principals),
                context.intProperty(PAGE_SIZE_SETTING, DEFAULT_PAGE_SIZE));
    }

    /**
     * No scope resolver and no extractor, on purpose: this is the shape most connectors want.
     *
     * <p>Returning {@code null} from both (the interface defaults) leaves the host to ingest every document
     * it walks to and to extract text with its own chain, which already handles engine fallback and
     * in-process Tika. A connector only needs its own when the source has a notion of scope the host cannot
     * see, or can convert content without a download.</p>
     */
    static List<String> settingNames() {
        return List.of(ROOT_PATH_SETTING, SOURCE_ID_SETTING, READ_PRINCIPALS_SETTING, PAGE_SIZE_SETTING);
    }
}
