package org.hyland.contentlake.connector.filesystem;

import org.hyland.contentlake.spi.ConnectorContext;
import org.hyland.contentlake.spi.ConnectorPlugin;
import org.hyland.contentlake.spi.ConnectorSchema;
import org.hyland.contentlake.spi.ContentSourceClient;
import org.hyland.contentlake.spi.ScopeResolver;
import org.hyland.contentlake.spi.TextExtractor;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * A Content Lake source for a local or mounted filesystem directory.
 *
 * <p>This was an in-tree module group with a service image of its own until #148. It is the same kind of
 * thing as the connectors beside it, and it was being delivered the expensive way: a Maven module group, a
 * Dockerfile stage pair, a compose service and an opt-in profile, to do what one jar in a directory does.
 * Nothing about the source needed any of that.</p>
 *
 * <h3>The settings are unchanged, deliberately</h3>
 * <p>Every setting keeps the name it had, so a deployment's existing {@code FILESYSTEM_*} environment
 * variables keep working. The migration is a different service to run, not a different thing to configure:
 * point the plugin runtime at this jar instead of running the retired filesystem service.</p>
 *
 * <h3>What it does not do</h3>
 * <ul>
 *   <li><strong>No ACL model.</strong> A filesystem has no permissions this could map, so every ingested
 *       file is stamped with {@code filesystem.read-principals}, which defaults to everyone. That default is
 *       right for a mounted corpus that is already shared with everyone who can reach the search endpoint,
 *       and wrong for anything else, which is why the schema states it.</li>
 *   <li><strong>Batch only.</strong> There is no change feed: {@code supportsChangeFeed()} is left at its
 *       default of false, so a pass is always a walk. Watching a directory for changes would be a real
 *       feature rather than a wiring change.</li>
 *   <li><strong>No server-side extraction.</strong> {@link #createTextExtractor} returns null, so the host's
 *       in-process Tika chain is used. There is nothing on the other end of a filesystem to ask.</li>
 * </ul>
 */
public class FileSystemConnectorPlugin implements ConnectorPlugin {

    static final String SOURCE_TYPE = FileSystemConnectorClient.SOURCE_TYPE;

    static final String ROOT_PATH_SETTING = SOURCE_TYPE + ".root-path";
    static final String SOURCE_ID_SETTING = SOURCE_TYPE + ".source-id";
    static final String READ_PRINCIPALS_SETTING = SOURCE_TYPE + ".read-principals";
    static final String INCLUDE_EXTENSIONS_SETTING = SOURCE_TYPE + ".include-extensions";
    static final String EXCLUDE_PATTERNS_SETTING = SOURCE_TYPE + ".exclude-patterns";
    static final String PAGE_SIZE_SETTING = SOURCE_TYPE + ".page-size";

    private static final int DEFAULT_PAGE_SIZE = 100;

    @Override
    public String sourceType() {
        return SOURCE_TYPE;
    }

    @Override
    public String displayName() {
        return "Filesystem connector";
    }

    /**
     * What this connector needs.
     *
     * <p>{@code root-path} is a {@code DIRECTORY} field, so the host checks at startup that it exists and is
     * readable. That is the one check here with real teeth: an ingester pointed at a path that was never
     * mounted reports zero documents and reads as an empty source rather than as a misconfiguration.</p>
     */
    @Override
    public ConnectorSchema schema() {
        return ConnectorSchema.builder(SOURCE_TYPE)
                .required(ROOT_PATH_SETTING, ConnectorSchema.FieldType.DIRECTORY,
                        "Absolute directory to ingest from, a local path or a mounted volume")
                .optional(SOURCE_ID_SETTING, ConnectorSchema.FieldType.STRING,
                        "Source alias stored as the second half of cin_sourceId")
                .optional(READ_PRINCIPALS_SETTING, ConnectorSchema.FieldType.LIST,
                        "Principals granted read access to every ingested file; defaults to everyone, "
                                + "because a filesystem has no permissions to map")
                .optional(INCLUDE_EXTENSIONS_SETTING, ConnectorSchema.FieldType.LIST,
                        "Extensions to ingest, without the dot; empty means every file")
                .optional(EXCLUDE_PATTERNS_SETTING, ConnectorSchema.FieldType.LIST,
                        "Path fragments that exclude a file or directory, e.g. .git. Hidden entries are "
                                + "always skipped without configuring anything")
                .optional(PAGE_SIZE_SETTING, ConnectorSchema.FieldType.INTEGER,
                        "Entries fetched per directory listing")
                .build();
    }

    @Override
    public ContentSourceClient createClient(ConnectorContext context) {
        return new FileSystemConnectorClient(settingsFrom(context));
    }

    @Override
    public ScopeResolver createScopeResolver(ConnectorContext context, ContentSourceClient client) {
        return new FileSystemScopeResolver(settingsFrom(context));
    }

    /**
     * Nothing to convert content with on the far side of a filesystem, so the host's chain is used.
     */
    @Override
    public TextExtractor createTextExtractor(ConnectorContext context) {
        return null;
    }

    /** Reads the settings. Touches the filesystem for nothing: the schema's DIRECTORY check does that. */
    FileSystemConnectorSettings settingsFrom(ConnectorContext context) {
        Set<String> readPrincipals = new LinkedHashSet<>(context.listProperty(READ_PRINCIPALS_SETTING));
        return new FileSystemConnectorSettings(
                context.property(ROOT_PATH_SETTING),
                context.property(SOURCE_ID_SETTING),
                readPrincipals,
                lowerCased(context.listProperty(INCLUDE_EXTENSIONS_SETTING)),
                context.listProperty(EXCLUDE_PATTERNS_SETTING),
                context.intProperty(PAGE_SIZE_SETTING, DEFAULT_PAGE_SIZE));
    }

    private static List<String> lowerCased(List<String> values) {
        return values.stream()
                .filter(value -> value != null && !value.isBlank())
                .map(value -> value.trim().toLowerCase(java.util.Locale.ROOT))
                .toList();
    }
}
