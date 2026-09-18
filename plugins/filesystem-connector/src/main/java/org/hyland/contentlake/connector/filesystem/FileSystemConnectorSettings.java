package org.hyland.contentlake.connector.filesystem;

import java.util.List;
import java.util.Set;

/**
 * Everything the filesystem connector was configured with.
 *
 * <p>A record built once by {@link FileSystemConnectorPlugin} from the {@code ConnectorContext}, where the
 * in-tree module this replaced used a mutable Spring {@code @ConfigurationProperties} bean. The setting
 * names are unchanged, so a deployment's existing {@code FILESYSTEM_*} environment variables keep working:
 * the host resolves a connector's settings from the environment with the same rule Spring used, dots and
 * hyphens to underscores and upper case.</p>
 *
 * @param rootPath          absolute directory to ingest, a local path or a mounted volume
 * @param sourceId          the second half of {@code cin_sourceId}
 * @param readPrincipals    who may retrieve the ingested files. The filesystem has no ACL model of its own,
 *                          so this is the only thing deciding it
 * @param includeExtensions extensions to ingest, without the dot; empty means every file
 * @param excludePatterns   path fragments that exclude a file or directory
 * @param pageSize          entries fetched per directory listing
 */
public record FileSystemConnectorSettings(String rootPath,
                                          String sourceId,
                                          Set<String> readPrincipals,
                                          List<String> includeExtensions,
                                          List<String> excludePatterns,
                                          int pageSize) {

    /** What the filesystem grants when nothing was configured: every authenticated caller. */
    public static final String EVERYONE = "__Everyone__";

    public FileSystemConnectorSettings {
        sourceId = sourceId == null || sourceId.isBlank() ? "filesystem" : sourceId.trim();
        readPrincipals = readPrincipals == null || readPrincipals.isEmpty()
                ? Set.of(EVERYONE)
                : Set.copyOf(readPrincipals);
        includeExtensions = includeExtensions == null ? List.of() : List.copyOf(includeExtensions);
        excludePatterns = excludePatterns == null ? List.of() : List.copyOf(excludePatterns);
        pageSize = pageSize > 0 ? pageSize : 100;
    }
}
