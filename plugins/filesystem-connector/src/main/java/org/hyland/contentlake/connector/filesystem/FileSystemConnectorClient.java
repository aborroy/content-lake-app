package org.hyland.contentlake.connector.filesystem;

import org.hyland.contentlake.spi.ConnectorSchema;
import org.hyland.contentlake.spi.ContentSourceClient;
import org.hyland.contentlake.spi.PermissionRule;
import org.hyland.contentlake.spi.SecurityConfig;
import org.hyland.contentlake.spi.SourceNode;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.logging.Logger;
import java.util.stream.Stream;

/**
 * {@link ContentSourceClient} over a local or mounted filesystem directory.
 *
 * <p>Node ids are absolute, normalised path strings. Extraction is source-agnostic, so no transform service
 * is required and the host's in-process Tika does the work. The filesystem has no native ACL model, so every
 * node is stamped with the configured read principals.</p>
 *
 * <h3>Two things that changed when this stopped being an in-tree module</h3>
 * <p>It no longer sets the generic {@code source_*} ingest properties. It never needed to: core derives all
 * six from the {@link SourceNode}'s own fields. Worse, because a node's own properties are merged
 * <em>after</em> core's, the old code overwrote core's {@code source_modifiedAt} with
 * {@code OffsetDateTime.toString()}, which core documents as the one rendering that must not be used, since
 * it elides zero seconds and so does not sort against itself. The {@code modifiedAfter} and
 * {@code modifiedBefore} filters compare that value as text, and the staleness short circuit reads it back,
 * so dropping those writes fixes a real defect rather than only removing a coupling.</p>
 *
 * <p>Logging is {@code java.util.logging} rather than Lombok's {@code @Slf4j}, matching the connectors
 * beside it: a plugin is built standalone and should not need an annotation processor.</p>
 */
public final class FileSystemConnectorClient implements ContentSourceClient {

    private static final Logger log = Logger.getLogger(FileSystemConnectorClient.class.getName());

    static final String SOURCE_TYPE = "filesystem";

    private final FileSystemConnectorSettings settings;

    public FileSystemConnectorClient(FileSystemConnectorSettings settings) {
        this.settings = settings;
    }

    @Override
    public String getSourceType() {
        return SOURCE_TYPE;
    }

    @Override
    public String getSourceId() {
        return settings.sourceId();
    }

    /**
     * What this connector needs.
     *
     * <p>{@code root-path} is validated as a directory that has to exist, which is the one startup check
     * here with real teeth: an ingester pointed at an unmounted path reports zero documents and reads as an
     * empty source rather than a misconfiguration.</p>
     *
     * <p>{@code read-principals} is listed because the filesystem has no ACL model of its own, so this is
     * the only thing deciding who can retrieve the ingested content. It is optional and defaults to
     * everyone, which is worth an operator seeing stated.</p>
     */
    @Override
    public ConnectorSchema connectorSchema() {
        return new FileSystemConnectorPlugin().schema();
    }

    /** The configured root as an absolute node id, for discovery to start from. */
    @Override
    public String getRootNodeId() {
        return Path.of(settings.rootPath()).toAbsolutePath().normalize().toString();
    }

    @Override
    public SourceNode getNode(String nodeId) {
        Path path = Path.of(nodeId);
        try {
            BasicFileAttributes attrs = Files.readAttributes(path, BasicFileAttributes.class);
            return toSourceNode(path, attrs);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to read filesystem node: " + nodeId, e);
        }
    }

    @Override
    public List<SourceNode> getChildren(String containerId, int skip, int maxItems) {
        Path dir = Path.of(containerId);
        if (!Files.isDirectory(dir)) {
            return List.of();
        }
        try (Stream<Path> entries = Files.list(dir)) {
            return entries.sorted()
                    .skip(Math.max(0, skip))
                    .limit(Math.max(0, maxItems))
                    .map(this::toSourceNodeQuietly)
                    .filter(Objects::nonNull)
                    .toList();
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to list directory: " + containerId, e);
        }
    }

    /**
     * Copies the file to a temp file and returns that, never the source file itself.
     *
     * <p>{@link ContentSourceClient#downloadContent} makes the caller responsible for deleting what it gets
     * back, and the shared pipeline does exactly that after extraction. Returning the source path would
     * therefore delete the very document just indexed. For a repository-backed source the distinction is
     * invisible because the download is always a copy; here the source is a local file, so the copy has to
     * be explicit.</p>
     */
    @Override
    public Resource downloadContent(String nodeId, String fileName) {
        Path source = Path.of(nodeId);
        try {
            Path temp = Files.createTempFile("filesystem-", "-" + safeSuffix(fileName, source));
            Files.copy(source, temp, StandardCopyOption.REPLACE_EXISTING);
            return new FileSystemResource(temp);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to copy file for extraction: " + nodeId, e);
        }
    }

    /** A temp-file suffix that keeps the extension, which extractors use to sniff the format. */
    private static String safeSuffix(String fileName, Path source) {
        String name = (fileName == null || fileName.isBlank())
                ? String.valueOf(source.getFileName())
                : fileName;
        // Separators become underscores and dot runs collapse, so nothing in a caller-supplied name can
        // walk out of the temp directory or produce a hidden file.
        String sanitised = name.replaceAll("[^A-Za-z0-9._-]", "_").replaceAll("\\.{2,}", "_");
        return sanitised.isBlank() ? "content" : sanitised;
    }

    @Override
    public byte[] getContent(String nodeId) {
        try {
            return Files.readAllBytes(Path.of(nodeId));
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to read file content: " + nodeId, e);
        }
    }

    /**
     * Converts one directory entry, dropping it only when it is no longer there.
     *
     * <p>A vanished entry, or a symlink whose target is gone, is a deletion: dropping it is what the
     * reconciliation sweep should then act on, and it is the only case that reads as
     * {@link NoSuchFileException}. Any other failure means the entry is there and could not be read, such as
     * a permission on the entry, a symlink loop or an unreachable mount, and swallowing it would let the
     * sweep read "not discovered" as "deleted at source" and remove a document that is still there. So it
     * fails the listing, which fails the subtree and the job, before any sweep can run.</p>
     */
    private SourceNode toSourceNodeQuietly(Path path) {
        try {
            return toSourceNode(path, Files.readAttributes(path, BasicFileAttributes.class));
        } catch (NoSuchFileException e) {
            log.fine(() -> "Filesystem entry " + path + " is no longer there: " + e.getMessage());
            return null;
        } catch (IOException e) {
            throw new UncheckedIOException(
                    "Filesystem entry " + path + " is present but its attributes cannot be read, so a "
                            + "discovery pass over this directory would be silently incomplete", e);
        }
    }

    private SourceNode toSourceNode(Path path, BasicFileAttributes attrs) {
        Path normalized = path.toAbsolutePath().normalize();
        String nodeId = normalized.toString();
        boolean folder = attrs.isDirectory();
        String name = normalized.getFileName() != null ? normalized.getFileName().toString() : nodeId;
        Path parent = normalized.getParent();
        String parentPath = parent != null ? parent.toString() : nodeId;
        String mimeType = folder ? null : probeMimeType(normalized);
        OffsetDateTime modifiedAt = attrs.lastModifiedTime().toInstant().atOffset(ZoneOffset.UTC);

        Set<String> readPrincipals = settings.readPrincipals();

        return new SourceNode(
                nodeId,
                settings.sourceId(),
                SOURCE_TYPE,
                name,
                folder ? nodeId : parentPath,
                mimeType,
                modifiedAt,
                folder,
                readPrincipals,
                Set.of(),
                // Deliberately empty. Core derives every generic source_* property from the fields above,
                // and a node's own properties are merged after core's, so anything repeated here would
                // overwrite core's version with this connector's.
                Map.of(),
                buildSecurityConfig(readPrincipals));
    }

    private static SecurityConfig buildSecurityConfig(Set<String> readPrincipals) {
        List<PermissionRule> rules = new ArrayList<>();
        if (readPrincipals != null) {
            for (String principal : readPrincipals) {
                if (principal == null || principal.isBlank()) {
                    continue;
                }
                String type = principal.startsWith("GROUP_")
                        || FileSystemConnectorSettings.EVERYONE.equals(principal) ? "group" : "user";
                rules.add(new PermissionRule(principal, type, principal, "READ"));
            }
        }
        // The filesystem exposes no inheritance model; the configured principals apply uniformly.
        return new SecurityConfig(true, rules);
    }

    private static String probeMimeType(Path path) {
        try {
            return Files.probeContentType(path);
        } catch (IOException e) {
            return null;
        }
    }
}
