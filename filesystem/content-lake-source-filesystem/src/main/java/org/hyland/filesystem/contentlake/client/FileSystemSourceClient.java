package org.hyland.filesystem.contentlake.client;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.hyland.contentlake.model.ContentLakeIngestProperties;
import org.hyland.contentlake.spi.ConnectorSchema;
import org.hyland.contentlake.spi.ContentSourceClient;
import org.hyland.contentlake.spi.PermissionRule;
import org.hyland.contentlake.spi.SecurityConfig;
import org.hyland.contentlake.spi.SourceNode;
import org.hyland.filesystem.contentlake.config.FileSystemProperties;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;

/**
 * {@link ContentSourceClient} over a local or mounted filesystem directory.
 *
 * <p>Node ids are absolute, normalized path strings. Text extraction is source-agnostic
 * ({@code TikaTextExtractor}), so no server-side transform service is required. The filesystem has no
 * native ACL model, so every node is stamped with the configured read principals.</p>
 */
@Slf4j
@RequiredArgsConstructor
public class FileSystemSourceClient implements ContentSourceClient {

    private final FileSystemProperties properties;

    @Override
    public String getSourceType() {
        return "filesystem";
    }

    @Override
    public String getSourceId() {
        return properties.getSourceId();
    }

    /**
     * What this connector needs (#123).
     *
     * <p>{@code root-path} is validated as a directory that has to exist, which is the one startup check
     * here that has real teeth: a filesystem ingester pointed at an unmounted path reports zero documents
     * and looks like an empty source rather than a misconfiguration.</p>
     *
     * <p>{@code read-principals} is listed because the filesystem has no ACL model of its own, so this is
     * the only thing deciding who can retrieve the ingested content. It is optional and defaults to
     * everyone, which is worth an operator seeing stated.</p>
     */
    @Override
    public ConnectorSchema connectorSchema() {
        return ConnectorSchema.builder(getSourceType())
                .required("filesystem.root-path", ConnectorSchema.FieldType.DIRECTORY,
                        "Absolute directory to ingest from, a local path or a mounted volume")
                .optional("filesystem.source-id", ConnectorSchema.FieldType.STRING,
                        "Source alias stored as the second half of cin_sourceId")
                .optional("filesystem.read-principals", ConnectorSchema.FieldType.LIST,
                        "Principals granted read access to every ingested file; defaults to everyone")
                .optional("filesystem.include-extensions", ConnectorSchema.FieldType.LIST,
                        "Extensions to ingest, without the dot; empty means every file")
                .optional("filesystem.exclude-patterns", ConnectorSchema.FieldType.LIST,
                        "Path fragments that exclude a file or directory, e.g. .git")
                .optional("filesystem.page-size", ConnectorSchema.FieldType.INTEGER,
                        "Entries fetched per directory listing")
                .build();
    }

    /** The configured root as an absolute node id, for discovery to start from. */
    @Override
    public String getRootNodeId() {
        return Path.of(properties.getRootPath()).toAbsolutePath().normalize().toString();
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
     * <p>The {@link org.hyland.contentlake.spi.ContentSourceClient#downloadContent} contract makes the
     * caller responsible for deleting what it gets back, and the shared pipeline does exactly that
     * after extraction. Returning the source path would therefore delete the very document just
     * indexed. For a repository-backed source the distinction is invisible because the download is
     * always a copy; here the source is a local file, so the copy has to be explicit.</p>
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
        // Separators become underscores and dot runs collapse, so nothing in a caller-supplied name
        // can walk out of the temp directory or produce a hidden file.
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

    private SourceNode toSourceNodeQuietly(Path path) {
        try {
            return toSourceNode(path, Files.readAttributes(path, BasicFileAttributes.class));
        } catch (IOException e) {
            log.warn("Skipping unreadable filesystem entry {}: {}", path, e.getMessage());
            return null;
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

        Set<String> readPrincipals = properties.getReadPrincipals();

        Map<String, Object> props = new LinkedHashMap<>();
        props.put(ContentLakeIngestProperties.SOURCE_NODE_ID, nodeId);
        props.put(ContentLakeIngestProperties.SOURCE_TYPE, "filesystem");
        props.put(ContentLakeIngestProperties.SOURCE_NAME, name);
        props.put(ContentLakeIngestProperties.SOURCE_PATH, folder ? nodeId : parentPath);
        props.put(ContentLakeIngestProperties.SOURCE_MIME_TYPE, mimeType);
        props.put(ContentLakeIngestProperties.SOURCE_MODIFIED_AT, modifiedAt.toString());
        props.values().removeIf(Objects::isNull);

        return new SourceNode(
                nodeId,
                properties.getSourceId(),
                "filesystem",
                name,
                folder ? nodeId : parentPath,
                mimeType,
                modifiedAt,
                folder,
                readPrincipals,
                Set.of(),
                props,
                buildSecurityConfig(readPrincipals));
    }

    private static SecurityConfig buildSecurityConfig(Set<String> readPrincipals) {
        List<PermissionRule> rules = new ArrayList<>();
        if (readPrincipals != null) {
            for (String principal : readPrincipals) {
                if (principal == null || principal.isBlank()) {
                    continue;
                }
                String type = principal.startsWith("GROUP_") || "__Everyone__".equals(principal) ? "group" : "user";
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
