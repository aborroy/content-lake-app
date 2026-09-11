package org.hyland.example.connector.directory;

import org.hyland.contentlake.spi.ContentSourceClient;
import org.hyland.contentlake.spi.SourceNode;
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
 * Reads content and metadata from a directory. Node ids are absolute paths.
 *
 * <p>Four methods carry the pipeline: {@link #getNode}, {@link #getChildren}, {@link #downloadContent} and
 * {@link #getContent}. {@link #getRootNodeId()} is the fifth worth implementing, and is what lets a host
 * start a batch pass without being told where.</p>
 *
 * <p>Two things this example is careful about, because both are easy to get wrong in a real connector:</p>
 * <ul>
 *   <li><strong>{@link #downloadContent} returns a copy.</strong> The SPI makes the caller responsible for
 *       deleting what it gets, and the pipeline does exactly that after extraction. Returning the source
 *       file would delete the document that was just indexed. Invisible for a repository-backed source,
 *       where a download is always a copy; here it has to be explicit.</li>
 *   <li><strong>{@link SourceNode#modifiedAt()} is real.</strong> It is what lets a re-sync skip unchanged
 *       content. A null, or a timestamp that always moves, makes every sync re-extract and re-embed
 *       everything.</li>
 * </ul>
 */
public class DirectoryConnectorClient implements ContentSourceClient {

    private final Path root;
    private final String sourceId;
    private final Set<String> readPrincipals;
    private final int pageSize;

    public DirectoryConnectorClient(String rootPath, String sourceId, Set<String> readPrincipals, int pageSize) {
        if (rootPath == null || rootPath.isBlank()) {
            throw new IllegalArgumentException(DirectoryConnectorPlugin.ROOT_PATH_SETTING + " is required");
        }
        this.root = Path.of(rootPath).toAbsolutePath().normalize();
        this.sourceId = sourceId;
        this.readPrincipals = Set.copyOf(readPrincipals);
        this.pageSize = pageSize;
    }

    @Override
    public String getSourceId() {
        return sourceId;
    }

    @Override
    public String getSourceType() {
        return DirectoryConnectorPlugin.SOURCE_TYPE;
    }

    /** Where a batch pass starts, so the ingester needs no {@code connector.roots} setting. */
    @Override
    public String getRootNodeId() {
        return root.toString();
    }

    @Override
    public SourceNode getNode(String nodeId) {
        Path path = resolveInsideRoot(nodeId);
        if (!Files.exists(path)) {
            // Null means "gone", which the pipeline treats as a deletion rather than an error.
            return null;
        }
        try {
            return toSourceNode(path, Files.readAttributes(path, BasicFileAttributes.class));
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot read " + nodeId, e);
        }
    }

    @Override
    public List<SourceNode> getChildren(String containerId, int skip, int maxItems) {
        Path directory = resolveInsideRoot(containerId);
        if (!Files.isDirectory(directory)) {
            return List.of();
        }
        int limit = maxItems > 0 ? maxItems : pageSize;
        try (Stream<Path> entries = Files.list(directory)) {
            List<SourceNode> page = new ArrayList<>();
            entries.sorted().skip(Math.max(0, skip)).limit(limit).forEach(entry -> {
                try {
                    page.add(toSourceNode(entry, Files.readAttributes(entry, BasicFileAttributes.class)));
                } catch (IOException e) {
                    // One unreadable entry is not worth failing the listing, and therefore the subtree,
                    // over. The host reports an incomplete pass only when a whole listing fails.
                    page.add(null);
                }
            });
            page.removeIf(Objects::isNull);
            return page;
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot list " + containerId, e);
        }
    }

    @Override
    public Resource downloadContent(String nodeId, String fileName) {
        Path source = resolveInsideRoot(nodeId);
        try {
            Path temp = Files.createTempFile("sample-directory-", "-" + safeSuffix(fileName, source));
            Files.copy(source, temp, StandardCopyOption.REPLACE_EXISTING);
            return new FileSystemResource(temp);
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot copy " + nodeId + " for extraction", e);
        }
    }

    @Override
    public byte[] getContent(String nodeId) {
        try {
            return Files.readAllBytes(resolveInsideRoot(nodeId));
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot read content of " + nodeId, e);
        }
    }

    /**
     * Node ids come back to the connector from the host, and after a round trip through the index they are
     * just strings. Confining them to the configured root keeps a stored id from reaching a file the
     * deployment never mounted.
     */
    private Path resolveInsideRoot(String nodeId) {
        Path candidate = Path.of(nodeId).toAbsolutePath().normalize();
        if (!candidate.startsWith(root)) {
            throw new IllegalArgumentException("Node id " + nodeId + " is outside " + root);
        }
        return candidate;
    }

    private SourceNode toSourceNode(Path path, BasicFileAttributes attributes) {
        Path normalized = path.toAbsolutePath().normalize();
        boolean folder = attributes.isDirectory();
        String name = normalized.getFileName() != null
                ? normalized.getFileName().toString()
                : normalized.toString();
        Path parent = normalized.getParent();
        String containerPath = folder
                ? normalized.toString()
                : (parent != null ? parent.toString() : normalized.toString());

        Map<String, Object> properties = new LinkedHashMap<>();
        properties.put("sample_directory_absolutePath", normalized.toString());
        properties.put("sample_directory_sizeBytes", folder ? 0L : attributes.size());

        return new SourceNode(
                normalized.toString(),
                sourceId,
                DirectoryConnectorPlugin.SOURCE_TYPE,
                name,
                containerPath,
                folder ? null : probeMimeType(normalized),
                attributes.lastModifiedTime().toInstant().atOffset(ZoneOffset.UTC),
                folder,
                readPrincipals,
                Set.of(),
                properties);
    }

    /** Keeps the extension, which extractors use to sniff the format. */
    private static String safeSuffix(String fileName, Path source) {
        String name = (fileName == null || fileName.isBlank())
                ? String.valueOf(source.getFileName())
                : fileName;
        String sanitised = name.replaceAll("[^A-Za-z0-9._-]", "_").replaceAll("\\.{2,}", "_");
        return sanitised.isBlank() ? "content" : sanitised;
    }

    private static String probeMimeType(Path path) {
        try {
            String probed = Files.probeContentType(path);
            return probed != null ? probed : "application/octet-stream";
        } catch (IOException e) {
            return "application/octet-stream";
        }
    }
}
