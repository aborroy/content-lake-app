package org.hyland.filesystem.contentlake.batch.service;

import org.hyland.contentlake.spi.SourceNode;
import org.hyland.filesystem.contentlake.client.FileSystemSourceClient;
import org.hyland.filesystem.contentlake.config.FileSystemProperties;
import org.hyland.filesystem.contentlake.service.FileSystemScopeResolver;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The filesystem walk over a real directory (#136).
 *
 * <p>A real temp directory rather than a stubbed client, because the defect this covers lives in the
 * interaction between the two: the client applies the page window and then drops what it cannot map, and
 * the walker used to read the resulting short page as "directory exhausted".</p>
 */
class FileSystemDiscoveryServiceTest {

    @TempDir
    Path root;

    /**
     * A directory whose entry count is not a multiple of the page size used to end on the short final
     * page. That was right only by accident: the walker cannot tell a short page apart from the last one,
     * so it now pages until it gets an empty one.
     */
    @Test
    void walksADirectoryWhoseSizeIsNotAMultipleOfThePageSize() throws IOException {
        writeFiles(25);

        FileSystemDiscoveryService.FileSystemDiscovery discovery = discover(10);

        assertThat(discovery.nodes()).hasSize(25);
        assertThat(discovery.outcome().complete()).isTrue();
    }

    /** The tail of a large directory is the part a short page would have cost. */
    @Test
    void findsTheTailOfADirectoryLargerThanOnePage() throws IOException {
        writeFiles(150);

        FileSystemDiscoveryService.FileSystemDiscovery discovery = discover(100);

        assertThat(discovery.nodes()).hasSize(150);
        assertThat(discovery.nodes()).extracting(SourceNode::name).contains("doc-149.txt", "doc-000.txt");
    }

    /** Nested directories page independently, so the same bug in a subdirectory is a subdirectory short. */
    @Test
    void pagesEachDirectoryOfANestedTreeInFull() throws IOException {
        writeFiles(7);
        Path sub = Files.createDirectory(root.resolve("sub"));
        for (int i = 0; i < 7; i++) {
            Files.writeString(sub.resolve("nested-" + i + ".txt"), "x");
        }

        FileSystemDiscoveryService.FileSystemDiscovery discovery = discover(3);

        assertThat(discovery.nodes()).hasSize(14);
    }

    /** An empty directory is one listing and no entries, not a failure. */
    @Test
    void handlesAnEmptyRoot() {
        FileSystemDiscoveryService.FileSystemDiscovery discovery = discover(10);

        assertThat(discovery.nodes()).isEmpty();
        assertThat(discovery.outcome().complete()).isTrue();
    }

    /**
     * Out-of-scope entries are dropped by the walker, not by the client, so they never shorten a page.
     * Worth pinning: it is the reason scope filtering does not need the paging fix applied twice.
     */
    @Test
    void anOutOfScopeExtensionDoesNotTruncateTheDirectory() throws IOException {
        for (int i = 0; i < 12; i++) {
            Files.writeString(root.resolve("doc-" + i + (i % 3 == 0 ? ".bin" : ".txt")), "x");
        }

        FileSystemProperties properties = properties(5);
        properties.setIncludeExtensions(List.of("txt"));

        assertThat(service(properties).discoverTallied().nodes()).hasSize(8);
    }

    private void writeFiles(int count) throws IOException {
        for (int i = 0; i < count; i++) {
            Files.writeString(root.resolve(String.format("doc-%03d.txt", i)), "content " + i);
        }
    }

    private FileSystemDiscoveryService.FileSystemDiscovery discover(int pageSize) {
        return service(properties(pageSize)).discoverTallied();
    }

    private FileSystemProperties properties(int pageSize) {
        FileSystemProperties properties = new FileSystemProperties();
        properties.setRootPath(root.toString());
        properties.setPageSize(pageSize);
        return properties;
    }

    private static FileSystemDiscoveryService service(FileSystemProperties properties) {
        return new FileSystemDiscoveryService(
                new FileSystemSourceClient(properties),
                new FileSystemScopeResolver(properties),
                properties);
    }
}
