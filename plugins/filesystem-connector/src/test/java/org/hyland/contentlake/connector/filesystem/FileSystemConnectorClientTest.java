package org.hyland.contentlake.connector.filesystem;

import org.hyland.contentlake.spi.SourceNode;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.core.io.Resource;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FileSystemConnectorClientTest {

    @TempDir
    Path root;

    private FileSystemConnectorClient client;

    @BeforeEach
    void setUp() {
        FileSystemConnectorSettings props = new FileSystemConnectorSettings(
                root.toString(), "fs-test", Set.of("__Everyone__"), List.of(), List.of(), 100);
        client = new FileSystemConnectorClient(props);
    }

    /**
     * The pipeline deletes whatever {@code downloadContent} returns once extraction finishes, so
     * returning the source path would delete the document that was just indexed. Every non-text MIME
     * type on this source goes down that path.
     */
    @Test
    void downloadContentReturnsATempCopyAndLeavesTheSourceFileInPlace() throws IOException {
        Path source = root.resolve("report.pdf");
        Files.writeString(source, "%PDF-1.4 report body", StandardCharsets.UTF_8);

        Resource downloaded = client.downloadContent(source.toString(), "report.pdf");
        Path temp = downloaded.getFile().toPath();

        assertThat(temp).isNotEqualTo(source);
        assertThat(temp).startsWith(Path.of(System.getProperty("java.io.tmpdir")).toRealPath());
        assertThat(Files.readString(temp)).isEqualTo("%PDF-1.4 report body");

        // Simulate the pipeline's cleanup: the source must survive it.
        Files.deleteIfExists(temp);
        assertThat(source).exists();
        assertThat(Files.readString(source)).isEqualTo("%PDF-1.4 report body");
    }

    @Test
    void downloadContentKeepsTheFileExtensionSoExtractorsCanSniffTheFormat() throws IOException {
        Path source = root.resolve("matrix.xlsx");
        Files.writeString(source, "sheet", StandardCharsets.UTF_8);

        Path temp = client.downloadContent(source.toString(), "matrix.xlsx").getFile().toPath();

        assertThat(temp.getFileName().toString()).endsWith(".xlsx");
        Files.deleteIfExists(temp);
    }

    @Test
    void downloadContentToleratesAnAwkwardFileName() throws IOException {
        Path source = root.resolve("weird name.pdf");
        Files.writeString(source, "body", StandardCharsets.UTF_8);

        Path temp = client.downloadContent(source.toString(), "../../weird name.pdf").getFile().toPath();

        // The guarantee that matters: nothing in a caller-supplied name walks out of the temp dir.
        assertThat(temp.getFileName().toString()).doesNotContain("/").doesNotContain("..");
        assertThat(temp).startsWith(Path.of(System.getProperty("java.io.tmpdir")).toRealPath());
        assertThat(Files.readString(temp)).isEqualTo("body");
        Files.deleteIfExists(temp);
    }

    @Test
    void getSourceTypeAndId() {
        assertThat(client.getSourceType()).isEqualTo("filesystem");
        assertThat(client.getSourceId()).isEqualTo("fs-test");
    }

    @Test
    void getNode_buildsFileSourceNodeWithSecurityConfig() throws IOException {
        Path file = root.resolve("report.txt");
        Files.writeString(file, "hello");

        SourceNode node = client.getNode(file.toAbsolutePath().normalize().toString());

        assertThat(node.folder()).isFalse();
        assertThat(node.name()).isEqualTo("report.txt");
        assertThat(node.sourceType()).isEqualTo("filesystem");
        assertThat(node.sourceId()).isEqualTo("fs-test");
        assertThat(node.readPrincipals()).containsExactly("__Everyone__");
        assertThat(node.security()).isNotNull();
        assertThat(node.security().permissions())
                .extracting(p -> p.identity())
                .containsExactly("__Everyone__");
        // Deliberately empty. Core derives every generic source_* property from the fields above, and a
        // node's own properties are merged after core's, so repeating them here would overwrite core's
        // values with this connector's. That is not hypothetical: the in-tree module this replaced
        // overwrote core's fixed-width source_modifiedAt with OffsetDateTime.toString(), which core
        // documents as the one rendering that does not sort against itself.
        assertThat(node.sourceProperties()).isEmpty();
    }

    @Test
    void getChildren_listsEntriesPaged() throws IOException {
        Files.writeString(root.resolve("a.txt"), "a");
        Files.writeString(root.resolve("b.txt"), "b");
        Files.createDirectory(root.resolve("sub"));

        List<SourceNode> firstPage = client.getChildren(root.toString(), 0, 2);
        List<SourceNode> secondPage = client.getChildren(root.toString(), 2, 2);

        assertThat(firstPage).hasSize(2);
        assertThat(secondPage).hasSize(1);
        assertThat(firstPage).extracting(SourceNode::name).contains("a.txt");
    }

    /**
     * A page shorter than {@code maxItems} is allowed, so callers page until they get an empty one. Pinned
     * here because the client is where the shortening happens: the window is applied before the entries are
     * converted, and a dropped entry shortens the page without ending the directory.
     */
    @Test
    void getChildren_returnsAnEmptyPageBeyondTheLastEntry() throws IOException {
        Files.writeString(root.resolve("a.txt"), "a");
        Files.writeString(root.resolve("b.txt"), "b");

        assertThat(client.getChildren(root.toString(), 2, 2)).isEmpty();
        assertThat(client.getChildren(root.toString(), 200, 2)).isEmpty();
    }

    /**
     * A dangling symlink has nothing to ingest and is the one failure that means "gone", so it is dropped and
     * the reconciliation sweep is left to act on it as a deletion.
     */
    @Test
    void getChildren_dropsAnEntryWhoseTargetIsGone() throws IOException {
        Files.writeString(root.resolve("real.txt"), "a");
        Path dangling = root.resolve("dangling.txt");
        assumeSymlinks(() -> Files.createSymbolicLink(dangling, root.resolve("never-existed.txt")));

        assertThat(client.getChildren(root.toString(), 0, 10))
                .extracting(SourceNode::name)
                .containsExactly("real.txt");
    }

    /**
     * The other half of that decision, and the one that protects the index. An entry that is present but
     * unreadable must not be silently dropped: a discovery pass would then report itself complete while
     * missing a document that is still at the source, and the sweep would delete it. A symlink loop is the
     * portable way to produce that state.
     */
    @Test
    void getChildren_failsOnAnEntryThatIsPresentButCannotBeRead() throws IOException {
        Path first = root.resolve("loop-a");
        Path second = root.resolve("loop-b");
        assumeSymlinks(() -> {
            Files.createSymbolicLink(first, second);
            Files.createSymbolicLink(second, first);
        });

        assertThatThrownBy(() -> client.getChildren(root.toString(), 0, 10))
                .isInstanceOf(UncheckedIOException.class)
                .hasMessageContaining("present but its attributes cannot be read");
    }

    /** Symlinks need a privilege on Windows, so the two cases above are skipped rather than failed there. */
    private static void assumeSymlinks(IoRunnable creation) {
        try {
            creation.run();
        } catch (IOException | UnsupportedOperationException e) {
            Assumptions.abort("This filesystem does not support symbolic links: " + e.getMessage());
        }
    }

    private interface IoRunnable {
        void run() throws IOException;
    }

    @Test
    void getContent_returnsFileBytes() throws IOException {
        Path file = root.resolve("data.txt");
        Files.writeString(file, "payload", StandardCharsets.UTF_8);

        byte[] content = client.getContent(file.toAbsolutePath().normalize().toString());

        assertThat(new String(content, StandardCharsets.UTF_8)).isEqualTo("payload");
    }
}
