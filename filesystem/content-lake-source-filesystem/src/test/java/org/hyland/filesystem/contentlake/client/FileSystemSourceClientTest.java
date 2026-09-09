package org.hyland.filesystem.contentlake.client;

import org.hyland.contentlake.spi.SourceNode;
import org.hyland.filesystem.contentlake.config.FileSystemProperties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.core.io.Resource;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class FileSystemSourceClientTest {

    @TempDir
    Path root;

    private FileSystemSourceClient client;

    @BeforeEach
    void setUp() {
        FileSystemProperties props = new FileSystemProperties();
        props.setRootPath(root.toString());
        props.setSourceId("fs-test");
        props.setReadPrincipals(Set.of("__Everyone__"));
        client = new FileSystemSourceClient(props);
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
        assertThat(node.sourceProperties()).containsEntry("source_type", "filesystem");
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

    @Test
    void getContent_returnsFileBytes() throws IOException {
        Path file = root.resolve("data.txt");
        Files.writeString(file, "payload", StandardCharsets.UTF_8);

        byte[] content = client.getContent(file.toAbsolutePath().normalize().toString());

        assertThat(new String(content, StandardCharsets.UTF_8)).isEqualTo("payload");
    }
}
