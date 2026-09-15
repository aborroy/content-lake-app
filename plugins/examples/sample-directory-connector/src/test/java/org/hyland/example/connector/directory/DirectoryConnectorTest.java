package org.hyland.example.connector.directory;

import org.hyland.contentlake.spi.ConnectorContext;
import org.hyland.contentlake.spi.ConnectorSchema;
import org.hyland.contentlake.spi.ContentSourceClient;
import org.hyland.contentlake.spi.SourceNode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.core.io.Resource;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * What a connector's own tests are worth checking: that the schema matches what the client reads, that
 * listings page, and that the two easy mistakes (deleting the source file, losing the modification time)
 * are not present.
 */
class DirectoryConnectorTest {

    private final DirectoryConnectorPlugin plugin = new DirectoryConnectorPlugin();

    @Test
    void theSchemaDeclaresEverySettingTheClientReads() {
        assertThat(plugin.schema().fields()).extracting(ConnectorSchema.Field::name)
                .containsExactlyInAnyOrderElementsOf(DirectoryConnectorPlugin.settingNames());
    }

    /** An unmounted volume must fail startup, not read as an empty source. */
    @Test
    void theRootPathIsTheOneRequiredSettingAndIsValidatedAsADirectory() {
        assertThat(plugin.schema().fields()).filteredOn(ConnectorSchema.Field::required)
                .extracting(ConnectorSchema.Field::name)
                .containsExactly(DirectoryConnectorPlugin.ROOT_PATH_SETTING);
        assertThat(plugin.schema().fields().getFirst().type())
                .isEqualTo(ConnectorSchema.FieldType.DIRECTORY);
    }

    @Test
    void theSchemaValidatesAConfiguredDirectory(@TempDir Path root) {
        assertThat(plugin.schema().validate(context(root)::property)).isEmpty();
    }

    @Test
    void theSchemaReportsAMissingRootPath() {
        ConnectorContext empty = name -> null;

        assertThat(plugin.schema().validate(empty::property))
                .anySatisfy(problem -> assertThat(problem)
                        .contains(DirectoryConnectorPlugin.ROOT_PATH_SETTING));
    }

    @Test
    void namesItsRootSoTheHostNeedsNoRootsSetting(@TempDir Path root) {
        ContentSourceClient client = plugin.createClient(context(root));

        // Absolute and normalized, not resolved through symlinks: node ids have to be comparable to the
        // ids getChildren produces, and those are built the same way.
        assertThat(client.getRootNodeId()).isEqualTo(root.toAbsolutePath().normalize().toString());
    }

    @Test
    void listsFilesAndFoldersWithMimeTypesAndTimestamps(@TempDir Path root) throws IOException {
        Files.writeString(root.resolve("a.txt"), "alpha");
        Files.createDirectory(root.resolve("sub"));

        ContentSourceClient client = plugin.createClient(context(root));
        List<SourceNode> children = client.getChildren(client.getRootNodeId(), 0, 10);

        assertThat(children).extracting(SourceNode::name).containsExactly("a.txt", "sub");
        SourceNode file = children.getFirst();
        assertThat(file.folder()).isFalse();
        assertThat(file.mimeType()).isEqualTo("text/plain");
        // Without this a re-sync re-extracts and re-embeds everything.
        assertThat(file.modifiedAt()).isNotNull();
        assertThat(file.readPrincipals()).containsExactly("__Everyone__");
        assertThat(children.getLast().folder()).isTrue();
    }

    @Test
    void pagesThroughADirectory(@TempDir Path root) throws IOException {
        for (String name : List.of("a.txt", "b.txt", "c.txt")) {
            Files.writeString(root.resolve(name), name);
        }
        ContentSourceClient client = plugin.createClient(context(root));

        assertThat(client.getChildren(client.getRootNodeId(), 0, 2))
                .extracting(SourceNode::name).containsExactly("a.txt", "b.txt");
        assertThat(client.getChildren(client.getRootNodeId(), 2, 2))
                .extracting(SourceNode::name).containsExactly("c.txt");
    }

    /** The pipeline deletes what downloadContent returns, so it must never be the source file. */
    @Test
    void downloadContentReturnsACopyThatCanBeDeletedSafely(@TempDir Path root) throws IOException {
        Path source = Files.writeString(root.resolve("a.txt"), "alpha");
        ContentSourceClient client = plugin.createClient(context(root));

        Resource downloaded = client.downloadContent(source.toString(), "a.txt");
        Path temp = downloaded.getFile().toPath();

        assertThat(temp).isNotEqualTo(source);
        assertThat(Files.readString(temp)).isEqualTo("alpha");
        Files.delete(temp);
        assertThat(source).exists();
    }

    @Test
    void aMissingNodeIsReportedAsGoneRatherThanAsAnError(@TempDir Path root) {
        ContentSourceClient client = plugin.createClient(context(root));

        assertThat(client.getNode(root.resolve("never-existed.txt").toString())).isNull();
    }

    /** A node id survives a round trip through the index, so it cannot be trusted to stay in scope. */
    @Test
    void refusesANodeIdOutsideTheConfiguredRoot(@TempDir Path root) {
        ContentSourceClient client = plugin.createClient(context(root));

        assertThatThrownBy(() -> client.getContent("/etc/passwd"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("outside");
    }

    @Test
    void requiresARootPath() {
        assertThatThrownBy(() -> plugin.createClient(name -> null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(DirectoryConnectorPlugin.ROOT_PATH_SETTING);
    }

    private static ConnectorContext context(Path root) {
        Map<String, String> values = new LinkedHashMap<>();
        values.put(DirectoryConnectorPlugin.ROOT_PATH_SETTING, root.toString());
        values.put(DirectoryConnectorPlugin.READ_PRINCIPALS_SETTING, "__Everyone__");
        return values::get;
    }
}
