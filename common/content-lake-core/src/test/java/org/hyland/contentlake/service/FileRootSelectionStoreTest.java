package org.hyland.contentlake.service;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** The store contract over the file implementation, plus what the file itself has to look like. */
class FileRootSelectionStoreTest extends RootSelectionStoreContractTest {

    @TempDir
    Path directory;

    private RootSelectionStore store;

    @Override
    RootSelectionStore store() {
        if (store == null) {
            store = new FileRootSelectionStore(selectionFile());
        }
        return store;
    }

    private Path selectionFile() {
        return directory.resolve("state/connector-roots.json");
    }

    @Test
    void createsTheParentDirectoryOnFirstSave() {
        store().save(SOURCE, RootSelection.of(List.of("root-a"), "admin"));

        assertThat(selectionFile()).exists();
    }

    @Test
    void writesTimestampsAsReadableIsoStrings() throws IOException {
        // An operator opening this file is usually answering "when did the scope change, and who changed it".
        // Epoch decimals answer neither without a conversion.
        store().save(SOURCE, RootSelection.of(List.of("root-a"), "admin"));

        String written = Files.readString(selectionFile());
        assertThat(written).contains("\"updatedBy\":\"admin\"");
        assertThat(written).containsPattern("\"updatedAt\":\"\\d{4}-\\d{2}-\\d{2}T");
    }

    @Test
    void survivesARestart() {
        store().save(SOURCE, RootSelection.of(List.of("root-a", "root-b"), "admin"));

        // A second store over the same file is what a restarted container has.
        RootSelectionStore reopened = new FileRootSelectionStore(selectionFile());

        assertThat(reopened.load(SOURCE)).get().extracting(RootSelection::rootNodeIds)
                .isEqualTo(List.of("root-a", "root-b"));
    }

    @Test
    void leavesNoTemporaryFileBehind() {
        store().save(SOURCE, RootSelection.of(List.of("root-a"), "admin"));

        assertThat(selectionFile().resolveSibling(selectionFile().getFileName() + ".tmp")).doesNotExist();
    }

    @Test
    void refusesToGuessAtAnUnreadableFile() throws IOException {
        // Reading a corrupt file as "no selection" would fall through to configured roots and silently widen
        // the next pass to the whole source, which is the one failure mode worth refusing over.
        Files.createDirectories(selectionFile().getParent());
        Files.writeString(selectionFile(), "{ this is not json");

        assertThatThrownBy(() -> store().load(SOURCE))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("root selection file");
    }
}
