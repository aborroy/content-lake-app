package org.hyland.contentlake.service;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** The store contract over the file implementation, plus what the file itself has to look like. */
class FileSyncCursorStoreTest extends SyncCursorStoreContractTest {

    @TempDir
    Path directory;

    private SyncCursorStore store;

    @Override
    SyncCursorStore store() {
        if (store == null) {
            store = new FileSyncCursorStore(cursorFile());
        }
        return store;
    }

    private Path cursorFile() {
        return directory.resolve("state/connector-cursor.json");
    }

    /** A missing parent directory is created rather than reported: the mount may start out empty. */
    @Test
    void createsTheParentDirectoryOnFirstSave() {
        store().save(SOURCE, SyncCursor.seeded("token-1"));

        assertThat(cursorFile()).exists();
    }

    /**
     * The timestamp is an {@code OffsetDateTime}, which Jackson serialises only with the JSR-310 module
     * registered, so the store owns its mapper. Worth asserting because the failure mode is not a wrong
     * value but an exception on every save, which leaves the cursor frozen at its first position.
     */
    @Test
    void writesTimestampsAsReadableIsoStrings() throws IOException {
        store().save(SOURCE, SyncCursor.seeded("token-1"));

        String json = Files.readString(cursorFile());
        assertThat(json).contains("\"value\":\"token-1\"");
        assertThat(json).containsPattern("\"updatedAt\":\"\\d{4}-\\d{2}-\\d{2}T");
    }

    /** A new store over the same file resumes where the previous process left off. */
    @Test
    void survivesARestart() {
        store().save(SOURCE, SyncCursor.seeded("token-1"));

        assertThat(new FileSyncCursorStore(cursorFile()).load(SOURCE))
                .map(SyncCursor::value)
                .contains("token-1");
    }

    /** The temporary file is a write mechanism, not state: a completed save leaves only the real file. */
    @Test
    void leavesNoTemporaryFileBehind() {
        store().save(SOURCE, SyncCursor.seeded("token-1"));

        assertThat(directory.resolve("state/connector-cursor.json.tmp")).doesNotExist();
    }

    /**
     * A corrupt file fails loudly. Treating it as "no cursor" would silently turn every subsequent run
     * into a full walk, which looks like the feature not working rather than a file to fix.
     */
    @Test
    void refusesToGuessAtAnUnreadableFile() throws IOException {
        Files.createDirectories(cursorFile().getParent());
        Files.writeString(cursorFile(), "{not json");

        assertThatThrownBy(() -> store().load(SOURCE))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("connector-cursor.json");
    }
}
