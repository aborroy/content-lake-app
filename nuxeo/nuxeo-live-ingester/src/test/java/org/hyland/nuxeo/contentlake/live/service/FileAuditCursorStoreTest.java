package org.hyland.nuxeo.contentlake.live.service;

import org.hyland.nuxeo.contentlake.live.model.AuditCursor;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.OffsetDateTime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

class FileAuditCursorStoreTest {

    @TempDir
    Path tempDir;

    @Test
    void saveAndLoad_roundTripsPerRepositoryKey() {
        FileAuditCursorStore store = new FileAuditCursorStore(tempDir.resolve("audit-cursor.json"));

        AuditCursor first = new AuditCursor(OffsetDateTime.parse("2026-03-26T16:48:41.235Z"), 46);
        AuditCursor second = new AuditCursor(OffsetDateTime.parse("2026-03-26T16:48:45.939Z"), 48);

        store.save("nuxeo:local", first);
        store.save("nuxeo:other", second);

        assertThat(store.load("nuxeo:local")).contains(first);
        assertThat(store.load("nuxeo:other")).contains(second);
        assertThat(store.load("missing")).isEmpty();
    }

    /**
     * The regression #126 was: the store took its mapper from the application context, the shared
     * Jackson 2 bean carried no {@code JavaTimeModule}, and every save of an {@code OffsetDateTime}
     * cursor threw, so the position never advanced. The store now owns its mapper, so a caller cannot
     * hand it one that cannot write the cursor -- this test asserts that a save with no mapper
     * configuration whatsoever succeeds and lands on disk.
     */
    @Test
    void save_writesTheCursorWithoutMapperConfigurationFromTheCaller() throws Exception {
        Path cursorFile = tempDir.resolve("audit-cursor.json");
        FileAuditCursorStore store = new FileAuditCursorStore(cursorFile);

        assertThatCode(() -> store.save("nuxeo:local",
                new AuditCursor(OffsetDateTime.parse("2026-03-26T16:48:41.235Z"), 46)))
                .doesNotThrowAnyException();

        assertThat(cursorFile).exists();
        // ISO-8601 rather than an epoch decimal, so the file is readable by whoever is diagnosing a
        // resume position.
        assertThat(Files.readString(cursorFile)).contains("2026-03-26T16:48:41.235Z");
    }

    /** A restart is a fresh store over the same file, which must resume from what was written. */
    @Test
    void aCursorSurvivesARestart() {
        Path cursorFile = tempDir.resolve("audit-cursor.json");
        AuditCursor cursor = new AuditCursor(OffsetDateTime.parse("2026-03-26T16:48:41.235Z"), 46);

        new FileAuditCursorStore(cursorFile).save("nuxeo:local", cursor);

        assertThat(new FileAuditCursorStore(cursorFile).load("nuxeo:local")).contains(cursor);
    }

    /**
     * A cursor file written before the ISO-8601 switch holds an epoch decimal. Reading one must still
     * work, or an upgrade would fail on the first load rather than merely re-reading from the start.
     */
    @Test
    void anEpochTimestampWrittenByAnEarlierBuildStillLoads() throws Exception {
        Path cursorFile = tempDir.resolve("audit-cursor.json");
        Files.writeString(cursorFile,
                "{\"cursors\":{\"nuxeo:local\":{\"lastLogDate\":1774543721.235000000,\"lastEntryId\":46}}}");

        assertThat(new FileAuditCursorStore(cursorFile).load("nuxeo:local"))
                .map(AuditCursor::lastEntryId)
                .contains(46L);
    }
}
