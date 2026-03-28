package org.hyland.nuxeo.contentlake.live.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.hyland.nuxeo.contentlake.live.model.AuditCursor;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.OffsetDateTime;

import static org.assertj.core.api.Assertions.assertThat;

class FileAuditCursorStoreTest {

    @TempDir
    Path tempDir;

    @Test
    void saveAndLoad_roundTripsPerRepositoryKey() {
        FileAuditCursorStore store = new FileAuditCursorStore(
                tempDir.resolve("audit-cursor.json"),
                new ObjectMapper().registerModule(new JavaTimeModule())
        );

        AuditCursor first = new AuditCursor(OffsetDateTime.parse("2026-03-26T16:48:41.235Z"), 46);
        AuditCursor second = new AuditCursor(OffsetDateTime.parse("2026-03-26T16:48:45.939Z"), 48);

        store.save("nuxeo:local", first);
        store.save("nuxeo:other", second);

        assertThat(store.load("nuxeo:local")).contains(first);
        assertThat(store.load("nuxeo:other")).contains(second);
        assertThat(store.load("missing")).isEmpty();
    }
}
