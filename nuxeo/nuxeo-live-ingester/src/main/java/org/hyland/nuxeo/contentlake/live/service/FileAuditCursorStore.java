package org.hyland.nuxeo.contentlake.live.service;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.hyland.nuxeo.contentlake.live.model.AuditCursor;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

public class FileAuditCursorStore implements AuditCursorStore {

    /**
     * The cursor file's own mapper, owned here rather than injected.
     *
     * <p>{@link AuditCursor#lastLogDate()} is an {@link java.time.OffsetDateTime}, which Jackson
     * serialises only with {@link JavaTimeModule} registered. Taking the mapper from the application
     * context made that a property of whichever bean happened to be injected: the shared Jackson 2
     * bean carries no modules, and Jackson's {@code REQUIRE_HANDLERS_FOR_JAVA8_TIMES} turns the gap
     * into a hard failure, so every cycle with a cursor to save died and the position never advanced.
     * Owning the mapper makes the format a property of this class, and the tests exercise the same
     * one production uses.</p>
     *
     * <p>Timestamps are written as ISO-8601 strings rather than epoch decimals so the file can be read
     * by an operator diagnosing where the ingester resumed from.</p>
     */
    private static final ObjectMapper OBJECT_MAPPER = JsonMapper.builder()
            .addModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
            .build();

    private final Path cursorFile;

    public FileAuditCursorStore(Path cursorFile) {
        this.cursorFile = cursorFile;
    }

    @Override
    public synchronized Optional<AuditCursor> load(String repositoryKey) {
        return Optional.ofNullable(readState().cursors().get(repositoryKey));
    }

    @Override
    public synchronized void save(String repositoryKey, AuditCursor cursor) {
        CursorState state = readState();
        Map<String, AuditCursor> updated = new LinkedHashMap<>(state.cursors());
        updated.put(repositoryKey, cursor);
        writeState(new CursorState(updated));
    }

    private CursorState readState() {
        if (Files.notExists(cursorFile)) {
            return CursorState.empty();
        }
        try {
            CursorState state = OBJECT_MAPPER.readValue(cursorFile.toFile(), CursorState.class);
            return state != null ? state : CursorState.empty();
        } catch (IOException e) {
            throw new IllegalStateException("Failed to read audit cursor file " + cursorFile, e);
        }
    }

    private void writeState(CursorState state) {
        try {
            Path parent = cursorFile.getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
            Path tempFile = cursorFile.resolveSibling(cursorFile.getFileName() + ".tmp");
            OBJECT_MAPPER.writeValue(tempFile.toFile(), new CursorState(new TreeMap<>(state.cursors())));
            try {
                Files.move(tempFile, cursorFile,
                        StandardCopyOption.REPLACE_EXISTING,
                        StandardCopyOption.ATOMIC_MOVE);
            } catch (IOException atomicMoveFailure) {
                Files.move(tempFile, cursorFile, StandardCopyOption.REPLACE_EXISTING);
            }
        } catch (IOException e) {
            throw new IllegalStateException("Failed to write audit cursor file " + cursorFile, e);
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    private record CursorState(Map<String, AuditCursor> cursors) {

        private CursorState {
            cursors = cursors == null ? Map.of() : Map.copyOf(cursors);
        }

        static CursorState empty() {
            return new CursorState(Map.of());
        }
    }
}
