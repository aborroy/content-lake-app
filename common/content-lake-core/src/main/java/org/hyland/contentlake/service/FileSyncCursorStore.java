package org.hyland.contentlake.service;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

/**
 * Keeps cursors in a JSON file, for a deployment that has a writable mount.
 *
 * <p>Not the default: the shipped compose stack mounts the connector host's volumes read-only, so a
 * file-backed store there fails on the first save. {@link HxprSyncCursorStore} needs no mount and is
 * the default for that reason.</p>
 */
public class FileSyncCursorStore implements SyncCursorStore {

    /**
     * The cursor file's own mapper, owned here rather than injected.
     *
     * <p>{@link SyncCursor#updatedAt()} is an {@link java.time.OffsetDateTime}, which Jackson
     * serialises only with {@link JavaTimeModule} registered. Taking the mapper from the application
     * context makes that a property of whichever bean happens to be injected: the shared Jackson bean
     * carries no modules, and Jackson's {@code REQUIRE_HANDLERS_FOR_JAVA8_TIMES} turns the gap into a
     * hard failure, so every pass with a cursor to save dies and the position never advances. Owning
     * the mapper makes the format a property of this class, and the tests exercise the same one
     * production uses.</p>
     *
     * <p>Timestamps are written as ISO-8601 strings rather than epoch decimals so the file can be read
     * by an operator diagnosing where a sync resumed from.</p>
     */
    private static final ObjectMapper OBJECT_MAPPER = JsonMapper.builder()
            .addModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
            .build();

    private final Path cursorFile;

    public FileSyncCursorStore(Path cursorFile) {
        this.cursorFile = cursorFile;
    }

    @Override
    public synchronized Optional<SyncCursor> load(String qualifiedSourceId) {
        return Optional.ofNullable(readState().cursors().get(qualifiedSourceId));
    }

    @Override
    public synchronized void save(String qualifiedSourceId, SyncCursor cursor) {
        Map<String, SyncCursor> updated = new LinkedHashMap<>(readState().cursors());
        updated.put(qualifiedSourceId, cursor);
        writeState(new CursorState(updated));
    }

    @Override
    public synchronized void clear(String qualifiedSourceId) {
        Map<String, SyncCursor> updated = new LinkedHashMap<>(readState().cursors());
        if (updated.remove(qualifiedSourceId) == null) {
            return;
        }
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
            throw new IllegalStateException("Failed to read sync cursor file " + cursorFile, e);
        }
    }

    /**
     * Writes through a temporary file and moves it into place, so a process killed mid-write leaves the
     * previous cursor rather than a truncated one. The non-atomic fallback covers filesystems that
     * cannot promise an atomic move; a torn file there is still better than no store at all.
     */
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
            throw new IllegalStateException("Failed to write sync cursor file " + cursorFile, e);
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    private record CursorState(Map<String, SyncCursor> cursors) {

        private CursorState {
            cursors = cursors == null ? Map.of() : Map.copyOf(cursors);
        }

        static CursorState empty() {
            return new CursorState(Map.of());
        }
    }
}
