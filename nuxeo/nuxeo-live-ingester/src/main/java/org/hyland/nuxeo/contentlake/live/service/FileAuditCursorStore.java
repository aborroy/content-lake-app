package org.hyland.nuxeo.contentlake.live.service;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
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

    private final Path cursorFile;
    private final ObjectMapper objectMapper;

    public FileAuditCursorStore(Path cursorFile, ObjectMapper objectMapper) {
        this.cursorFile = cursorFile;
        this.objectMapper = objectMapper.copy();
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
            CursorState state = objectMapper.readValue(cursorFile.toFile(), CursorState.class);
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
            objectMapper.writeValue(tempFile.toFile(), new CursorState(new TreeMap<>(state.cursors())));
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
