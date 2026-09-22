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
 * Root selections in a JSON file, for a deployment with a writable mount.
 *
 * <p>Mirrors {@link FileSyncCursorStore}, including why the mapper is owned here rather than injected:
 * {@link RootSelection#updatedAt()} is an {@link java.time.OffsetDateTime}, which Jackson serialises only with
 * {@link JavaTimeModule} registered. The shared Jackson bean carries no modules and Jackson's
 * {@code REQUIRE_HANDLERS_FOR_JAVA8_TIMES} turns that gap into a hard failure, so taking the mapper from the
 * context would make every write die on a detail of whichever bean was injected. Owning it makes the format a
 * property of this class, and the tests then exercise the one production uses.</p>
 *
 * <p>Timestamps are ISO-8601 strings rather than epoch decimals, because an operator reading this file is
 * usually answering "when did the scope change, and who changed it".</p>
 */
public class FileRootSelectionStore implements RootSelectionStore {

    private static final ObjectMapper OBJECT_MAPPER = JsonMapper.builder()
            .addModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
            .build();

    private final Path selectionFile;

    public FileRootSelectionStore(Path selectionFile) {
        this.selectionFile = selectionFile;
    }

    @Override
    public synchronized Optional<RootSelection> load(String qualifiedSourceId) {
        return Optional.ofNullable(readState().selections().get(qualifiedSourceId));
    }

    @Override
    public synchronized void save(String qualifiedSourceId, RootSelection selection) {
        Map<String, RootSelection> updated = new LinkedHashMap<>(readState().selections());
        updated.put(qualifiedSourceId, selection);
        writeState(new SelectionState(updated));
    }

    @Override
    public synchronized void clear(String qualifiedSourceId) {
        Map<String, RootSelection> updated = new LinkedHashMap<>(readState().selections());
        if (updated.remove(qualifiedSourceId) == null) {
            return;
        }
        writeState(new SelectionState(updated));
    }

    private SelectionState readState() {
        if (Files.notExists(selectionFile)) {
            return SelectionState.empty();
        }
        try {
            SelectionState state = OBJECT_MAPPER.readValue(selectionFile.toFile(), SelectionState.class);
            return state != null ? state : SelectionState.empty();
        } catch (IOException e) {
            // Refused rather than guessed at: a corrupt file read as "no selection" would silently widen the
            // scope of the next pass to the whole source.
            throw new IllegalStateException("Failed to read root selection file " + selectionFile, e);
        }
    }

    /**
     * Writes through a temporary file and moves it into place, so a process killed mid-write leaves the
     * previous selection rather than a truncated one. The non-atomic fallback covers filesystems that cannot
     * promise an atomic move.
     */
    private void writeState(SelectionState state) {
        try {
            Path parent = selectionFile.getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
            Path tempFile = selectionFile.resolveSibling(selectionFile.getFileName() + ".tmp");
            OBJECT_MAPPER.writeValue(tempFile.toFile(), new SelectionState(new TreeMap<>(state.selections())));
            try {
                Files.move(tempFile, selectionFile,
                        StandardCopyOption.REPLACE_EXISTING,
                        StandardCopyOption.ATOMIC_MOVE);
            } catch (IOException atomicMoveFailure) {
                Files.move(tempFile, selectionFile, StandardCopyOption.REPLACE_EXISTING);
            }
        } catch (IOException e) {
            throw new IllegalStateException("Failed to write root selection file " + selectionFile, e);
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    private record SelectionState(Map<String, RootSelection> selections) {

        private SelectionState {
            selections = selections == null ? Map.of() : Map.copyOf(selections);
        }

        static SelectionState empty() {
            return new SelectionState(Map.of());
        }
    }
}
