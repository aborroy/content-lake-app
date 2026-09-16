package org.hyland.contentlake.service;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A cursor store that forgets everything when the process stops.
 *
 * <p>For development and tests, and the honest answer for a container with neither a writable mount nor
 * hxpr write access: every run is then a full walk, which is exactly today's behaviour, rather than an
 * incremental pass resuming from a position nobody kept.</p>
 */
public class InMemorySyncCursorStore implements SyncCursorStore {

    private final Map<String, SyncCursor> cursors = new ConcurrentHashMap<>();

    @Override
    public Optional<SyncCursor> load(String qualifiedSourceId) {
        return Optional.ofNullable(cursors.get(qualifiedSourceId));
    }

    @Override
    public void save(String qualifiedSourceId, SyncCursor cursor) {
        cursors.put(qualifiedSourceId, cursor);
    }

    @Override
    public void clear(String qualifiedSourceId) {
        cursors.remove(qualifiedSourceId);
    }
}
