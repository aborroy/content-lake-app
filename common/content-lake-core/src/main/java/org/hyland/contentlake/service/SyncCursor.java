package org.hyland.contentlake.service;

import java.time.OffsetDateTime;

/**
 * Where an incremental sync resumed from, and how far it has drifted from an authoritative pass.
 *
 * @param value      the source's own resume token, stored verbatim. Opaque: nothing here parses,
 *                   compares or orders it, because a delta link, a page token and an epoch second are
 *                   all legitimate values and only the source knows which it issued
 * @param updatedAt  when the host last advanced the cursor, for an operator asking how current an
 *                   index is without reading the source
 * @param generation how many incremental passes have run since the last full walk. A walk resets it to
 *                   zero; each saved incremental pass adds one. This is what
 *                   {@code connector.change-feed.full-walk-every} counts, and the only reason the host
 *                   needs to remember anything about a cursor beyond its value: a feed that silently
 *                   misses a deletion is only ever corrected by walking again
 */
public record SyncCursor(String value, OffsetDateTime updatedAt, long generation) {

    public SyncCursor {
        generation = Math.max(0L, generation);
    }

    /** A cursor taken straight after a full walk, so the drift count starts over. */
    public static SyncCursor seeded(String value) {
        return new SyncCursor(value, OffsetDateTime.now(), 0L);
    }

    /** The same cursor moved to the next window, counting one more pass since the last walk. */
    public SyncCursor advancedTo(String nextValue) {
        return new SyncCursor(nextValue, OffsetDateTime.now(), generation + 1L);
    }
}
