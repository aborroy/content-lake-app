package org.hyland.contentlake.service;

import java.util.Optional;

/**
 * Where the host keeps a source's change-feed position.
 *
 * <p>The host owns cursor storage, never the connector. A connector is handed a cursor and returns the
 * next one; it persists nothing, needs no writable state of its own, and cannot disagree with the host
 * about where a sync resumed. That is what keeps a connector shippable as a jar with no deployment
 * story beyond the jar itself.</p>
 *
 * <p>Keys are the qualified source id, {@code "<sourceType>:<sourceId>"} -- the same string the
 * reconciliation report logs -- so a cursor and the sweep that would otherwise have deleted its
 * documents name the source identically.</p>
 *
 * <p>Implementations are called from a single ingestion job at a time but must tolerate concurrent
 * calls, since nothing structurally prevents two jobs for different sources overlapping.</p>
 */
public interface SyncCursorStore {

    /** The stored cursor for a source, or empty when the source has never been synced incrementally. */
    Optional<SyncCursor> load(String qualifiedSourceId);

    /** Records a source's position. Overwrites any previous value. */
    void save(String qualifiedSourceId, SyncCursor cursor);

    /**
     * Forgets a source's position, so the next pass walks it.
     *
     * <p>This is the path back to the authoritative mechanism, taken when a source reports its cursor
     * expired. Clearing a cursor that is not there is not an error.</p>
     */
    void clear(String qualifiedSourceId);
}
