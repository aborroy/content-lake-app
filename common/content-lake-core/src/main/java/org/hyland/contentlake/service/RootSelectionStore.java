package org.hyland.contentlake.service;

import java.util.Optional;

/**
 * Where the host keeps the roots an operator chose for a source.
 *
 * <p>This exists so a selection can be changed without restarting a container. Roots were startup
 * configuration, read once into an immutable list, which meant an operator screen could not change what a
 * sync walks; the pass now asks this store instead.</p>
 *
 * <p>Keys are the qualified source id, {@code "<sourceType>:<sourceId>"} -- the same string a cursor and a
 * reconciliation report use -- so all three name a source identically.</p>
 *
 * <h3>Deliberately a separate interface from {@link SyncCursorStore}</h3>
 * <p>The two have the same three methods and the same key, and are still not the same store. A cursor and a
 * selection for one source would collide on a single document path, and {@code SyncCursor}'s generation
 * counter means "incremental passes since the last walk", which a selection has no analogue for. Making one
 * generic over its value type would churn three implementations and a contract test to express a similarity
 * that nothing needs to act on.</p>
 *
 * <p>Implementations are called from a single ingestion job at a time but must tolerate concurrent calls,
 * since an operator can write a selection while a pass is reading one.</p>
 */
public interface RootSelectionStore {

    /**
     * The stored selection for a source, or empty when nobody has chosen one.
     *
     * <p>Empty means "not chosen", which falls through to configured roots and then to the connector's own.
     * A present selection holding an empty list means "chosen, and nothing" and must not fall through.</p>
     */
    Optional<RootSelection> load(String qualifiedSourceId);

    /** Records a selection. Overwrites any previous value. */
    void save(String qualifiedSourceId, RootSelection selection);

    /**
     * Forgets a source's selection, so the next pass falls back to configured roots and then to the
     * connector's own. Clearing a selection that is not there is not an error.
     */
    void clear(String qualifiedSourceId);
}
