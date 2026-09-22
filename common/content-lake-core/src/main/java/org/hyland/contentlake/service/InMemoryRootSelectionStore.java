package org.hyland.contentlake.service;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A selection that lasts as long as the process.
 *
 * <p>For a test, and for a deployment that wants the endpoint without persistence: a restart forgets the
 * choice and the pass falls back to configured roots, which is the pre-selection behaviour.</p>
 */
public class InMemoryRootSelectionStore implements RootSelectionStore {

    private final Map<String, RootSelection> selections = new ConcurrentHashMap<>();

    @Override
    public Optional<RootSelection> load(String qualifiedSourceId) {
        return Optional.ofNullable(selections.get(qualifiedSourceId));
    }

    @Override
    public void save(String qualifiedSourceId, RootSelection selection) {
        selections.put(qualifiedSourceId, selection);
    }

    @Override
    public void clear(String qualifiedSourceId) {
        selections.remove(qualifiedSourceId);
    }
}
