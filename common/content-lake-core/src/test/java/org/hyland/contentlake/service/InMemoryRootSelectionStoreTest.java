package org.hyland.contentlake.service;

/** The store contract over the in-memory implementation. */
class InMemoryRootSelectionStoreTest extends RootSelectionStoreContractTest {

    private final RootSelectionStore store = new InMemoryRootSelectionStore();

    @Override
    RootSelectionStore store() {
        return store;
    }
}
