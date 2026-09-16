package org.hyland.contentlake.service;

/** The store contract over the volatile implementation. */
class InMemorySyncCursorStoreTest extends SyncCursorStoreContractTest {

    private final SyncCursorStore store = new InMemorySyncCursorStore();

    @Override
    SyncCursorStore store() {
        return store;
    }
}
