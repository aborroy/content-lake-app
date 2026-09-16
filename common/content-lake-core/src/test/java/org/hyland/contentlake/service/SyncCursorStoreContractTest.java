package org.hyland.contentlake.service;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * What every {@link SyncCursorStore} has to do, asserted once per implementation that can be exercised
 * without a running hxpr. The hxpr-backed store has its own test: its interesting properties are about
 * the shape of the document it writes, not about load and save.
 */
abstract class SyncCursorStoreContractTest {

    static final String SOURCE = "sample:instance-1";

    /** The store under test, the same instance for the duration of one test method. */
    abstract SyncCursorStore store();

    @Test
    void readsBackNothingForAnUnknownSource() {
        assertThat(store().load(SOURCE)).isEmpty();
    }

    @Test
    void readsBackWhatItSaved() {
        SyncCursorStore store = store();
        store.save(SOURCE, SyncCursor.seeded("token-1"));

        assertThat(store.load(SOURCE)).hasValueSatisfying(cursor -> {
            assertThat(cursor.value()).isEqualTo("token-1");
            assertThat(cursor.generation()).isZero();
            assertThat(cursor.updatedAt()).isNotNull();
        });
    }

    @Test
    void aLaterSaveReplacesTheEarlierOne() {
        SyncCursorStore store = store();
        store.save(SOURCE, SyncCursor.seeded("token-1"));
        store.save(SOURCE, SyncCursor.seeded("token-1").advancedTo("token-2"));

        assertThat(store.load(SOURCE)).hasValueSatisfying(cursor -> {
            assertThat(cursor.value()).isEqualTo("token-2");
            assertThat(cursor.generation()).isEqualTo(1L);
        });
    }

    /** Sources do not share a cursor: one clearing its own must not force the other into a full walk. */
    @Test
    void sourcesAreKeyedIndependently() {
        SyncCursorStore store = store();
        store.save(SOURCE, SyncCursor.seeded("token-1"));
        store.save("sample:instance-2", SyncCursor.seeded("token-2"));
        store.clear(SOURCE);

        assertThat(store.load(SOURCE)).isEmpty();
        assertThat(store.load("sample:instance-2")).map(SyncCursor::value).contains("token-2");
    }

    /** Clearing what is not there is the state the caller wanted, so it is not an error. */
    @Test
    void clearingAnUnknownSourceIsSilent() {
        store().clear(SOURCE);
    }
}
