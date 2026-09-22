package org.hyland.contentlake.service;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * What every {@link RootSelectionStore} has to do, whatever it stores selections in.
 *
 * <p>The hxpr-backed store has its own test as well: its interesting properties are about the shape of the
 * document it writes, not about load and save.</p>
 */
abstract class RootSelectionStoreContractTest {

    static final String SOURCE = "sharepoint:instance-1";

    /** The same instance for the duration of one test method. */
    abstract RootSelectionStore store();

    @Test
    void readsBackNothingForASourceNobodyHasChosenRootsFor() {
        assertThat(store().load(SOURCE)).isEmpty();
    }

    @Test
    void readsBackWhatItSaved() {
        store().save(SOURCE, RootSelection.of(List.of("b!drive:folder-a", "b!drive:folder-b"), "admin"));

        assertThat(store().load(SOURCE)).isPresent().get().satisfies(selection -> {
            assertThat(selection.rootNodeIds()).containsExactly("b!drive:folder-a", "b!drive:folder-b");
            assertThat(selection.updatedBy()).isEqualTo("admin");
            assertThat(selection.updatedAt()).isNotNull();
        });
    }

    @Test
    void keepsTheOrderTheRootsWereChosenIn() {
        // The order is what an operator sees reflected back in a picker, so a store that sorted or hashed them
        // would make the UI disagree with itself between saving and reloading.
        store().save(SOURCE, RootSelection.of(List.of("zeta", "alpha", "mu"), "admin"));

        assertThat(store().load(SOURCE)).get()
                .extracting(RootSelection::rootNodeIds)
                .isEqualTo(List.of("zeta", "alpha", "mu"));
    }

    @Test
    void aLaterSaveReplacesTheEarlierOne() {
        store().save(SOURCE, RootSelection.of(List.of("first"), "admin"));
        store().save(SOURCE, RootSelection.of(List.of("second", "third"), "someone-else"));

        assertThat(store().load(SOURCE)).get().satisfies(selection -> {
            assertThat(selection.rootNodeIds()).containsExactly("second", "third");
            assertThat(selection.updatedBy()).isEqualTo("someone-else");
        });
    }

    @Test
    void anEmptySelectionIsStoredRatherThanTreatedAsAbsent() {
        // This is the distinction the whole precedence chain rests on. "Chosen, and nothing" must not fall
        // through to configured roots, or clearing a picker would silently re-ingest the entire source.
        store().save(SOURCE, RootSelection.of(List.of(), "admin"));

        assertThat(store().load(SOURCE)).isPresent();
        assertThat(store().load(SOURCE)).get().extracting(RootSelection::isEmpty).isEqualTo(true);
    }

    @Test
    void sourcesAreKeyedIndependently() {
        store().save(SOURCE, RootSelection.of(List.of("mine"), "admin"));
        store().save("cmis:other", RootSelection.of(List.of("theirs"), "admin"));

        assertThat(store().load(SOURCE)).get().extracting(RootSelection::rootNodeIds)
                .isEqualTo(List.of("mine"));
        assertThat(store().load("cmis:other")).get().extracting(RootSelection::rootNodeIds)
                .isEqualTo(List.of("theirs"));
    }

    @Test
    void clearingRemovesTheSelectionSoTheNextPassFallsBack() {
        store().save(SOURCE, RootSelection.of(List.of("chosen"), "admin"));
        store().clear(SOURCE);

        assertThat(store().load(SOURCE)).isEmpty();
    }

    @Test
    void clearingAnUnknownSourceIsSilent() {
        store().clear("nobody:has-this");
    }
}
