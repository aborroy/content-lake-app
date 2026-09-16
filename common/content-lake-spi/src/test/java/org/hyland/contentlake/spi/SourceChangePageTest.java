package org.hyland.contentlake.spi;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** The page a connector hands back, and the states a host has to be able to tell apart. */
class SourceChangePageTest {

    private static SourceNode node(String nodeId) {
        return new SourceNode(nodeId, "instance-1", "sample", nodeId + ".txt", "/" + nodeId,
                "text/plain", null, false, Set.of(), Set.of(), Map.of());
    }

    @Nested
    class Defensiveness {

        @Test
        void nullListsBecomeEmptyOnes() {
            SourceChangePage page = new SourceChangePage(null, null, "cursor-1", false, false);

            assertThat(page.changed()).isEmpty();
            assertThat(page.deleted()).isEmpty();
            assertThat(page.isEmpty()).isTrue();
        }

        /** A connector that keeps mutating its own list after returning cannot change what the host saw. */
        @Test
        void listsAreCopiedAtConstruction() {
            List<SourceNode> changed = new ArrayList<>(List.of(node("a")));
            List<SourceTombstone> deleted = new ArrayList<>(List.of(SourceTombstone.deleted("b")));

            SourceChangePage page = SourceChangePage.of(changed, deleted, "cursor-1", false);
            changed.add(node("c"));
            deleted.clear();

            assertThat(page.changed()).hasSize(1);
            assertThat(page.deleted()).hasSize(1);
        }

        @Test
        void theCopiesAreUnmodifiable() {
            SourceChangePage page = SourceChangePage.of(List.of(node("a")), List.of(), "cursor-1", false);

            assertThatThrownBy(() -> page.changed().add(node("b")))
                    .isInstanceOf(UnsupportedOperationException.class);
        }
    }

    @Nested
    class States {

        @Test
        void aPageOfChangesCarriesBothListsAndItsResumeToken() {
            SourceChangePage page = SourceChangePage.of(
                    List.of(node("a"), node("b")),
                    List.of(SourceTombstone.deleted("c")),
                    "cursor-2",
                    true);

            assertThat(page.changed()).hasSize(2);
            assertThat(page.deleted()).hasSize(1);
            assertThat(page.nextCursor()).isEqualTo("cursor-2");
            assertThat(page.moreAvailable()).isTrue();
            assertThat(page.cursorExpired()).isFalse();
            assertThat(page.isEmpty()).isFalse();
        }

        /** "Nothing changed" still advances the cursor: the window was read, it was just quiet. */
        @Test
        void anEmptyPageKeepsItsCursor() {
            SourceChangePage page = SourceChangePage.empty("cursor-3");

            assertThat(page.isEmpty()).isTrue();
            assertThat(page.nextCursor()).isEqualTo("cursor-3");
            assertThat(page.moreAvailable()).isFalse();
            assertThat(page.cursorExpired()).isFalse();
        }

        /**
         * Expiry carries nothing, and that is what makes it distinguishable from a quiet window: the
         * absent cursor is the signal that the host has to forget its own and walk.
         */
        @Test
        void anExpiredPageCarriesNeitherChangesNorACursor() {
            SourceChangePage page = SourceChangePage.expired();

            assertThat(page.cursorExpired()).isTrue();
            assertThat(page.changed()).isEmpty();
            assertThat(page.deleted()).isEmpty();
            assertThat(page.nextCursor()).isNull();
            assertThat(page.moreAvailable()).isFalse();
        }

        /** An empty window and an expired cursor are both "no changes", and must not be confused. */
        @Test
        void expiryIsNotAnEmptyWindow() {
            assertThat(SourceChangePage.expired()).isNotEqualTo(SourceChangePage.empty(null));
        }
    }
}
