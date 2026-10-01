package org.hyland.contentlake.rag.conversation;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ConversationKeysTest {

    @Test
    void sameSessionId_differentOwners_giveDifferentKeys() {
        assertThat(ConversationKeys.of("alice", "s1")).isNotEqualTo(ConversationKeys.of("bob", "s1"));
    }

    @Test
    void separatorInOwnerOrSessionId_cannotMakeTwoPairsCollide() {
        assertThat(ConversationKeys.of("a/b", "c")).isNotEqualTo(ConversationKeys.of("a", "b/c"));
    }

    @Test
    void blankOwnerOrSessionId_isRejected() {
        assertThatThrownBy(() -> ConversationKeys.of(" ", "s1")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> ConversationKeys.of("alice", null)).isInstanceOf(IllegalArgumentException.class);
    }
}
