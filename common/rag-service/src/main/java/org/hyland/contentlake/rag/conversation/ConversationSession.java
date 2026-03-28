package org.hyland.contentlake.rag.conversation;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.List;

/**
 * Conversation session with turn history and lifecycle metadata.
 */
@Data
@Builder(toBuilder = true)
@NoArgsConstructor
@AllArgsConstructor
public class ConversationSession {

    private String sessionId;
    private List<ConversationTurn> turns;
    private Instant createdAt;
    private Instant lastAccessAt;
}
