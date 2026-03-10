package org.alfresco.contentlake.rag.conversation;

import java.time.Instant;
import java.util.Optional;

/**
 * Storage abstraction for conversation memory.
 *
 * <p>Default implementation is in-memory. Redis/DB implementations can be added later
 * without changing {@link ConversationMemoryService}.</p>
 */
public interface ConversationMemoryStore {

    Optional<ConversationSession> findById(String sessionId);

    ConversationSession save(ConversationSession session);

    void deleteById(String sessionId);

    int deleteExpiredBefore(Instant cutoff);
}
