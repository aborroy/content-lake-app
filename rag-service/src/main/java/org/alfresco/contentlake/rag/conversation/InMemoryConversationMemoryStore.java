package org.alfresco.contentlake.rag.conversation;

import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * In-memory conversation storage.
 */
public class InMemoryConversationMemoryStore implements ConversationMemoryStore {

    private final ConcurrentHashMap<String, ConversationSession> sessions = new ConcurrentHashMap<>();

    @Override
    public Optional<ConversationSession> findById(String sessionId) {
        return Optional.ofNullable(sessions.get(sessionId));
    }

    @Override
    public ConversationSession save(ConversationSession session) {
        sessions.put(session.getSessionId(), session);
        return session;
    }

    @Override
    public void deleteById(String sessionId) {
        sessions.remove(sessionId);
    }

    @Override
    public int deleteExpiredBefore(Instant cutoff) {
        AtomicInteger removed = new AtomicInteger(0);
        sessions.entrySet().removeIf(entry -> {
            ConversationSession session = entry.getValue();
            Instant lastAccessAt = session != null ? session.getLastAccessAt() : null;
            boolean expired = lastAccessAt != null && lastAccessAt.isBefore(cutoff);
            if (expired) {
                removed.incrementAndGet();
            }
            return expired;
        });
        return removed.get();
    }
}
