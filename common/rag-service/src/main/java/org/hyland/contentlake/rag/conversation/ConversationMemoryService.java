package org.hyland.contentlake.rag.conversation;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.hyland.contentlake.rag.config.RagProperties;
import org.springframework.stereotype.Service;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Conversation memory service with TTL expiration and sliding-window retention.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class ConversationMemoryService {

    private final ConversationMemoryStore store;
    private final RagProperties ragProperties;
    private final Clock clock;
    private final ConcurrentHashMap<String, Object> sessionLocks = new ConcurrentHashMap<>();

    public ConversationSession getOrCreateSession(String sessionId) {
        String id = normalizeSessionId(sessionId);
        synchronized (lockFor(id)) {
            Instant now = clock.instant();
            pruneExpiredSessions(now);
            return getOrCreateSessionInternal(id, now);
        }
    }

    public List<ConversationTurn> getRecentTurns(String sessionId) {
        return getOrCreateSession(sessionId).getTurns();
    }

    public ConversationSession appendUserTurn(String sessionId, String content) {
        return appendTurn(sessionId, ConversationTurn.Role.USER, content);
    }

    public ConversationSession appendAssistantTurn(String sessionId, String content) {
        return appendTurn(sessionId, ConversationTurn.Role.ASSISTANT, content);
    }

    public void resetSession(String sessionId) {
        String id = normalizeSessionId(sessionId);
        synchronized (lockFor(id)) {
            store.deleteById(id);
        }
    }

    private ConversationSession appendTurn(String sessionId, ConversationTurn.Role role, String content) {
        String id = normalizeSessionId(sessionId);
        if (content == null || content.isBlank()) {
            return getOrCreateSession(id);
        }

        synchronized (lockFor(id)) {
            Instant now = clock.instant();
            pruneExpiredSessions(now);

            ConversationSession session = getOrCreateSessionInternal(id, now);

            List<ConversationTurn> turns = new ArrayList<>(session.getTurns());
            turns.add(ConversationTurn.builder()
                    .role(role)
                    .content(content.trim())
                    .timestamp(now)
                    .build());

            List<ConversationTurn> windowedTurns = applySlidingWindow(turns);

            ConversationSession updated = session.toBuilder()
                    .turns(windowedTurns)
                    .lastAccessAt(now)
                    .build();
            return store.save(updated);
        }
    }

    private ConversationSession getOrCreateSessionInternal(String sessionId, Instant now) {
        return store.findById(sessionId)
                .map(existing -> touch(existing, now))
                .orElseGet(() -> createSession(sessionId, now));
    }

    private ConversationSession createSession(String sessionId, Instant now) {
        ConversationSession created = ConversationSession.builder()
                .sessionId(sessionId)
                .turns(List.of())
                .createdAt(now)
                .lastAccessAt(now)
                .build();
        return store.save(created);
    }

    private ConversationSession touch(ConversationSession session, Instant now) {
        ConversationSession updated = session.toBuilder()
                .lastAccessAt(now)
                .build();
        return store.save(updated);
    }

    private List<ConversationTurn> applySlidingWindow(List<ConversationTurn> turns) {
        int maxTurns = Math.max(1, ragProperties.getConversation().getMaxHistoryTurns());
        if (turns.size() <= maxTurns) {
            return List.copyOf(turns);
        }
        return List.copyOf(turns.subList(turns.size() - maxTurns, turns.size()));
    }

    private void pruneExpiredSessions(Instant now) {
        int ttlMinutes = Math.max(1, ragProperties.getConversation().getSessionTtlMinutes());
        Instant cutoff = now.minus(Duration.ofMinutes(ttlMinutes));
        int removed = store.deleteExpiredBefore(cutoff);
        if (removed > 0) {
            log.debug("Pruned {} expired conversation sessions", removed);
        }
    }

    private static String normalizeSessionId(String sessionId) {
        if (sessionId == null || sessionId.isBlank()) {
            throw new IllegalArgumentException("sessionId is required");
        }
        return sessionId.trim();
    }

    private Object lockFor(String sessionId) {
        return sessionLocks.computeIfAbsent(sessionId, key -> new Object());
    }
}
