package org.alfresco.contentlake.rag.conversation;

import org.alfresco.contentlake.rag.config.RagProperties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

class ConversationMemoryServiceTest {

    private ConversationMemoryService service;
    private MutableClock clock;
    private RagProperties properties;

    @BeforeEach
    void setUp() {
        properties = new RagProperties();
        RagProperties.ConversationProperties conversation = new RagProperties.ConversationProperties();
        conversation.setEnabled(true);
        conversation.setMaxHistoryTurns(3);
        conversation.setSessionTtlMinutes(30);
        conversation.setQueryReformulation(true);
        properties.setConversation(conversation);

        clock = new MutableClock(Instant.parse("2026-01-01T00:00:00Z"), ZoneOffset.UTC);
        service = new ConversationMemoryService(
                new InMemoryConversationMemoryStore(),
                properties,
                clock
        );
    }

    @Test
    void getOrCreateSession_missingSession_createsNewSession() {
        ConversationSession session = service.getOrCreateSession("session-1");

        assertThat(session.getSessionId()).isEqualTo("session-1");
        assertThat(session.getTurns()).isEmpty();
        assertThat(session.getCreatedAt()).isEqualTo(clock.instant());
        assertThat(session.getLastAccessAt()).isEqualTo(clock.instant());
    }

    @Test
    void getOrCreateSession_expiredSession_recreatesSession() {
        service.appendUserTurn("session-1", "Hello");
        clock.advance(Duration.ofMinutes(31));

        ConversationSession session = service.getOrCreateSession("session-1");

        assertThat(session.getTurns()).isEmpty();
        assertThat(session.getCreatedAt()).isEqualTo(clock.instant());
    }

    @Test
    void appendTurn_historyExceedsWindow_truncatesOldestTurns() {
        service.appendUserTurn("session-1", "u1");
        service.appendAssistantTurn("session-1", "a1");
        service.appendUserTurn("session-1", "u2");
        ConversationSession session = service.appendAssistantTurn("session-1", "a2");

        assertThat(session.getTurns()).hasSize(3);
        assertThat(session.getTurns().stream().map(ConversationTurn::getContent).toList())
                .containsExactly("a1", "u2", "a2");
    }

    @Test
    void sessions_areIsolatedBySessionId() {
        service.appendUserTurn("session-a", "hello-a");
        service.appendUserTurn("session-b", "hello-b");

        List<ConversationTurn> turnsA = service.getRecentTurns("session-a");
        List<ConversationTurn> turnsB = service.getRecentTurns("session-b");

        assertThat(turnsA).hasSize(1);
        assertThat(turnsA.getFirst().getContent()).isEqualTo("hello-a");
        assertThat(turnsB).hasSize(1);
        assertThat(turnsB.getFirst().getContent()).isEqualTo("hello-b");
    }

    @Test
    void resetSession_removesExistingTurns() {
        service.appendUserTurn("session-1", "u1");
        service.appendAssistantTurn("session-1", "a1");

        service.resetSession("session-1");
        ConversationSession recreated = service.getOrCreateSession("session-1");

        assertThat(recreated.getTurns()).isEmpty();
    }

    @Test
    void appendTurn_concurrentAppends_preservesAllTurnsWithinWindow() throws Exception {
        properties.getConversation().setMaxHistoryTurns(200);

        int parallelWrites = 25;
        ExecutorService executor = Executors.newFixedThreadPool(parallelWrites);
        CountDownLatch start = new CountDownLatch(1);
        List<java.util.concurrent.Future<?>> futures = new ArrayList<>();

        for (int i = 0; i < parallelWrites; i++) {
            final int idx = i;
            futures.add(executor.submit(() -> {
                start.await();
                service.appendUserTurn("session-concurrent", "msg-" + idx);
                return null;
            }));
        }

        start.countDown();
        for (var future : futures) {
            future.get(5, TimeUnit.SECONDS);
        }
        executor.shutdown();
        executor.awaitTermination(5, TimeUnit.SECONDS);

        List<String> contents = service.getRecentTurns("session-concurrent").stream()
                .map(ConversationTurn::getContent)
                .toList();

        assertThat(contents).hasSize(parallelWrites);
        for (int i = 0; i < parallelWrites; i++) {
            assertThat(contents).contains("msg-" + i);
        }
    }

    private static final class MutableClock extends Clock {
        private Instant instant;
        private final ZoneId zone;

        private MutableClock(Instant instant, ZoneId zone) {
            this.instant = instant;
            this.zone = zone;
        }

        @Override
        public ZoneId getZone() {
            return zone;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return new MutableClock(instant, zone);
        }

        @Override
        public Instant instant() {
            return instant;
        }

        private void advance(Duration duration) {
            instant = instant.plus(duration);
        }
    }
}
