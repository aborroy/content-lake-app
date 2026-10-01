package org.hyland.contentlake.rag.conversation;

import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.hyland.contentlake.client.HxprDocumentApi;
import org.hyland.contentlake.client.HxprService;
import org.hyland.contentlake.model.HxprDocument;
import org.hyland.contentlake.rag.config.RagProperties;
import org.springframework.ai.chat.messages.Message;
import org.springframework.ai.chat.messages.SystemMessage;
import org.springframework.ai.chat.messages.UserMessage;
import org.springframework.ai.chat.model.ChatModel;
import org.springframework.ai.chat.model.ChatResponse;
import org.springframework.ai.chat.prompt.Prompt;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Long-term conversation memory: an LLM-maintained running summary that preserves key facts, named
 * entities, chosen filters, and user intent beyond the sliding turn window and across restarts.
 *
 * <p>{@link ConversationMemoryService} keeps only the last N turns in memory (lost on restart or
 * once they age out). This service persists a compact summary as an hxpr document under the
 * configured sessions folder, giving durability and ACL protection at no extra infrastructure cost.
 * It is off by default ({@code rag.conversation.summary.enabled}) until that folder is provisioned.</p>
 *
 * <p>Uses {@link ChatModel} directly (the summary is prose, not structured) and treats every failure
 * as non-fatal: a failed update or load simply leaves the conversation running on its in-memory
 * window, exactly as it did before this feature existed.</p>
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class SessionSummaryService {

    /** cin_ingestProperties key holding the running summary text. */
    static final String SUMMARY_PROPERTY = "contentLake_sessionSummary";

    private static final String SYSTEM_PROMPT = """
            You maintain a concise running summary of a conversation between a user and a document
            assistant. Given the previous summary (if any) and the latest turns, produce an updated
            summary that preserves: key facts established, named entities, any filters or scope the
            user chose, and the user's overall intent. Drop small talk. Keep it under 200 words and
            write it as plain notes, not a dialogue. Return ONLY the updated summary.""";

    private final ChatModel chatModel;
    private final HxprService hxprService;
    private final HxprDocumentApi documentApi;
    private final RagProperties ragProperties;

    /**
     * Runs the refreshes. Single-threaded so that successive turns stay consistent -- a refresh
     * reads the previous summary before writing the new one, so overlapping refreshes would lose
     * turns. Owned here rather than exposed as a bean: any {@code Executor} bean in the context
     * suppresses Spring Boot's {@code applicationTaskExecutor}, which MVC uses for the async
     * requests this service's own streaming caller depends on.
     */
    private final ExecutorService summaryExecutor = Executors.newSingleThreadExecutor(runnable -> {
        Thread thread = new Thread(runnable, "session-summary");
        thread.setDaemon(true);
        return thread;
    });

    public boolean isEnabled() {
        return ragProperties.getConversation().getSummary().isEnabled();
    }

    /**
     * Loads the persisted running summary for a session, or {@code null} if none exists, the feature
     * is disabled, or the read fails.
     */
    public String loadSummary(String sessionId) {
        if (!isEnabled() || sessionId == null || sessionId.isBlank()) {
            return null;
        }
        try {
            HxprDocument doc = hxprService.findByPath(summaryPath(sessionId));
            if (doc == null || doc.getCinIngestProperties() == null) {
                return null;
            }
            Object summary = doc.getCinIngestProperties().get(SUMMARY_PROPERTY);
            return summary != null ? summary.toString() : null;
        } catch (Exception e) {
            log.warn("Failed to load session summary for {}: {}", sessionId, e.getMessage());
            return null;
        }
    }

    /**
     * Queues a summary refresh for a completed turn and returns immediately.
     *
     * <p>The refresh is a full LLM call plus two hxpr round trips. Running it inline would hold up
     * whatever the caller does next -- on the streaming path that is the {@code metadata} event, so
     * the answer would sit on screen for seconds before its sources appeared. Nothing in the
     * response depends on the new summary: the {@code currentSummary} a caller receives is the one
     * that informed the answer it is reading, and the refresh lands shortly after.</p>
     *
     * <p>{@code summaryExecutor} is single-threaded, which is what keeps successive turns
     * consistent: each task reads the previous summary only after the preceding task has stored
     * it. Best-effort, as before -- a failed refresh leaves the conversation on its in-memory
     * window.</p>
     */
    public void updateAfterTurn(String sessionId, List<ConversationTurn> turns) {
        if (!isEnabled() || sessionId == null || sessionId.isBlank() || turns == null || turns.isEmpty()) {
            return;
        }
        summaryExecutor.execute(() -> refreshSummary(sessionId, turns));
    }

    @PreDestroy
    void stopSummaryExecutor() {
        summaryExecutor.shutdownNow();
    }

    /** The work {@link #updateAfterTurn} queues; visible for tests, which run it inline. */
    void refreshSummary(String sessionId, List<ConversationTurn> turns) {
        try {
            String previous = loadSummary(sessionId);
            String updated = generateSummary(previous, turns);
            if (updated == null || updated.isBlank()) {
                return;
            }
            persist(sessionId, updated.trim());
        } catch (Exception e) {
            log.warn("Failed to update session summary for {}: {}", sessionId, e.getMessage());
        }
    }

    private String generateSummary(String previous, List<ConversationTurn> turns) {
        StringBuilder user = new StringBuilder();
        if (previous != null && !previous.isBlank()) {
            user.append("Previous summary:\n").append(previous.trim()).append("\n\n");
        }
        user.append("Latest turns:\n");
        for (ConversationTurn turn : turns) {
            if (turn.getContent() == null || turn.getContent().isBlank()) {
                continue;
            }
            String role = turn.getRole() == ConversationTurn.Role.ASSISTANT ? "Assistant" : "User";
            user.append(role).append(": ").append(turn.getContent().trim()).append("\n");
        }
        user.append("\nUpdated summary:\n");

        List<Message> messages = List.of(new SystemMessage(SYSTEM_PROMPT), new UserMessage(user.toString()));
        ChatResponse response = chatModel.call(new Prompt(messages));
        if (response == null || response.getResult() == null || response.getResult().getOutput() == null) {
            return null;
        }
        return response.getResult().getOutput().getText();
    }

    private void persist(String sessionId, String summary) {
        String basePath = basePath();
        hxprService.ensureFolder(basePath);

        String path = summaryPath(sessionId);
        HxprDocument existing = hxprService.findByPath(path);

        Map<String, Object> props = new LinkedHashMap<>();
        props.put(SUMMARY_PROPERTY, summary);

        HxprDocument doc = new HxprDocument();
        doc.setSysPrimaryType("SysFile");
        doc.setSysName(safeName(sessionId));
        // cin_ingestProperties belongs to the CinRemote mixin; without it hxpr answers 400
        // "cin_ingestPropertyNames" and the summary is silently never stored.
        doc.setSysMixinTypes(List.of(HxprDocument.MIXIN_CIN_REMOTE));
        doc.setCinIngestProperties(props);
        // cin_ingestPropertyNames must always mirror cin_ingestProperties.keySet().
        doc.setCinIngestPropertyNames(new ArrayList<>(props.keySet()));

        if (existing != null) {
            documentApi.updateById(existing.getSysId(), doc);
        } else {
            hxprService.createDocument(basePath, doc);
        }
    }

    private String basePath() {
        String base = ragProperties.getConversation().getSummary().getBasePath();
        if (base == null || base.isBlank()) {
            base = "/_sessions";
        }
        return base.endsWith("/") && base.length() > 1 ? base.substring(0, base.length() - 1) : base;
    }

    private String summaryPath(String sessionId) {
        String base = basePath();
        return "/".equals(base) ? "/" + safeName(sessionId) : base + "/" + safeName(sessionId);
    }

    /**
     * Hex SHA-256 of the conversation key: path-safe, and unlike character substitution it cannot map
     * two keys to one document, so one user's summary is never read or overwritten by another's.
     */
    private String safeName(String sessionId) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256")
                    .digest(sessionId.trim().getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(digest);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is unavailable", e);
        }
    }
}
