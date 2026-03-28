package org.hyland.contentlake.rag.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.hyland.contentlake.rag.conversation.ConversationTurn;
import org.springframework.ai.chat.messages.Message;
import org.springframework.ai.chat.messages.SystemMessage;
import org.springframework.ai.chat.messages.UserMessage;
import org.springframework.ai.chat.model.ChatModel;
import org.springframework.ai.chat.model.ChatResponse;
import org.springframework.ai.chat.prompt.Prompt;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

/**
 * Reformulates follow-up user questions into standalone retrieval queries.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class QueryReformulationService {

    private final ChatModel chatModel;

    public String reformulate(String question, List<ConversationTurn> history) {
        if (question == null || question.isBlank()) {
            return question;
        }
        if (history == null || history.isEmpty()) {
            return question;
        }

        List<Message> messages = new ArrayList<>();
        messages.add(new SystemMessage("""
                You rewrite follow-up questions into standalone search queries.
                Rules:
                1. Use conversation history to resolve pronouns and references.
                2. Preserve user intent and constraints.
                3. Do not invent facts.
                4. Return ONLY the rewritten query text."""));
        messages.add(new UserMessage(buildPrompt(question, history)));

        try {
            ChatResponse response = chatModel.call(new Prompt(messages));
            if (response == null || response.getResult() == null || response.getResult().getOutput() == null) {
                return question;
            }
            String rewritten = response.getResult().getOutput().getText();
            if (rewritten == null || rewritten.isBlank()) {
                return question;
            }
            return rewritten.trim();
        } catch (Exception e) {
            log.warn("Query reformulation failed, using original question: {}", e.getMessage());
            return question;
        }
    }

    private static String buildPrompt(String question, List<ConversationTurn> history) {
        StringBuilder historyText = new StringBuilder();
        for (ConversationTurn turn : history) {
            String role = turn.getRole() == ConversationTurn.Role.ASSISTANT ? "Assistant" : "User";
            String content = turn.getContent() != null ? turn.getContent().trim() : "";
            if (!content.isBlank()) {
                historyText.append(role).append(": ").append(content).append("\n");
            }
        }

        return """
                Conversation history:
                %s

                Follow-up question:
                %s

                Rewritten standalone search query:
                """.formatted(historyText.toString().trim(), question.trim());
    }
}
