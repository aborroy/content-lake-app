package org.hyland.contentlake.rag.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * Configuration properties for RAG behaviour.
 *
 * <p>Bound from {@code rag.*} in {@code application.yml}.</p>
 */
@Data
@Component
@ConfigurationProperties(prefix = "rag")
public class RagProperties {

    /** Default number of chunks to retrieve for context. */
    private int defaultTopK = 5;

    /** Default minimum similarity score threshold. */
    private double defaultMinScore = 0.5;

    /**
     * Maximum character length of the assembled context sent to the LLM.
     *
     * <p>The previous default of 4000 chars (~1000 tokens) was very conservative
     * and often caused useful chunks to be truncated. Even small local models
     * like llama3.2 support 128K token context windows. 12000 chars (~3000 tokens)
     * allows 5-8 substantial chunks while leaving ample room for the system prompt,
     * user question, and generated answer within any reasonable model's limits.</p>
     */
    private int maxContextLength = 12000;

    /**
     * Default system prompt for the LLM.
     *
     * <p>Structured to work well with smaller local models by providing explicit
     * formatting guidance and step-by-step instructions for citation.</p>
     */
    private String defaultSystemPrompt = """
            You are a document assistant that answers questions based strictly on the provided context.

            RULES:
            1. Use ONLY information from the DOCUMENT CONTEXT below. Do not use prior knowledge.
            2. When referencing information, cite the source using its label (e.g. "According to Source 1...").
            3. If multiple sources contain relevant information, synthesize them and cite each.
            4. If the context does not contain enough information to fully answer the question, clearly \
            state what you can answer and what is missing.
            5. Be concise and direct. Do not repeat the question or add unnecessary preamble.""";

    /** Conversation memory settings. */
    private ConversationProperties conversation = new ConversationProperties();

    /** Source-specific deep-link templates returned in search and RAG responses. */
    private SourceLinkProperties sourceLinks = new SourceLinkProperties();

    @Data
    public static class ConversationProperties {

        /** Enables/disables conversation memory features. */
        private boolean enabled = true;

        /** Number of most recent turns kept in memory per session. */
        private int maxHistoryTurns = 10;

        /** Session expiration timeout based on inactivity. */
        private int sessionTtlMinutes = 30;

        /** Enables/disables conversation-aware query reformulation. */
        private boolean queryReformulation = true;
    }

    @Data
    public static class SourceLinkProperties {

        /** Share-style deep link for Alfresco documents. */
        private String alfrescoTemplate =
                "${content.service.url}/share/page/document-details?nodeRef=workspace://SpacesStore/{nodeId}";

        /** Default Nuxeo Web UI browse link using the full repository path. */
        private String nuxeoTemplate =
                "${nuxeo.base-url}/ui/#!/browse{nuxeoPath}";
    }
}
