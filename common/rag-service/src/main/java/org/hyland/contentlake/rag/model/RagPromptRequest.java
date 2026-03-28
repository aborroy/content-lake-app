package org.hyland.contentlake.rag.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Request payload for the RAG prompt endpoint.
 *
 * <p>The question is used to perform a semantic search against the embedded chunks,
 * and the top-K results are assembled as context for the LLM to generate an answer.</p>
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class RagPromptRequest {

    /** The user's natural-language question. */
    private String question;

    /** Optional conversation session id. If omitted, server derives a user-scoped session id. */
    private String sessionId;

    /** When true, clears the target conversation session before processing this prompt. */
    @Builder.Default
    private boolean resetSession = false;

    /** Number of chunks to retrieve for context (default: configured in rag.default-top-k). */
    @Builder.Default
    private int topK = 0;

    /** Minimum similarity threshold (default: configured in rag.default-min-score). */
    @Builder.Default
    private double minScore = 0.0;

    /** Optional HXQL filter to scope the search. */
    private String filter;

    /** Optional source type filter (`alfresco` or `nuxeo`). */
    private String sourceType;

    /** Embedding type to match. */
    private String embeddingType;

    /** Optional system prompt override. */
    private String systemPrompt;

    /** Whether to include retrieved chunks in the response for debugging. */
    @Builder.Default
    private boolean includeContext = false;
}
