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

    /**
     * Minimum similarity threshold, or {@code null} for the configured {@code rag.default-min-score}.
     *
     * <p>Nullable so that an explicit {@code 0.0} ("no threshold") is distinguishable from an absent
     * value. As a primitive it was not, so the configured default silently replaced it and a caller
     * could not switch the threshold off.</p>
     */
    private Double minScore;

    /** Optional HXQL filter to scope the search. */
    private String filter;

    /** Optional source type filter: any ingested source type, such as `alfresco`, `nuxeo`, `filesystem` or a connector plugin's own type. */
    private String sourceType;

    /** Embedding type to match. */
    private String embeddingType;

    /** Optional system prompt override. */
    private String systemPrompt;

    /** Whether to include retrieved chunks in the response for debugging. */
    @Builder.Default
    private boolean includeContext = false;

    /**
     * When true, an LLM step infers structured metadata filters (date range, mime, path, category)
     * from the question before retrieval and applies them additively to any caller-supplied filter.
     * Off by default; only effective on the hybrid search path.
     */
    @Builder.Default
    private boolean inferFilters = false;

    /**
     * Requested answer shape (#70). {@link ResponseFormat#TEXT} (default) returns free text only;
     * {@link ResponseFormat#STRUCTURED} additionally returns a typed {@code structured} object in the
     * response, derived in a second pass so the free-text {@code answer} is unaffected.
     */
    @Builder.Default
    private ResponseFormat responseFormat = ResponseFormat.TEXT;
}
