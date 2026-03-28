package org.hyland.contentlake.rag.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * Response payload for the RAG prompt endpoint.
 *
 * <p>Contains the LLM-generated answer, timing breakdown, and references
 * to the source documents used as context.</p>
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class RagPromptResponse {

    /** The LLM-generated answer. */
    private String answer;

    /** The question that was asked. */
    private String question;

    /** Effective conversation session id used for this request. */
    private String sessionId;

    /** Query actually used for retrieval (may be reformulated from the original question). */
    private String retrievalQuery;

    /** Number of prior turns included as conversation history context. */
    private Integer historyTurnsUsed;

    /** LLM model used for generation. */
    private String model;

    /** Total token count reported for this answer (prompt + completion), when available. */
    private Integer tokenCount;

    /** Time spent on semantic search (ms). */
    private long searchTimeMs;

    /** Time spent on LLM generation (ms). */
    private long generationTimeMs;

    /** Total end-to-end time (ms). */
    private long totalTimeMs;

    /** Number of source chunks used as context. */
    private int sourcesUsed;

    /** Source documents referenced in the answer. */
    private List<Source> sources;

    /** Full retrieved context (only when includeContext=true in request). */
    private List<ContextChunk> context;

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class Source {

        /** HXPR internal document identifier. */
        private String documentId;

        /** Source node identifier. */
        private String nodeId;

        /** Source-system identifier stored in cin_sourceId. */
        private String sourceId;

        /** Short source type label such as `alfresco` or `nuxeo`. */
        private String sourceType;

        /** Source document name. */
        private String name;

        /** Source document path. */
        private String path;

        /** Deep link to open the document in its native source UI. */
        private String openInSourceUrl;

        /** Relevant chunk text from this source. */
        private String chunkText;

        /** Cosine similarity score. */
        private double score;
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class ContextChunk {

        /** Rank in search results (1-based). */
        private int rank;

        /** Cosine similarity score. */
        private double score;

        /** The chunk text sent to the LLM. */
        private String text;

        /** Source document name. */
        private String sourceName;

        /** Source document path. */
        private String sourcePath;

        /** Source type for this context chunk. */
        private String sourceType;

        /** Deep link to open the source document for this context chunk. */
        private String openInSourceUrl;
    }
}
