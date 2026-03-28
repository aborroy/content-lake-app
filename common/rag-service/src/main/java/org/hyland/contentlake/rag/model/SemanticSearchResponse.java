package org.hyland.contentlake.rag.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * Response payload for the semantic search endpoint.
 *
 * <p>Each result contains the matched chunk text, source document metadata,
 * the cosine similarity score, and chunk-level metadata.</p>
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class SemanticSearchResponse {

    /** Original query text. */
    private String query;

    /** Embedding model used for the query vector. */
    private String model;

    /** Dimensionality of the query vector. */
    private int vectorDimension;

    /** Number of results returned. */
    private int resultCount;

    /** Total matching embeddings (approximate when trackTotalCount is false). */
    private long totalCount;

    /** Time taken for the search in milliseconds. */
    private long searchTimeMs;

    /** Ordered list of search hits. */
    private List<SearchHit> results;

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class SearchHit {

        /** Rank position (1-based). */
        private int rank;

        /** Cosine similarity score (0.0 – 1.0). */
        private double score;

        /** The matched chunk text. */
        private String chunkText;

        /** Source document metadata. */
        private SourceDocument sourceDocument;

        /** Chunk positioning and strategy metadata. */
        private ChunkMetadata chunkMetadata;
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class SourceDocument {

        /** HXPR internal document identifier. */
        private String documentId;

        /** Alfresco node identifier (stored in cin_id). */
        private String nodeId;

        /** Source-system identifier stored in cin_sourceId. */
        private String sourceId;

        /** Short source type label such as `alfresco` or `nuxeo`. */
        private String sourceType;

        /** Source document name. */
        private String name;

        /** Source document path. */
        private String path;

        /** MIME type of the source document. */
        private String mimeType;

        /** Deep link to open the document in its native source UI. */
        private String openInSourceUrl;
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class ChunkMetadata {

        /** Embedding identifier within the HXPR index. */
        private String embeddingId;

        /** Embedding type / model identifier. */
        private String embeddingType;

        /** Page number (if available from location metadata). */
        private Integer page;

        /** Paragraph index (if available from location metadata). */
        private Integer paragraph;

        /** Character length of the chunk text. */
        private int chunkLength;
    }
}
