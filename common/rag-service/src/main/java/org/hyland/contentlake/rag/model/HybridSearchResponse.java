package org.hyland.contentlake.rag.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * Response payload for the hybrid search endpoint.
 *
 * <p>Returns fused results from both vector and keyword search,
 * along with the fusion strategy used and per-hit score breakdown.</p>
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class HybridSearchResponse {

    /** Original query text. */
    private String query;

    /** Fusion strategy that was applied ("rrf" or "weighted"). */
    private String strategy;

    /** Score normalisation mode used when strategy = "weighted". */
    private String normalization;

    /** Embedding model used for the vector leg. */
    private String model;

    /** Number of results returned. */
    private int resultCount;

    /** Number of vector candidates retrieved before fusion. */
    private int vectorCandidates;

    /** Number of keyword candidates retrieved before fusion. */
    private int keywordCandidates;

    /**
     * Number of query formulations retrieved for, counting the original.
     *
     * <p>Null when query expansion is disabled, which keeps the default response shape unchanged for
     * clients that predate it.</p>
     */
    private Integer queryVariants;

    /**
     * Distinct source documents the returned chunks belong to.
     *
     * <p>See {@link SemanticSearchResponse#getDocumentCount()}: populated on every response, and the only
     * way to read a short answer under a document budget correctly.</p>
     */
    private Integer documentCount;

    /** The document budget actually applied, after clamping, or null when none was requested. */
    private Integer appliedTopDocuments;

    /** The per-document chunk budget actually applied, or null when no document budget was requested. */
    private Integer appliedChunksPerDocument;

    /** Time taken for the full hybrid search in milliseconds. */
    private long searchTimeMs;

    /** Error message when the request was rejected (e.g., malformed filter). Null on success. */
    private String error;

    /**
     * Ordered list of fused search hits.
     *
     * <p>Flat, with {@code sourceDocument} repeated per hit. See
     * {@link SemanticSearchResponse#getResults()} for what a document budget does to the ordering.</p>
     */
    private List<HybridHit> results;

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class HybridHit {

        /** Rank position (1-based) in the fused result list. */
        private int rank;

        /** Fused score (RRF score or weighted combination). */
        private double score;

        /** The matched chunk text. */
        private String chunkText;

        /** Source document metadata. */
        private SemanticSearchResponse.SourceDocument sourceDocument;

        /** Chunk positioning and strategy metadata. */
        private SemanticSearchResponse.ChunkMetadata chunkMetadata;

        /** Score from the vector (semantic) search leg, if present. */
        private Double vectorScore;

        /** Score from the keyword (fulltext) search leg, if present. */
        private Double keywordScore;

        /** Rank in the vector search results (null if not present in vector results). */
        private Integer vectorRank;

        /** Rank in the keyword search results (null if not present in keyword results). */
        private Integer keywordRank;

        /**
         * Per-chunk embedding vector, carried through retrieval for MMR diversity selection.
         * Present only for hits surfaced via the vector leg. Never serialized to API responses.
         */
        @JsonIgnore
        private List<Double> vector;
    }
}
