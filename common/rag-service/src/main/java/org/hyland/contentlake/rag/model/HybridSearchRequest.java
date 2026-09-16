package org.hyland.contentlake.rag.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

/**
 * Request payload for the hybrid search endpoint.
 *
 * <p>Combines vector (semantic) and keyword (fulltext) search.
 * The caller can override the default fusion strategy and weights per request.</p>
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class HybridSearchRequest {

    /** Free-text query used for both vector embedding and keyword matching. */
    private String query;

    /** Maximum number of fused chunks to return (default from config). */
    @Builder.Default
    private int maxResults = 0;

    /**
     * Distinct source documents to return chunks from (max 50), or {@code null} for chunk-oriented paging.
     *
     * <p>See {@link SemanticSearchRequest#getTopDocuments()}: same field, same precedence over
     * {@link #maxResults} that it has over {@code topK}, and same reason for being nullable.</p>
     */
    private Integer topDocuments;

    /**
     * Most chunks to take from any one document (max 10), or {@code null} to use
     * {@code rag.retrieval.document-diversity.max-chunks-per-document}.
     *
     * <p>See {@link SemanticSearchRequest#getChunksPerDocument()}.</p>
     */
    private Integer chunksPerDocument;

    /** Number of candidates to retrieve from each source before fusion (default from config). */
    @Builder.Default
    private int candidateCount = 0;

    /** Fusion strategy override: "rrf" or "weighted". Null uses the configured default. */
    private String strategy;

    /** Normalisation override for weighted fusion: "max" or "minmax". */
    private String normalization;

    /** Vector weight override (only for "weighted" strategy). */
    @Builder.Default
    private double vectorWeight = 0.0;

    /** Text weight override (only for "weighted" strategy). */
    @Builder.Default
    private double textWeight = 0.0;

    /** Optional HXQL filter appended to the permission filter. */
    private String filter;

    /**
     * Optional name of a hxpr named query, an alternative to the inline {@link #filter}: its
     * matching documents scope the search. Null (the default) leaves retrieval unchanged.
     */
    private String namedQuery;

    /** Optional source type filter (`alfresco` or `nuxeo`). */
    private String sourceType;

    /** Optional structured metadata filter layer (source mime, path, modified range, custom properties). */
    private MetadataFilter metadata;

    /** Embedding type to match. Defaults to wildcard ("*"). */
    private String embeddingType;

    /**
     * Minimum score threshold for final results, or {@code null} to use the configured default.
     *
     * <p>Nullable rather than a primitive so that an explicit {@code 0.0} is distinguishable from
     * "not supplied". As a primitive defaulting to 0.0, a caller asking for no threshold was
     * indistinguishable from a caller saying nothing, so the server default replaced it and no
     * request could actually turn the threshold off.</p>
     */
    private Double minScore;

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class MetadataFilter {

        /** Filter by source MIME type (for example: "application/pdf"). */
        private String mimeType;

        /** Restrict results to documents whose source path starts with this prefix. */
        private String pathPrefix;

        /** Inclusive lower bound for source modified timestamp (ISO-8601 string). */
        private String modifiedAfter;

        /** Inclusive upper bound for source modified timestamp (ISO-8601 string). */
        private String modifiedBefore;

        /** Optional exact-match filters against cin_ingestProperties keys. */
        private Map<String, String> properties;
    }

    /**
     * Opts this search out of the per-document cap (#134). Not part of the wire format; see
     * {@link SemanticSearchRequest#isSkipDocumentDiversity()} for why the RAG pipeline opts out while an
     * endpoint caller does not, and why the flag is negative rather than positive.
     */
    @com.fasterxml.jackson.annotation.JsonIgnore
    private boolean skipDocumentDiversity;
}
