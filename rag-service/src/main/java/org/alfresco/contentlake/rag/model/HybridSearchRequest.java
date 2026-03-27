package org.alfresco.contentlake.rag.model;

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

    /** Maximum number of fused results to return (default from config). */
    @Builder.Default
    private int maxResults = 0;

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

    /** Optional source type filter (`alfresco` or `nuxeo`). */
    private String sourceType;

    /** Optional structured metadata filter layer (source mime, path, modified range, custom properties). */
    private MetadataFilter metadata;

    /** Embedding type to match. Defaults to wildcard ("*"). */
    private String embeddingType;

    /** Minimum score threshold for final results. */
    @Builder.Default
    private double minScore = 0.0;

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
}
