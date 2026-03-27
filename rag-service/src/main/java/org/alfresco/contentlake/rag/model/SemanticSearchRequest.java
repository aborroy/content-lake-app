package org.alfresco.contentlake.rag.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Request payload for the semantic search endpoint.
 *
 * <p>Accepts a natural-language query that is embedded using the same model
 * used during ingestion, then searched against the hxpr embeddings index via kNN.</p>
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class SemanticSearchRequest {

    /** Free-text query to embed and search. */
    private String query;

    /** Maximum number of results to return (default 5, max 50). */
    @Builder.Default
    private int topK = 5;

    /** Optional HXQL filter to scope the search (appended to the permission filter). */
    private String filter;

    /** Optional source type filter (`alfresco` or `nuxeo`). */
    private String sourceType;

    /** Embedding type to match. Defaults to wildcard ("*") which matches all types. */
    private String embeddingType;

    /** Minimum similarity score threshold (0.0 – 1.0). Results below this score are excluded. */
    @Builder.Default
    private double minScore = 0.0;
}
