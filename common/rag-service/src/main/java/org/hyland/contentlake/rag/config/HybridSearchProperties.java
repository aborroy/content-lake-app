package org.hyland.contentlake.rag.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * Configuration properties for hybrid search (vector + keyword).
 *
 * <p>Supports two fusion strategies:
 * <ul>
 *   <li><strong>rrf</strong> — Reciprocal Rank Fusion (default). Merges by rank position,
 *       independent of score scale differences between vector and keyword results.</li>
 *   <li><strong>weighted</strong> — Weighted linear combination. Normalises scores to [0,1]
 *       then applies {@code vectorWeight} and {@code textWeight}.</li>
 * </ul>
 */
@Data
@Component
@ConfigurationProperties(prefix = "search.hybrid")
public class HybridSearchProperties {

    /** Enables/disables hybrid search behavior. */
    private boolean enabled = true;

    /** Fusion strategy: "rrf" or "weighted". */
    private String strategy = "rrf";

    /** Score normalisation mode for weighted fusion: "max" or "minmax". */
    private String normalization = "max";

    /** Weight for vector (semantic) scores when strategy = "weighted". */
    private double vectorWeight = 0.7;

    /** Weight for keyword (text) scores when strategy = "weighted". */
    private double textWeight = 0.3;

    /** Number of candidates to retrieve from each source before fusion. */
    private int initialCandidates = 20;

    /** Maximum number of results to return after fusion. */
    private int finalResults = 5;

    /** RRF ranking constant (k). Higher values smooth rank differences. */
    private int rrfK = 60;

    /** Default minimum score threshold for hybrid results. */
    private double defaultMinScore = 0.0;

    /** Backward-compatible alias for older naming (`candidate-count`). */
    public int getCandidateCount() {
        return initialCandidates;
    }

    /** Backward-compatible alias for older naming (`candidate-count`). */
    public void setCandidateCount(int candidateCount) {
        this.initialCandidates = candidateCount;
    }

    /** Backward-compatible alias for older naming (`max-results`). */
    public int getMaxResults() {
        return finalResults;
    }

    /** Backward-compatible alias for older naming (`max-results`). */
    public void setMaxResults(int maxResults) {
        this.finalResults = maxResults;
    }
}
