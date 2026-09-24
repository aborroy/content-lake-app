package org.hyland.contentlake.rag.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.hyland.contentlake.rag.config.HybridSearchProperties;
import org.hyland.contentlake.rag.model.HybridSearchRequest;
import org.hyland.contentlake.rag.model.HybridSearchResponse;
import org.hyland.contentlake.rag.service.HybridSearchService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.client.HttpClientErrorException;

/**
 * REST controller for hybrid (vector + keyword) search.
 *
 * <p>Combines semantic vector search with fulltext keyword search and fuses
 * the results using Reciprocal Rank Fusion (RRF) or weighted scoring.
 * All endpoints require Alfresco authentication.
 * Results are filtered by the authenticated user's document permissions.</p>
 */
@Slf4j
@RestController
@RequestMapping("/api/rag/search/hybrid")
@RequiredArgsConstructor
public class HybridSearchController {

    private final HybridSearchService hybridSearchService;
    private final HybridSearchProperties hybridSearchProperties;

    /**
     * Executes a hybrid search combining vector and keyword retrieval.
     *
     * @param request search parameters (query, strategy, weights, filters)
     * @return fused search results with per-hit score breakdown
     */
    @PostMapping
    public ResponseEntity<HybridSearchResponse> search(@RequestBody HybridSearchRequest request) {
        if (request.getQuery() == null || request.getQuery().isBlank()) {
            return ResponseEntity.badRequest().body(
                    HybridSearchResponse.builder()
                            .query("")
                            .resultCount(0)
                            .build()
            );
        }

        // A non-positive document budget is rejected rather than clamped, because unlike a too-large value
        // there is no sensible reading of it: "zero documents" is not the same request with a bound applied.
        if (isNonPositive(request.getTopDocuments()) || isNonPositive(request.getChunksPerDocument())) {
            return ResponseEntity.badRequest().body(
                    HybridSearchResponse.builder()
                            .query(request.getQuery())
                            .resultCount(0)
                            .build()
            );
        }

        if (!hybridSearchProperties.isEnabled()) {
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body(
                    HybridSearchResponse.builder()
                            .query(request.getQuery())
                            .strategy("disabled")
                            .resultCount(0)
                            .build()
            );
        }

        log.debug("Hybrid search request: query=\"{}\", strategy={}, maxResults={}, topDocuments={}, "
                        + "chunksPerDocument={}",
                request.getQuery(), request.getStrategy(), request.getMaxResults(),
                request.getTopDocuments(), request.getChunksPerDocument());

        try {
            HybridSearchResponse response = hybridSearchService.search(request);
            return ResponseEntity.ok(response);
        } catch (HttpClientErrorException.BadRequest e) {
            // Malformed filter: hxpr returned 400 with a parser message. Surface it as 400.
            log.warn("Hybrid search rejected by engine (malformed filter): {}", e.getMessage());
            return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                    .body(HybridSearchResponse.builder()
                            .query(request.getQuery())
                            .resultCount(0)
                            .error(e.getResponseBodyAsString())
                            .build());
        }
    }

    /** A supplied budget of zero or less. Absent (null) is not a budget and is not rejected. */
    private static boolean isNonPositive(Integer value) {
        return value != null && value <= 0;
    }
}
