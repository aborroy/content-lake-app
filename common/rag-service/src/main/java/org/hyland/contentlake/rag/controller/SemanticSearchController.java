package org.hyland.contentlake.rag.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.hyland.contentlake.rag.model.SemanticSearchRequest;
import org.hyland.contentlake.rag.model.SemanticSearchResponse;
import org.hyland.contentlake.rag.service.SemanticSearchService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.client.HttpClientErrorException;

import java.util.Map;

/**
 * REST controller for semantic (vector) search.
 *
 * <p>Provides endpoints to execute semantic queries against the hxpr embeddings index.
 * All endpoints require Alfresco authentication (Basic Auth or ticket).
 * Results are filtered by the authenticated user's document permissions.</p>
 */
@Slf4j
@RestController
@RequestMapping("/api/rag/search/semantic")
@RequiredArgsConstructor
public class SemanticSearchController {

    private final SemanticSearchService semanticSearchService;

    /**
     * Executes a semantic search against the embedded chunks.
     *
     * @param request search parameters (query, topK or topDocuments, filter, minScore)
     * @return ranked search results with similarity scores and metadata
     */
    @PostMapping
    public ResponseEntity<SemanticSearchResponse> search(@RequestBody SemanticSearchRequest request) {
        if (request.getQuery() == null || request.getQuery().isBlank()) {
            return ResponseEntity.badRequest().body(
                    SemanticSearchResponse.builder()
                            .query("")
                            .resultCount(0)
                            .totalCount(0)
                            .build()
            );
        }

        // A non-positive document budget is rejected rather than clamped, because unlike a too-large value
        // there is no sensible reading of it: "zero documents" is not the same request with a bound applied.
        if (isNonPositive(request.getTopDocuments()) || isNonPositive(request.getChunksPerDocument())) {
            return ResponseEntity.badRequest().body(
                    SemanticSearchResponse.builder()
                            .query(request.getQuery())
                            .resultCount(0)
                            .totalCount(0)
                            .build()
            );
        }

        log.debug("Semantic search request: query=\"{}\", topK={}, topDocuments={}, chunksPerDocument={}, "
                        + "minScore={}",
                request.getQuery(), request.getTopK(), request.getTopDocuments(),
                request.getChunksPerDocument(), request.getMinScore());

        try {
            SemanticSearchResponse response = semanticSearchService.search(request);
            return ResponseEntity.ok(response);
        } catch (HttpClientErrorException.BadRequest e) {
            // Malformed filter: hxpr returned 400 with a parser message. Surface it as 400 so the caller
            // knows it's their input, not a service failure.
            log.warn("Search rejected by engine (malformed filter): {}", e.getMessage());
            return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                    .body(SemanticSearchResponse.builder()
                            .query(request.getQuery())
                            .resultCount(0)
                            .totalCount(0)
                            .searchTimeMs(0)
                            .error(e.getResponseBodyAsString())
                            .build());
        }
    }

    /** A supplied budget of zero or less. Absent (null) is not a budget and is not rejected. */
    private static boolean isNonPositive(Integer value) {
        return value != null && value <= 0;
    }

    /**
     * Health check endpoint for the semantic search subsystem.
     *
     * @return health status including model and index information
     */
    @GetMapping("/api/rag/search/health")
    public ResponseEntity<Map<String, Object>> health() {
        try {
            var embedding = semanticSearchService.search(
                    SemanticSearchRequest.builder().query("health check").topK(1).build()
            );

            return ResponseEntity.ok(Map.of(
                    "status", "UP",
                    "model", embedding.getModel() != null ? embedding.getModel() : "unknown",
                    "vectorDimension", embedding.getVectorDimension(),
                    "searchTimeMs", embedding.getSearchTimeMs(),
                    "indexReachable", true
            ));
        } catch (Exception e) {
            log.error("Semantic search health check failed: {}", e.getMessage());
            return ResponseEntity.ok(Map.of(
                    "status", "DOWN",
                    "error", e.getMessage() != null ? e.getMessage() : "Unknown error",
                    "indexReachable", false
            ));
        }
    }
}
