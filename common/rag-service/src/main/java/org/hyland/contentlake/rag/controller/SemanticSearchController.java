package org.hyland.contentlake.rag.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.hyland.contentlake.rag.model.SemanticSearchRequest;
import org.hyland.contentlake.rag.model.SemanticSearchResponse;
import org.hyland.contentlake.rag.service.SemanticSearchService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

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
     * @param request search parameters (query, topK, filter, minScore)
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

        log.debug("Semantic search request: query=\"{}\", topK={}, minScore={}",
                request.getQuery(), request.getTopK(), request.getMinScore());

        SemanticSearchResponse response = semanticSearchService.search(request);
        return ResponseEntity.ok(response);
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
