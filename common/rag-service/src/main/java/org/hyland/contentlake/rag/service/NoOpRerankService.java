package org.hyland.contentlake.rag.service;

import org.hyland.contentlake.rag.model.SemanticSearchResponse.SearchHit;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * Default reranker that keeps current ordering unchanged.
 */
@Service
public class NoOpRerankService implements RerankService {

    @Override
    public List<SearchHit> rerank(String query, List<SearchHit> hits) {
        if (hits == null || hits.isEmpty()) {
            return List.of();
        }
        return List.copyOf(hits);
    }
}
