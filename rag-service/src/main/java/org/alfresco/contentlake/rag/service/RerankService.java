package org.alfresco.contentlake.rag.service;

import org.alfresco.contentlake.rag.model.SemanticSearchResponse.SearchHit;

import java.util.List;

/**
 * Extension point for search result reranking.
 */
public interface RerankService {

    List<SearchHit> rerank(String query, List<SearchHit> hits);
}
