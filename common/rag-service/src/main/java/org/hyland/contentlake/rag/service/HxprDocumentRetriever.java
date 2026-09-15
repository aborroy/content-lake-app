package org.hyland.contentlake.rag.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.hyland.contentlake.rag.config.RagProperties;
import org.hyland.contentlake.rag.model.HybridSearchRequest;
import org.hyland.contentlake.rag.model.HybridSearchResponse;
import org.hyland.contentlake.rag.model.SemanticSearchRequest;
import org.hyland.contentlake.rag.model.SemanticSearchResponse;
import org.hyland.contentlake.rag.model.SemanticSearchResponse.SearchHit;
import org.springframework.ai.document.Document;
import org.springframework.ai.rag.Query;
import org.springframework.ai.rag.retrieval.search.DocumentRetriever;

import java.util.List;
import java.util.Map;

/**
 * Spring AI {@link DocumentRetriever} backed by Content Lake's hxpr search stack.
 *
 * <p>Delegates to {@link HybridSearchService} when {@code rag.use-hybrid-search=true}
 * (default) and {@link SemanticSearchService} otherwise, exactly as the previous inline
 * {@code RagService.retrieveContext()} did. Retrieval parameters (topK, minScore, filter,
 * sourceType, embeddingType) travel in {@link Query#context()} so ACL/permission and
 * source-type filtering behavior is preserved verbatim — this retriever adds no filtering
 * of its own.</p>
 *
 * <p>Each returned {@link Document} carries:</p>
 * <ul>
 *   <li>{@link Document#getScore()} — the search hit score</li>
 *   <li>metadata {@link #HIT_METADATA_KEY} — the original {@link SearchHit}, so the advisor
 *       can rerank and build rich source metadata without re-querying hxpr</li>
 * </ul>
 */
@Slf4j
@RequiredArgsConstructor
public class HxprDocumentRetriever implements DocumentRetriever {

    /** {@link Query#context()} key: {@link Integer} number of chunks to retrieve. */
    public static final String CTX_TOP_K = "cl.topK";
    /** {@link Query#context()} key: {@link Double} minimum similarity score. */
    public static final String CTX_MIN_SCORE = "cl.minScore";
    /** {@link Query#context()} key: {@link String} optional HXQL filter. */
    public static final String CTX_FILTER = "cl.filter";
    /** {@link Query#context()} key: {@link String} optional source-type filter. */
    public static final String CTX_SOURCE_TYPE = "cl.sourceType";
    /** {@link Query#context()} key: {@link String} optional embedding type. */
    public static final String CTX_EMBEDDING_TYPE = "cl.embeddingType";
    /**
     * {@link Query#context()} key: optional {@link HybridSearchRequest.MetadataFilter} inferred from
     * the question. Applied on the hybrid path only (semantic requests carry no metadata layer).
     */
    public static final String CTX_METADATA_FILTER = "cl.metadataFilter";

    /** {@link Document#getMetadata()} key holding the original {@link SearchHit}. */
    public static final String HIT_METADATA_KEY = "cl.searchHit";

    private final SemanticSearchService semanticSearchService;
    private final HybridSearchService hybridSearchService;
    private final RagProperties ragProperties;

    @Override
    public List<Document> retrieve(Query query) {
        Map<String, Object> ctx = query.context();
        int topK = intValue(ctx.get(CTX_TOP_K), ragProperties.getDefaultTopK());
        double minScore = doubleValue(ctx.get(CTX_MIN_SCORE), ragProperties.getDefaultMinScore());
        String filter = stringValue(ctx.get(CTX_FILTER));
        String sourceType = stringValue(ctx.get(CTX_SOURCE_TYPE));
        String embeddingType = stringValue(ctx.get(CTX_EMBEDDING_TYPE));
        HybridSearchRequest.MetadataFilter metadataFilter = metadataFilterValue(ctx.get(CTX_METADATA_FILTER));
        boolean useHybrid = ragProperties.isUseHybridSearch();

        // When MMR is enabled, over-retrieve a larger candidate pool; the advisor's diversity
        // selector trims it back down to topK before reranking. Otherwise retrieve exactly topK.
        boolean mmrEnabled = ragProperties.getMmr().isEnabled();
        int retrievalSize = mmrEnabled ? Math.max(ragProperties.getMmr().getPoolSize(), topK) : topK;

        log.info("Retrieve phase: query=\"{}\", topK={}, retrievalSize={}, minScore={}, hybrid={}, mmr={}",
                query.text(), topK, retrievalSize, minScore, useHybrid, mmrEnabled);

        List<SearchHit> hits;
        if (useHybrid) {
            // minScore must be passed through: omitting it silently substituted the server-side
            // search.hybrid.default-min-score for both the request value and rag.default-min-score,
            // and since hybrid search is the default path that made every minScore setting and any
            // sweep over it meaningless.
            HybridSearchRequest hybridRequest = HybridSearchRequest.builder()
                    .query(query.text())
                    .maxResults(retrievalSize)
                    .minScore(minScore)
                    .filter(filter)
                    .sourceType(sourceType)
                    .embeddingType(embeddingType)
                    .metadata(metadataFilter)
                    // The per-document cap is for a caller browsing results, not for the generator
                    // (#134). Measured on the 78-question golden set, capping this path costs
                    // faithfulness (0.806 -> 0.762 at a cap of 2, 0.710 at 3) and citation accuracy
                    // (0.808 -> 0.779, 0.695), because a long document that IS the answer loses the
                    // chunks that supported it while other documents' best chunks take the slots.
                    .skipDocumentDiversity(true)
                    .build();
            HybridSearchResponse response = hybridSearchService.search(hybridRequest);
            hits = mapHybridHits(response.getResults());
        } else {
            if (metadataFilter != null) {
                log.warn("Inferred metadata filter ignored: semantic (non-hybrid) search has no metadata layer");
            }
            SemanticSearchRequest searchRequest = SemanticSearchRequest.builder()
                    .query(query.text())
                    .topK(retrievalSize)
                    .minScore(minScore)
                    .filter(filter)
                    .sourceType(sourceType)
                    .embeddingType(embeddingType)
                    // See the hybrid branch above: the cap belongs to the endpoints, not to retrieval
                    // for generation.
                    .skipDocumentDiversity(true)
                    .build();
            SemanticSearchResponse response = semanticSearchService.search(searchRequest);
            hits = response.getResults() != null ? response.getResults() : List.of();
        }

        return hits.stream().map(HxprDocumentRetriever::toDocument).toList();
    }

    /** Wraps a {@link SearchHit} as a Spring AI {@link Document}, preserving score and the hit itself. */
    private static Document toDocument(SearchHit hit) {
        String text = hit.getChunkText() != null ? hit.getChunkText() : "";
        return Document.builder()
                .text(text)
                .score(hit.getScore())
                .metadata(HIT_METADATA_KEY, hit)
                .build();
    }

    /** Extracts the original {@link SearchHit} carried in a retrieved document's metadata. */
    public static SearchHit hitOf(Document document) {
        Object hit = document.getMetadata().get(HIT_METADATA_KEY);
        return hit instanceof SearchHit searchHit ? searchHit : null;
    }

    private List<SearchHit> mapHybridHits(List<HybridSearchResponse.HybridHit> hits) {
        if (hits == null) {
            return List.of();
        }
        return hits.stream()
                .map(h -> SearchHit.builder()
                        .rank(h.getRank())
                        .score(h.getScore())
                        .chunkText(h.getChunkText())
                        .sourceDocument(h.getSourceDocument())
                        .chunkMetadata(h.getChunkMetadata())
                        .vector(h.getVector())
                        .build())
                .toList();
    }

    private static int intValue(Object value, int fallback) {
        return value instanceof Number number ? number.intValue() : fallback;
    }

    private static double doubleValue(Object value, double fallback) {
        return value instanceof Number number ? number.doubleValue() : fallback;
    }

    private static String stringValue(Object value) {
        return value instanceof String s ? s : null;
    }

    private static HybridSearchRequest.MetadataFilter metadataFilterValue(Object value) {
        return value instanceof HybridSearchRequest.MetadataFilter mf ? mf : null;
    }
}
