package org.hyland.contentlake.rag.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.hyland.contentlake.rag.cache.RagQueryCache;
import org.hyland.contentlake.rag.config.RagProperties;
import org.hyland.contentlake.rag.observability.RagObservations;
import org.hyland.contentlake.rag.model.SemanticSearchRequest;
import org.hyland.contentlake.rag.model.SemanticSearchResponse;
import org.hyland.contentlake.rag.model.SemanticSearchResponse.ChunkMetadata;
import org.hyland.contentlake.rag.model.SemanticSearchResponse.SearchHit;
import org.hyland.contentlake.rag.model.SemanticSearchResponse.SourceDocument;
import org.hyland.contentlake.security.AclFilterBuilder;
import org.hyland.contentlake.security.CallerIdentityService;
import org.hyland.contentlake.security.SecurityContextService;
import org.hyland.contentlake.client.HxprService;
import org.hyland.contentlake.client.NamedQueryService;
import org.hyland.contentlake.hxpr.api.model.Embedding;
import org.hyland.contentlake.hxpr.api.model.VectorSearchResult;
import org.hyland.contentlake.model.ContentLakeIngestProperties;
import org.hyland.contentlake.model.HxprDocument;
import org.hyland.contentlake.model.SectionMap;
import org.hyland.contentlake.service.EmbeddingService;
import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Service for executing permission-aware semantic searches against the HXPR vector index.
 *
 * <p>Workflow:
 * <ol>
 *   <li>Embed the query text using the same model used at ingestion time</li>
 *   <li>Retrieve the authenticated user authorities from each configured content source</li>
 *   <li>Build an HXQL permission filter matching the user authorities against {@code sys_racl}</li>
 *   <li>Execute kNN vector search via HXPR</li>
 *   <li>Enrich results with parent document metadata</li>
 * </ol>
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class SemanticSearchService {

    /** Caller-visible ceiling on {@code topK} and on {@code topDocuments}. Part of the published contract. */
    private static final int MAX_TOP_K = 50;

    /** Caller-visible ceiling on {@code chunksPerDocument}. */
    private static final int MAX_CHUNKS_PER_DOCUMENT = 10;

    /** Ceiling on the chunks a document budget may return, whatever its two factors multiply to. */
    private static final int MAX_CHUNK_LIMIT = 200;

    /**
     * Internal ceiling on what is asked of hxpr, deliberately far above {@link #MAX_TOP_K}.
     *
     * <p>The two used to be the same constant, which made {@code topDocuments} unsatisfiable: you cannot
     * guarantee N distinct documents out of a candidate pool capped at the same number as the answer, and
     * over-fetch saturated at {@code topK >= 17} and vanished at 50. This bounds retrieval only; the
     * response ceiling is unchanged.</p>
     */
    private static final int MAX_FETCH_K = 500;

    /**
     * Most kNN calls one variant may make, counting the first.
     *
     * <p>A document budget can be starved by a pool that is all one document, and the only remedy is to ask
     * for more rows. Bounded because each probe is a full kNN call: two deepenings take a 10-document
     * budget from 40 rows to 160, which covers a realistically skewed pool, and an unbounded loop would
     * turn one slow query into several.</p>
     */
    private static final int MAX_VECTOR_PROBES = 3;

    private static final double FALLBACK_MIN_SCORE = 0.5d;

    private final HxprService hxprService;
    private final EmbeddingService embeddingService;
    private final SecurityContextService securityContextService;
    private final SourceMetadataResolver sourceMetadataResolver;
    private final SectionMapResolver sectionMapResolver;
    private final QueryExpansionService queryExpansionService;
    private final RagProperties ragProperties;
    private final NamedQueryService namedQueryService;
    /**
     * Optional (#121): the vector leg over every embedding type the corpus holds. Null in unit tests
     * that construct this service without it, in which case the leg makes the same single wildcard
     * call it always did.
     */
    private final MultiTypeVectorSearchService multiTypeVectorSearch;
    /** Optional (#72): null in unit tests that construct this service without the cache collaborator. */
    private final RagQueryCache queryCache;
    /** Optional (#73): null in unit tests that construct this service without the tracing collaborator. */
    private final RagObservations observations;
    /**
     * The shared source catalogue. Optional: null in unit tests, where {@link #sourceCatalog()} builds a
     * private one. Read only by the startup diagnostic now that the filter itself is built elsewhere.
     */
    private final PermissionSourceCatalog permissionSourceCatalog;
    /**
     * The ACL-scoped query for a caller, over every source they are authenticated for. Optional: null in
     * unit tests, where {@link #permissionFilter()} builds one with no group resolvers, which yields the
     * caller's default authorities for every source.
     */
    private final PermissionFilterBuilder permissionFilterBuilder;
    /** The caller's identity per source type. Optional: null in unit tests, where one is built lazily. */
    private final CallerIdentityService callerIdentityServiceBean;

    @Value("${alfresco.source-id:}")
    private String alfrescoSourceId;

    @Value("${rag.permission.source-ids:}")
    private String permissionSourceIds;

    @Value("${rag.security.admin-bypass.enabled:false}")
    private boolean adminBypassEnabled;

    @Value("${nuxeo.source-id:}")
    private String nuxeoSourceId;

    @Value("${semantic-search.default-min-score:" + FALLBACK_MIN_SCORE + "}")
    private double defaultMinScore;

    /** Built on first use by {@link #sourceCatalog()} when no bean was injected. */
    private volatile PermissionSourceCatalog fallbackSourceCatalog;
    private volatile PermissionFilterBuilder fallbackPermissionFilter;
    private volatile CallerIdentityService fallbackCallerIdentityService;

    /**
     * Permission-aware semantic search. When the query cache (#72) is enabled, an identical
     * query+filter+principal combination seen within the TTL window returns the cached response
     * without re-embedding or re-querying hxpr. The cache key includes the caller's principal scope,
     * so results are never shared across ACL contexts.
     */
    public SemanticSearchResponse search(SemanticSearchRequest request) {
        boolean cacheOn = queryCache != null && queryCache.isEnabled();
        String cacheKey = cacheOn ? buildCacheKey(request) : null;
        if (cacheKey != null) {
            SemanticSearchResponse cached = queryCache.getResult(cacheKey);
            if (cached != null) {
                log.debug("Semantic search cache hit for query \"{}\"", request.getQuery());
                return cached;
            }
        }

        SemanticSearchResponse response = executeSearch(request);

        if (cacheKey != null) {
            queryCache.putResult(cacheKey, response);
        }
        return response;
    }

    /** Builds the ACL-scoped, filter-aware cache key for a request (see {@link RagQueryCache}). */
    private String buildCacheKey(SemanticSearchRequest request) {
        Authentication auth = SecurityContextHolder.getContext().getAuthentication();
        return "sem" + ' ' + RagQueryCache.principalScope(auth)
                + ' ' + RagQueryCache.normalize(request.getQuery())
                + ' ' + request.getTopK()
                // Both budgets belong in the key: without them the same query asked with and asked without
                // topDocuments returns whichever of the two ran first.
                + ' ' + request.getTopDocuments()
                + ' ' + request.getChunksPerDocument()
                + ' ' + request.getMinScore()
                + ' ' + request.getFilter()
                + ' ' + request.getSourceType()
                + ' ' + request.getEmbeddingType()
                + ' ' + request.getNamedQuery();
    }

    private SemanticSearchResponse executeSearch(SemanticSearchRequest request) {
        long startTime = System.currentTimeMillis();

        ResultBudget budget = resolveBudget(request);
        double minScore = resolveMinScore(request);

        Authentication auth = SecurityContextHolder.getContext().getAuthentication();
        String logUser = auth != null ? auth.getName() : "anonymous";

        // Permission filter resolution is deferred and memoized: resolving group membership costs REST
        // calls to Alfresco and Nuxeo, so a query that never reaches hxpr should not pay for it, and a
        // query that runs several variants should pay for it once. Expansion changes what is asked,
        // never who is allowed to see the answer.
        Supplier<String> hxqlFilter = memoize(() -> buildHxqlFilter(request, auth));

        List<QueryVariant> variants = expand(request.getQuery());

        if (variants == null) {
            VariantResult single = searchVariant(
                    QueryVariant.original(request.getQuery()), request, budget, minScore, hxqlFilter, logUser);
            List<SearchHit> hits = applyBudget(single.hits(), budget, request);
            long searchTimeMs = System.currentTimeMillis() - startTime;
            log.info("Semantic search completed: {} results in {}ms for query: \"{}\" (minScore={})",
                    hits.size(), searchTimeMs, request.getQuery(), minScore);
            return response(request, hits, budget, single.vectorDimension(), single.totalCount(), searchTimeMs);
        }

        List<List<SearchHit>> perVariant = new ArrayList<>(variants.size());
        int vectorDimension = 0;
        long totalCount = 0;
        for (QueryVariant variant : variants) {
            VariantResult result = searchVariant(variant, request, budget, minScore, hxqlFilter, logUser);
            if (!result.hits().isEmpty()) {
                perVariant.add(result.hits());
            }
            if (vectorDimension == 0) {
                vectorDimension = result.vectorDimension();
            }
            // The variants search the same index, so the widest match count is the informative one;
            // summing would report the same chunk several times over.
            totalCount = Math.max(totalCount, result.totalCount());
        }

        // Fuse over the over-fetched pool, then apply the budget: fusing to the answer size first would
        // discard the very chunks the cap needs to promote and the selection needs to choose between.
        List<SearchHit> hits = applyBudget(
                RrfFusion.fuse(perVariant, ragProperties.getQueryExpansion().getRrfK(), budget.fetchK()),
                budget, request);
        long searchTimeMs = System.currentTimeMillis() - startTime;

        log.info("Semantic search completed: {} results in {}ms for query: \"{}\" "
                        + "(minScore={}, variants={}, contributing={})",
                hits.size(), searchTimeMs, request.getQuery(), minScore, variants.size(), perVariant.size());

        return response(request, hits, budget, vectorDimension, totalCount, searchTimeMs);
    }

    /**
     * Reduces the retrieved pool to the answer, either by selecting documents or by trimming chunks.
     *
     * <p>The two are different reductions, not one parameterised one. Trimming spends a chunk budget and
     * backfills so it can never shorten a result set; selecting spends a document budget and must not
     * backfill, or the guarantee {@code topDocuments} exists to give is gone (#135).</p>
     */
    private List<SearchHit> applyBudget(List<SearchHit> hits, ResultBudget budget, SemanticSearchRequest request) {
        if (budget.documentBudget()) {
            return DocumentGroupSelector.select(hits, budget.topDocuments(), budget.chunksPerDocument());
        }
        return DocumentDiversityLimiter.limit(hits, budget.chunkLimit(), maxChunksPerDocument(request));
    }

    /**
     * How much to retrieve and how much to return, from either budget the caller may have asked in.
     *
     * <p>With no {@code topDocuments} this is the pre-#135 arithmetic unchanged, {@link #MAX_TOP_K} bounding
     * both the answer and the fetch. With one, the answer is a product of two request-visible numbers and
     * the fetch is bounded by {@link #MAX_FETCH_K} instead, because a pool no larger than the answer cannot
     * be made to yield a given number of distinct documents.</p>
     */
    private ResultBudget resolveBudget(SemanticSearchRequest request) {
        int topK = Math.min(Math.max(request.getTopK(), 1), MAX_TOP_K);

        Integer requestedDocuments = request.getTopDocuments();
        if (requestedDocuments == null) {
            // The legs retrieve past topK when the per-document cap is on, so the cap has other documents'
            // chunks to promote: a budget already filled by one document has nothing to swap in.
            int fetchK = Math.min(topK * Math.max(overFetchFactor(request), 1), MAX_TOP_K);
            return new ResultBudget(null, 0, topK, fetchK);
        }

        int documents = clamp(requestedDocuments, 1, MAX_TOP_K);
        // Falls back to the configured cap rather than to "unlimited", because a document budget with no
        // per-document bound has no defined size: one document could fill the whole answer.
        int perDocument = request.getChunksPerDocument() != null
                ? clamp(request.getChunksPerDocument(), 1, MAX_CHUNKS_PER_DOCUMENT)
                : Math.max(configuredChunksPerDocument(), 1);

        int chunkLimit = Math.min(documents * perDocument, MAX_CHUNK_LIMIT);
        // At least doubled: the selection needs chunks of documents it will not keep to choose between.
        int fetchK = Math.min(chunkLimit * Math.max(overFetchFactor(request), 2), MAX_FETCH_K);

        return new ResultBudget(documents, perDocument, chunkLimit, fetchK);
    }

    private static int clamp(int value, int min, int max) {
        return Math.min(Math.max(value, min), max);
    }

    /** The configured per-document cap, or 0 when document diversity is disabled. */
    private int configuredChunksPerDocument() {
        RagProperties.RetrievalProperties.DocumentDiversityProperties diversity = documentDiversity();
        return diversity != null && diversity.isEnabled() ? diversity.getMaxChunksPerDocument() : 0;
    }

    /**
     * What to retrieve and what to return for one request.
     *
     * @param topDocuments      the clamped document budget, or null when the caller asked in chunks
     * @param chunksPerDocument chunks one document may contribute; meaningless without {@code topDocuments}
     * @param chunkLimit        chunks the response may carry
     * @param fetchK            rows to ask hxpr for, per variant, before any deepening
     */
    private record ResultBudget(Integer topDocuments, int chunksPerDocument, int chunkLimit, int fetchK) {

        boolean documentBudget() {
            return topDocuments != null;
        }
    }

    /**
     * The per-document chunk cap for this request, or 0 when it does not apply.
     *
     * <p>Two conditions: the deployment enables it, and the request wants it. The second is what keeps
     * the cap on the endpoints, where a caller asking for ten results and getting two documents cannot
     * tell a crowded-out document from an unindexed one, and off the RAG pipeline, where it measurably
     * costs answer quality (see {@link SemanticSearchRequest#isSkipDocumentDiversity()}).</p>
     */
    private int maxChunksPerDocument(SemanticSearchRequest request) {
        if (request != null && request.isSkipDocumentDiversity()) {
            return 0;
        }
        RagProperties.RetrievalProperties.DocumentDiversityProperties diversity = documentDiversity();
        return diversity != null && diversity.isEnabled() ? diversity.getMaxChunksPerDocument() : 0;
    }

    /**
     * The over-fetch multiple, or 1 when nothing will be promoted.
     *
     * <p>Tied to the same two conditions as the cap: over-fetching for a request that will not cap costs
     * hxpr work for a pool nothing draws from.</p>
     */
    private int overFetchFactor(SemanticSearchRequest request) {
        if (request != null && request.isSkipDocumentDiversity()) {
            return 1;
        }
        RagProperties.RetrievalProperties.DocumentDiversityProperties diversity = documentDiversity();
        return diversity != null && diversity.isEnabled() ? diversity.getOverFetchFactor() : 1;
    }

    private RagProperties.RetrievalProperties.DocumentDiversityProperties documentDiversity() {
        if (ragProperties == null || ragProperties.getRetrieval() == null) {
            return null;
        }
        return ragProperties.getRetrieval().getDocumentDiversity();
    }

    /** Embeds a query, caching the vector (#72) and spanning the embedding call (#73) when enabled. */
    private List<Double> embedQueryCached(String text, String embeddingType) {
        Supplier<List<Double>> loader = () -> traced("rag.embed.query", () -> embeddingService.embedQuery(text));
        if (queryCache != null && queryCache.isEnabled()) {
            return queryCache.embedQuery(text, embeddingType, loader);
        }
        return loader.get();
    }

    /** Runs {@code work} inside a named tracing span when observation is wired; otherwise inline. */
    private <T> T traced(String name, Supplier<T> work) {
        return observations != null ? observations.observe(name, work) : work.get();
    }

    /** Expansion is best-effort: a failure here must never fail the search. */
    private List<QueryVariant> expand(String query) {
        try {
            List<QueryVariant> variants = queryExpansionService.expand(query);
            return variants != null && variants.size() > 1 ? variants : null;
        } catch (Exception e) {
            log.warn("Query expansion failed, searching the original query only: {}", e.getMessage());
            return null;
        }
    }

    /** Builds the sys_racl permission filter, dual-auth aware, combined with the request's own filters. */
    private String buildHxqlFilter(SemanticSearchRequest request, Authentication auth) {
        String sourceTypeFilter = buildSourceTypeFilter(request.getSourceType());
        String additionalFilter = combineFilters(request.getFilter(), sourceTypeFilter);
        // A named query, when supplied, resolves server-side to an HXQL fragment; no-op when absent.
        additionalFilter = combineFilters(additionalFilter, namedQueryService.resolveFilter(request.getNamedQuery()));
        return permissionFilter().query(callerIdentityService().identities(auth), permissionSettings(),
                request.getSourceType(), additionalFilter);
    }

    /**
     * Builds the ACL permission filter for the <em>current</em> authenticated principal, combined with
     * an optional additional HXQL predicate. Resolves identity from the {@link SecurityContextHolder}
     * exactly as {@link #search} does, so ACL-scoped tool/MCP operations (#65, #61) cannot bypass
     * {@code sys_racl}. Returns a complete HXQL query ({@code SELECT ... WHERE ...}).
     */
    public String currentUserPermissionFilter(String sourceType, String additionalFilter) {
        return permissionFilter().query(callerIdentityService().currentIdentities(), permissionSettings(),
                sourceType, additionalFilter);
    }

    /** Single-threaded memoization; each search resolves its filter at most once. */
    private static <T> Supplier<T> memoize(Supplier<T> delegate) {
        return new Supplier<>() {

            private T value;
            private boolean resolved;

            @Override
            public T get() {
                if (!resolved) {
                    value = delegate.get();
                    resolved = true;
                }
                return value;
            }
        };
    }

    /** One retrieval pass for one query variant: embed, kNN, threshold, enrich. */
    private VariantResult searchVariant(QueryVariant variant,
                                        SemanticSearchRequest request,
                                        ResultBudget budget,
                                        double minScore,
                                        Supplier<String> hxqlFilter,
                                        String logUser) {
        // A variant may arrive with its own vector (HyDE embeds document-side, without the query
        // instruction prefix); otherwise embed query-side as usual.
        List<Double> queryVector = variant.vectorVector();
        if (queryVector == null) {
            log.info("Embedding query: \"{}\" (variant={}, fetchK={}, minScore={}, user={})",
                    variant.vectorText(), variant.label(), budget.fetchK(), minScore, logUser);
            queryVector = embedQueryCached(variant.vectorText(), request.getEmbeddingType());
        }

        if (queryVector == null || queryVector.isEmpty()) {
            log.warn("Empty embedding vector for query: {}", variant.vectorText());
            return VariantResult.empty(0);
        }

        String filter = hxqlFilter.get();
        log.debug("Executing vector search with filter: {}", filter);
        VectorSearchResult vectorResult = probeForDocuments(
                variant, queryVector, request, filter, budget, minScore);

        if (vectorResult == null || vectorResult.getEmbeddings() == null || vectorResult.getEmbeddings().isEmpty()) {
            log.info("No results for query: \"{}\"", variant.vectorText());
            return VariantResult.empty(queryVector.size());
        }

        // Threshold before enriching. fetchDocumentMetadata issues one SysContent point query per distinct
        // document, so enriching the raw pool pays for documents whose only chunks are about to be dropped:
        // the score is on the Embedding itself and needs no metadata to read. HybridSearchService already
        // runs in this order.
        List<Embedding> retained = applyScoreThreshold(vectorResult.getEmbeddings(), minScore);

        Map<String, SectionMap> sectionMaps = new ConcurrentHashMap<>();
        Map<String, SourceDocument> documentCache = fetchDocumentMetadata(retained, sectionMaps);
        List<SearchHit> hits = buildSearchHits(retained, documentCache, sectionMaps);
        long totalCount = vectorResult.getTotalCount() != null ? vectorResult.getTotalCount() : hits.size();

        return new VariantResult(hits, queryVector.size(), totalCount);
    }

    /**
     * The kNN call, deepened while a document budget is starved.
     *
     * <p>Re-queries at a doubled limit rather than paging: {@code MultiTypeVectorSearchService.Request} has
     * no offset component, and a global offset is not expressible across a per-embedding-type merge, since
     * page 2 of a merge is not the merge of each type's page 2. Each probe discards the previous result
     * rather than merging it, because a kNN result at a larger limit is a superset in rank order, so merging
     * would buy nothing and risk counting a chunk twice. The query vector is already embedded, so a deeper
     * probe pays only for kNN.</p>
     *
     * <p>Stops as soon as hxpr returns fewer rows than asked: that means the filtered index is exhausted and
     * no deeper probe can find another document.</p>
     */
    private VectorSearchResult probeForDocuments(QueryVariant variant,
                                                 List<Double> queryVector,
                                                 SemanticSearchRequest request,
                                                 String filter,
                                                 ResultBudget budget,
                                                 double minScore) {
        int limit = budget.fetchK();
        VectorSearchResult result = vectorLegTraced(variant, queryVector, request, filter, limit);

        if (!budget.documentBudget()) {
            return result;
        }

        for (int probe = 1; probe < MAX_VECTOR_PROBES && limit < MAX_FETCH_K; probe++) {
            int rows = rowCount(result);
            if (rows < limit) {
                break;
            }
            int documents = distinctDocuments(result, minScore);
            if (documents >= budget.topDocuments()) {
                break;
            }

            int deeper = Math.min(limit * 2, MAX_FETCH_K);
            log.debug("Document budget starved: {} of {} documents in {} rows; re-querying at limit {}",
                    documents, budget.topDocuments(), rows, deeper);
            limit = deeper;
            result = vectorLegTraced(variant, queryVector, request, filter, limit);
        }
        return result;
    }

    private VectorSearchResult vectorLegTraced(QueryVariant variant, List<Double> queryVector,
                                               SemanticSearchRequest request, String filter, int limit) {
        return traced("rag.search.vector",
                () -> vectorLeg(variant, queryVector, request.getEmbeddingType(), filter, limit));
    }

    private static int rowCount(VectorSearchResult result) {
        return result == null || result.getEmbeddings() == null ? 0 : result.getEmbeddings().size();
    }

    /** Distinct documents among the candidates that would survive the threshold. */
    private static int distinctDocuments(VectorSearchResult result, double minScore) {
        if (result == null || result.getEmbeddings() == null) {
            return 0;
        }
        Set<String> docIds = new HashSet<>();
        for (Embedding embedding : result.getEmbeddings()) {
            if (scoreOf(embedding) >= minScore && embedding.getSysembedDocId() != null) {
                docIds.add(embedding.getSysembedDocId());
            }
        }
        return docIds.size();
    }

    private SemanticSearchResponse response(SemanticSearchRequest request,
                                            List<SearchHit> hits,
                                            ResultBudget budget,
                                            int vectorDimension,
                                            long totalCount,
                                            long searchTimeMs) {
        return SemanticSearchResponse.builder()
                .query(request.getQuery())
                .model(embeddingService.getModelName())
                .vectorDimension(vectorDimension)
                .resultCount(hits.size())
                .totalCount(totalCount)
                // Reported whatever the caller asked in: a chunk-oriented caller otherwise recomputes it by
                // grouping the hits, and under a document budget it is how a short answer is read correctly.
                .documentCount(DocumentGroupSelector.documentCount(hits))
                .appliedTopDocuments(budget.topDocuments())
                .appliedChunksPerDocument(budget.documentBudget() ? budget.chunksPerDocument() : null)
                .searchTimeMs(searchTimeMs)
                .results(hits)
                .build();
    }

    /**
     * The kNN call, over every embedding type present when multi-type retrieval is wired (#121) and
     * over the {@code *} wildcard when it is not.
     *
     * <p>A second model embeds this variant the same way the primary did: document-side for a variant
     * that arrived with a pre-computed vector (HyDE's passage must not get the query instruction
     * prefix), query-side otherwise.</p>
     */
    private VectorSearchResult vectorLeg(QueryVariant variant, List<Double> vector,
                                         String embeddingType, String filter, int topK) {
        if (multiTypeVectorSearch == null) {
            return hxprService.vectorSearch(vector, embeddingType, filter, topK);
        }

        boolean documentSide = variant.documentSideVector();
        return multiTypeVectorSearch.search(MultiTypeVectorSearchService.Request.of(
                vector,
                embedder -> documentSide
                        ? embedder.embed(variant.vectorText())
                        : embedder.embedQuery(variant.vectorText()),
                embeddingType,
                filter,
                topK));
    }

    /** Outcome of a single variant's retrieval pass. */
    private record VariantResult(List<SearchHit> hits, int vectorDimension, long totalCount) {

        static VariantResult empty(int vectorDimension) {
            return new VariantResult(List.of(), vectorDimension, 0);
        }
    }

    private double resolveMinScore(SemanticSearchRequest request) {
        try {
            double req = request.getMinScore();
            if (Double.isNaN(req) || req <= 0d) {
                return clampMinScore(defaultMinScore);
            }
            return clampMinScore(req);
        } catch (Exception ignore) {
            return clampMinScore(defaultMinScore);
        }
    }

    private static double clampMinScore(double value) {
        if (Double.isNaN(value)) {
            return FALLBACK_MIN_SCORE;
        }
        if (value < 0d) {
            return 0d;
        }
        return Math.min(value, 1d);
    }

    // ---------------------------------------------------------------
    // Document metadata enrichment
    // ---------------------------------------------------------------

    private Map<String, SourceDocument> fetchDocumentMetadata(List<Embedding> embeddings,
                                                              Map<String, SectionMap> sectionMaps) {
        Map<String, SourceDocument> cache = new ConcurrentHashMap<>();

        Set<String> docIds = embeddings.stream()
                .map(Embedding::getSysembedDocId)
                .filter(Objects::nonNull)
                .filter(SemanticSearchService::looksLikeUuid)
                .collect(Collectors.toSet());

        if (docIds.isEmpty()) {
            log.debug("No resolvable sysembed_docId values; skipping metadata enrichment");
            return cache;
        }

        for (String docId : docIds) {
            try {
                HxprDocument.QueryResult result = hxprService.query(
                        "SELECT * FROM SysContent WHERE sys_id = '"
                                + AclFilterBuilder.escapeLiteral(docId) + "'",
                        1, 0);

                if (result != null && result.getDocuments() != null) {
                    result.getDocuments().stream()
                            .findFirst()
                            .ifPresent(doc -> {
                                cache.put(docId, sourceMetadataResolver.resolveSourceDocument(docId, doc));
                                SectionMap map = sectionMapResolver.parse(doc);
                                if (map != null) {
                                    sectionMaps.put(docId, map);
                                }
                            });
                }
            } catch (Exception e) {
                log.warn("Failed to fetch metadata for document {}: {}", docId, e.getMessage());
            }
        }

        log.debug("Enriched {} / {} document references", cache.size(), docIds.size());
        return cache;
    }

    // ---------------------------------------------------------------
    // Result building
    // ---------------------------------------------------------------

    /**
     * The candidates scoring at or above {@code minScore}, in the order hxpr returned them.
     *
     * <p>Separated from {@link #buildSearchHits} so the threshold can run before enrichment. The
     * per-candidate diagnostics stay here, since this is now the only place that sees a rejected
     * candidate.</p>
     */
    private static List<Embedding> applyScoreThreshold(List<Embedding> embeddings, double minScore) {
        List<Embedding> retained = new ArrayList<>(embeddings.size());
        int candidateIndex = 0;

        for (Embedding embedding : embeddings) {
            double score = scoreOf(embedding);
            candidateIndex++;

            if (log.isDebugEnabled()) {
                String text = embedding.getSysembedText();
                String preview = text != null ? text.substring(0, Math.min(60, text.length())) : "";
                log.debug("  Candidate [{}] docId={} score={} {}\"{}...\"",
                        candidateIndex, embedding.getSysembedDocId(), String.format("%.3f", score),
                        score < minScore ? "[FILTERED] " : "", preview);
            }

            if (score >= minScore) {
                retained.add(embedding);
            }
        }

        if (log.isDebugEnabled()) {
            log.debug("Score filter: minScore={} candidates={} filtered={} passing={}",
                    minScore, embeddings.size(), embeddings.size() - retained.size(), retained.size());
        }
        return retained;
    }

    private static double scoreOf(Embedding embedding) {
        return embedding.getSysembedScore() != null ? embedding.getSysembedScore() : 0.0;
    }

    /** Builds hits from candidates that have already passed the score threshold. */
    private List<SearchHit> buildSearchHits(List<Embedding> embeddings,
                                            Map<String, SourceDocument> documentCache,
                                            Map<String, SectionMap> sectionMaps) {
        List<SearchHit> hits = new ArrayList<>();
        int rank = 1;

        for (Embedding embedding : embeddings) {
            double score = scoreOf(embedding);

            String chunkText = embedding.getSysembedText();
            String docId = embedding.getSysembedDocId();

            ChunkMetadata.ChunkMetadataBuilder chunkMeta = ChunkMetadata.builder()
                    .embeddingId(embedding.getSysembedId())
                    .embeddingType(embedding.getSysembedType())
                    .chunkLength(chunkText.length());

            Integer paragraph = null;
            if (embedding.getSysembedLocation() != null
                    && embedding.getSysembedLocation().getText() != null) {
                chunkMeta.page(embedding.getSysembedLocation().getText().getPage());
                paragraph = embedding.getSysembedLocation().getText().getParagraph();
                chunkMeta.paragraph(paragraph);
            }
            // chunkType (#69) resolved from the document's section map (chunk index -> section type).
            SectionMap sectionMap = docId != null ? sectionMaps.get(docId) : null;
            chunkMeta.chunkType(sectionMapResolver.chunkType(sectionMap, paragraph));

            SourceDocument sourceDoc = (docId != null && documentCache.containsKey(docId))
                    ? documentCache.get(docId)
                    : SourceDocument.builder().documentId(docId).build();

            hits.add(SearchHit.builder()
                    .rank(rank++)
                    .score(score)
                    .chunkText(chunkText)
                    .sourceDocument(sourceDoc)
                    .chunkMetadata(chunkMeta.build())
                    .vector(embedding.getSysembedVector())
                    .build());
        }

        return hits;
    }

    // ---------------------------------------------------------------
    // Utilities
    // ---------------------------------------------------------------

    static boolean looksLikeUuid(String value) {
        if (value == null || value.length() < 32) return false;
        if (value.contains("{") || value.contains("}")) return false;
        return value.matches("[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}");
    }

    static String combineFilters(String filterA, String filterB) {
        boolean hasA = filterA != null && !filterA.isBlank();
        boolean hasB = filterB != null && !filterB.isBlank();

        if (hasA && hasB) {
            return "(" + filterA.trim() + ") AND (" + filterB.trim() + ")";
        }
        if (hasA) {
            return filterA.trim();
        }
        if (hasB) {
            return filterB.trim();
        }
        return null;
    }

    private String buildSourceTypeFilter(String sourceType) {
        String normalized = PermissionSourceCatalog.normalizeType(sourceType);
        if (normalized == null) {
            return null;
        }
        return "cin_ingestProperties." + ContentLakeIngestProperties.SOURCE_TYPE
                + " = '" + AclFilterBuilder.escapeLiteral(normalized) + "'";
    }

    /**
     * Logs the resolved permission-source-id configuration once at startup and, when
     * {@code rag.permission.source-ids} is pinned, warns if it fails to cover the sources actually
     * present in the index. A pinned value that misses an indexed source silently hides that source's
     * group/user-restricted documents (public {@code __Everyone__} docs still pass), so this turns an
     * invisible ACL failure into a visible diagnostic.
     *
     * <p>Every source type is checked, not only Alfresco: a pin was until #133 the only way to make a
     * third source retrievable at all, so a stale pin is exactly the state a deployment that used that
     * workaround is in.</p>
     */
    @PostConstruct
    void logPermissionSourceIdConfiguration() {
        if (permissionSourceIds == null || permissionSourceIds.isBlank()) {
            log.info("rag.permission.source-ids is not set; the permission filter covers every source "
                    + "discovered in the index plus the configured Alfresco and Nuxeo source ids");
            return;
        }

        List<String> configured = PermissionSourceCatalog.parsePinned(permissionSourceIds);
        log.info("rag.permission.source-ids is pinned to {}; discovery disabled", configured);

        Map<String, String> indexed = sourceCatalog().indexedSources();
        List<String> uncovered = indexed.entrySet().stream()
                .filter(entry -> !configured.contains(entry.getKey()))
                .map(entry -> (entry.getValue() == null ? "" : entry.getValue() + ":") + entry.getKey())
                .toList();
        if (!uncovered.isEmpty()) {
            log.warn("rag.permission.source-ids {} does not cover indexed sources {}; documents from the "
                            + "missing source(s) will be hidden from search results (leave the property "
                            + "unset to discover them)",
                    configured, uncovered);
        }
    }

    /**
     * The catalogue of sources to filter on: the injected bean when there is one, otherwise a private
     * instance built lazily so it is available to a unit test that never runs {@code @PostConstruct}. It
     * holds only what came from the index; the configured ids are read from this service's own fields on
     * every call, which is what keeps the two from disagreeing.
     */
    /**
     * The permission-filter builder, or one with no group resolvers when this service was constructed
     * without it. A builder with no resolvers yields the caller's default authorities for every source,
     * which is the same answer this service gave for a source it had no directory client for.
     */
    private PermissionFilterBuilder permissionFilter() {
        if (permissionFilterBuilder != null) {
            return permissionFilterBuilder;
        }
        PermissionFilterBuilder current = fallbackPermissionFilter;
        if (current == null) {
            current = PermissionFilterBuilder.withoutGroupResolvers(hxprService);
            fallbackPermissionFilter = current;
        }
        return current;
    }

    private CallerIdentityService callerIdentityService() {
        if (callerIdentityServiceBean != null) {
            return callerIdentityServiceBean;
        }
        CallerIdentityService current = fallbackCallerIdentityService;
        if (current == null) {
            current = new CallerIdentityService(securityContextService);
            fallbackCallerIdentityService = current;
        }
        return current;
    }

    /**
     * The configured source ids and the local bypass policy, read from this service's own fields on every
     * call. Passed per call rather than held by the builder, which is what keeps the two from disagreeing.
     */
    private PermissionFilterBuilder.Settings permissionSettings() {
        return new PermissionFilterBuilder.Settings(configuredSources(), adminBypassEnabled);
    }

    private PermissionSourceCatalog sourceCatalog() {
        if (permissionSourceCatalog != null) {
            return permissionSourceCatalog;
        }
        PermissionSourceCatalog current = fallbackSourceCatalog;
        if (current == null) {
            current = new PermissionSourceCatalog(hxprService);
            fallbackSourceCatalog = current;
        }
        return current;
    }

    private PermissionSourceCatalog.Configured configuredSources() {
        return PermissionSourceCatalog.Configured.of(alfrescoSourceId, nuxeoSourceId, permissionSourceIds);
    }
}
