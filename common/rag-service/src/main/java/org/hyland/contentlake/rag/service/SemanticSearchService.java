package org.hyland.contentlake.rag.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.hyland.contentlake.rag.cache.RagQueryCache;
import org.hyland.contentlake.rag.config.RagProperties;
import org.hyland.contentlake.rag.observability.RagObservations;
import org.hyland.contentlake.rag.model.SemanticSearchRequest;
import org.hyland.contentlake.rag.model.SemanticSearchResponse;
import org.hyland.contentlake.rag.security.DualSourceAuthentication;
import org.hyland.contentlake.rag.model.SemanticSearchResponse.ChunkMetadata;
import org.hyland.contentlake.rag.model.SemanticSearchResponse.SearchHit;
import org.hyland.contentlake.rag.model.SemanticSearchResponse.SourceDocument;
import org.hyland.contentlake.security.AclFilterBuilder;
import org.hyland.contentlake.security.GroupResolutionFailurePolicy;
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
import org.springframework.http.*;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import java.util.regex.Pattern;
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

    private static final int MAX_TOP_K = 50;

    // The sys_racl predicate itself lives in AclFilterBuilder, which owns every constant and every
    // clause it is made of. GROUP_PREFIX stays here because it is also used to normalise Nuxeo group
    // names, which is not an ACL concern.
    private static final String GROUP_PREFIX = "GROUP_";
    private static final Pattern SOURCE_ID_EQUALS_PATTERN = Pattern.compile("cin_sourceId\\s*=\\s*'([^']+)'");

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

    private static final int SOURCE_DISCOVERY_LIMIT = 25;

    @Value("${alfresco.source-id:}")
    private String alfrescoSourceId;

    @Value("${rag.permission.source-ids:}")
    private String permissionSourceIds;

    @Value("${rag.security.group-resolution-failure:fail-closed}")
    private String groupResolutionFailureMode;

    @Value("${rag.security.admin-bypass.enabled:false}")
    private boolean adminBypassEnabled;

    @Value("${nuxeo.source-id:}")
    private String nuxeoSourceId;

    @Value("${nuxeo.base-url:http://localhost:8081/nuxeo}")
    private String nuxeoUrl;

    @Value("${nuxeo.username:Administrator}")
    private String nuxeoUsername;

    @Value("${nuxeo.password:Administrator}")
    private String nuxeoPassword;

    @Value("${content.service.url}")
    private String alfrescoUrl;

    @Value("${content.service.security.basicAuth.username}")
    private String serviceAccountUsername;

    @Value("${content.service.security.basicAuth.password}")
    private String serviceAccountPassword;

    @Value("${semantic-search.default-min-score:" + FALLBACK_MIN_SCORE + "}")
    private double defaultMinScore;

    private volatile List<String> cachedAlfrescoSourceIds;

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
                + ' ' + request.getMinScore()
                + ' ' + request.getFilter()
                + ' ' + request.getSourceType()
                + ' ' + request.getEmbeddingType()
                + ' ' + request.getNamedQuery();
    }

    private SemanticSearchResponse executeSearch(SemanticSearchRequest request) {
        long startTime = System.currentTimeMillis();

        int topK = Math.min(Math.max(request.getTopK(), 1), MAX_TOP_K);

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
                    QueryVariant.original(request.getQuery()), request, topK, minScore, hxqlFilter, logUser);
            long searchTimeMs = System.currentTimeMillis() - startTime;
            log.info("Semantic search completed: {} results in {}ms for query: \"{}\" (minScore={})",
                    single.hits().size(), searchTimeMs, request.getQuery(), minScore);
            return response(request, single.hits(), single.vectorDimension(), single.totalCount(), searchTimeMs);
        }

        List<List<SearchHit>> perVariant = new ArrayList<>(variants.size());
        int vectorDimension = 0;
        long totalCount = 0;
        for (QueryVariant variant : variants) {
            VariantResult result = searchVariant(variant, request, topK, minScore, hxqlFilter, logUser);
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

        List<SearchHit> hits = RrfFusion.fuse(perVariant, ragProperties.getQueryExpansion().getRrfK(), topK);
        long searchTimeMs = System.currentTimeMillis() - startTime;

        log.info("Semantic search completed: {} results in {}ms for query: \"{}\" "
                        + "(minScore={}, variants={}, contributing={})",
                hits.size(), searchTimeMs, request.getQuery(), minScore, variants.size(), perVariant.size());

        return response(request, hits, vectorDimension, totalCount, searchTimeMs);
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
        if (auth instanceof DualSourceAuthentication dual) {
            return buildPermissionFilter(
                    dual.getAlfrescoUsername(), dual.getNuxeoUsername(),
                    request.getSourceType(), additionalFilter);
        }
        String username = securityContextService.getCurrentUsername();
        return buildPermissionFilter(username, request.getSourceType(), additionalFilter);
    }

    /**
     * Builds the ACL permission filter for the <em>current</em> authenticated principal, combined with
     * an optional additional HXQL predicate. Resolves identity from the {@link SecurityContextHolder}
     * exactly as {@link #search} does, so ACL-scoped tool/MCP operations (#65, #61) cannot bypass
     * {@code sys_racl}. Returns a complete HXQL query ({@code SELECT ... WHERE ...}).
     */
    public String currentUserPermissionFilter(String sourceType, String additionalFilter) {
        Authentication auth = SecurityContextHolder.getContext().getAuthentication();
        if (auth instanceof DualSourceAuthentication dual) {
            return buildPermissionFilter(dual.getAlfrescoUsername(), dual.getNuxeoUsername(),
                    sourceType, additionalFilter);
        }
        return buildPermissionFilter(securityContextService.getCurrentUsername(), sourceType, additionalFilter);
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

    /** One retrieval pass for one query variant: embed, kNN, enrich, threshold. */
    private VariantResult searchVariant(QueryVariant variant,
                                        SemanticSearchRequest request,
                                        int topK,
                                        double minScore,
                                        Supplier<String> hxqlFilter,
                                        String logUser) {
        // A variant may arrive with its own vector (HyDE embeds document-side, without the query
        // instruction prefix); otherwise embed query-side as usual.
        List<Double> queryVector = variant.vectorVector();
        if (queryVector == null) {
            log.info("Embedding query: \"{}\" (variant={}, topK={}, minScore={}, user={})",
                    variant.vectorText(), variant.label(), topK, minScore, logUser);
            queryVector = embedQueryCached(variant.vectorText(), request.getEmbeddingType());
        }

        if (queryVector == null || queryVector.isEmpty()) {
            log.warn("Empty embedding vector for query: {}", variant.vectorText());
            return VariantResult.empty(0);
        }

        String filter = hxqlFilter.get();
        log.debug("Executing vector search with filter: {}", filter);
        final List<Double> vector = queryVector;
        VectorSearchResult vectorResult = traced("rag.search.vector",
                () -> vectorLeg(variant, vector, request.getEmbeddingType(), filter, topK));

        if (vectorResult == null || vectorResult.getEmbeddings() == null || vectorResult.getEmbeddings().isEmpty()) {
            log.info("No results for query: \"{}\"", variant.vectorText());
            return VariantResult.empty(queryVector.size());
        }

        Map<String, SectionMap> sectionMaps = new ConcurrentHashMap<>();
        Map<String, SourceDocument> documentCache = fetchDocumentMetadata(vectorResult.getEmbeddings(), sectionMaps);
        List<SearchHit> hits = buildSearchHits(vectorResult.getEmbeddings(), documentCache, sectionMaps, minScore);
        long totalCount = vectorResult.getTotalCount() != null ? vectorResult.getTotalCount() : hits.size();

        return new VariantResult(hits, queryVector.size(), totalCount);
    }

    private SemanticSearchResponse response(SemanticSearchRequest request,
                                            List<SearchHit> hits,
                                            int vectorDimension,
                                            long totalCount,
                                            long searchTimeMs) {
        return SemanticSearchResponse.builder()
                .query(request.getQuery())
                .model(embeddingService.getModelName())
                .vectorDimension(vectorDimension)
                .resultCount(hits.size())
                .totalCount(totalCount)
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
    // Permission filter (sys_racl)
    // ---------------------------------------------------------------

    String buildPermissionFilter(String username, String additionalFilter) {
        return buildPermissionFilter(username, null, additionalFilter);
    }

    /**
     * Dual-auth variant: routes Alfresco sources to {@code alfrescoUser} and Nuxeo sources
     * to {@code nuxeoUser}. Sources whose corresponding user is {@code null} are excluded
     * (the caller has not authenticated against that repository).
     */
    String buildPermissionFilter(String alfrescoUser, String nuxeoUser,
                                 String sourceType, String additionalFilter) {
        List<String> sourceIds = resolvePermissionSourceIds(sourceType, additionalFilter);
        Map<String, List<String>> authoritiesBySource =
                resolveAuthoritiesByDualSource(alfrescoUser, nuxeoUser, sourceIds);

        List<String> sourceClauses = new ArrayList<>();
        for (String sourceId : sourceIds) {
            String username = isNuxeoSource(sourceId) ? nuxeoUser : alfrescoUser;
            if (username == null) {
                // Not authenticated against this source, so exclude it from results entirely.
                continue;
            }
            List<String> authorities = authoritiesBySource.get(sourceId);
            if (authorities == null || authorities.isEmpty()) {
                // Authorities unresolved rather than empty. Substituting a default here would undo the
                // fail-closed decision taken in getUserAuthorities.
                log.warn("Excluding source {} from the permission filter for user {}: no authorities resolved",
                        sourceId, username);
                continue;
            }
            sourceClauses.add(sourcePermissionClause(sourceId, authorities));
        }

        log.debug("Dual-auth permission filter: alfrescoUser={}, nuxeoUser={}, sourceIds={}",
                alfrescoUser, nuxeoUser, sourceIds);

        if (sourceClauses.isEmpty()) {
            log.warn("No permission clauses resolved (alfrescoUser={}, nuxeoUser={}, sourceType={}, filter={})",
                    alfrescoUser, nuxeoUser, sourceType, additionalFilter);
        }

        return AclFilterBuilder.query(sourceClauses, additionalFilter);
    }

    Map<String, List<String>> resolveAuthoritiesByDualSource(String alfrescoUser, String nuxeoUser,
                                                             List<String> sourceIds) {
        Map<String, List<String>> authoritiesBySource = new LinkedHashMap<>();
        for (String sourceId : sourceIds) {
            String username = isNuxeoSource(sourceId) ? nuxeoUser : alfrescoUser;
            if (username != null) {
                authoritiesBySource.put(sourceId, getUserAuthorities(username, sourceId));
            }
        }
        return authoritiesBySource;
    }

    String buildPermissionFilter(String username, String sourceType, String additionalFilter) {
        List<String> sourceIds = resolvePermissionSourceIds(sourceType, additionalFilter);
        Map<String, List<String>> authoritiesBySource = resolveAuthoritiesBySource(username, sourceIds);

        List<String> sourceClauses = new ArrayList<>();
        for (String sourceId : sourceIds) {
            List<String> authorities = authoritiesBySource.get(sourceId);
            if (authorities == null || authorities.isEmpty()) {
                log.warn("Excluding source {} from the permission filter for user {}: no authorities resolved",
                        sourceId, username);
                continue;
            }
            sourceClauses.add(sourcePermissionClause(sourceId, authorities));
        }

        log.debug("Permission filter with source-scoped authorities for user {} (sourceIds={})", username, sourceIds);

        if (sourceClauses.isEmpty()) {
            log.warn("No permission source ids resolved for user {} (sourceType={}, additionalFilter={})",
                    username, sourceType, additionalFilter);
        }

        return AclFilterBuilder.query(sourceClauses, additionalFilter);
    }

    Map<String, List<String>> resolveAuthoritiesBySource(String username, List<String> sourceIds) {
        Map<String, List<String>> authoritiesBySource = new LinkedHashMap<>();
        for (String sourceId : sourceIds) {
            authoritiesBySource.put(sourceId, getUserAuthorities(username, sourceId));
        }
        return authoritiesBySource;
    }

    /**
     * The caller's authorities on one source, or an empty list when they could not be resolved and the
     * configured policy is to fail closed. An empty list is not "no groups": it means the answer is
     * unknown, and {@link #buildPermissionFilter(String, String, String)} drops the source instead of
     * guessing.
     */
    List<String> getUserAuthorities(String username, String sourceId) {
        LinkedHashSet<String> authorities =
                new LinkedHashSet<>(AclFilterBuilder.defaultAuthorities(username));
        try {
            if (isAlfrescoSource(sourceId)) {
                authorities.addAll(fetchAlfrescoGroups(username));
            } else if (isNuxeoSource(sourceId)) {
                authorities.addAll(fetchNuxeoGroups(username));
            }
            log.debug("Resolved {} authorities for user {} on source {}", authorities.size(), username, sourceId);
        } catch (Exception e) {
            return onGroupResolutionFailure(username, sourceId, e);
        }

        return List.copyOf(authorities);
    }

    /**
     * Applies {@code rag.security.group-resolution-failure}. Both modes log at WARN: a directory outage
     * is worth knowing about whichever behaviour is configured, because in one mode the caller silently
     * loses group-granted documents and in the other they lose the source.
     */
    private List<String> onGroupResolutionFailure(String username, String sourceId, Exception cause) {
        GroupResolutionFailurePolicy policy =
                GroupResolutionFailurePolicy.parse(groupResolutionFailureMode);
        if (policy == GroupResolutionFailurePolicy.DEGRADE) {
            log.warn("Failed to resolve authorities for user {} on source {}; policy is {}, so proceeding "
                            + "with username + GROUP_EVERYONE and no group-granted access: {}",
                    username, sourceId, policy, cause.getMessage());
            return AclFilterBuilder.defaultAuthorities(username);
        }
        log.warn("Failed to resolve authorities for user {} on source {}; policy is {}, so the source is "
                        + "excluded from the permission filter and the caller sees nothing from it: {}",
                username, sourceId, policy, cause.getMessage());
        return List.of();
    }

    @SuppressWarnings("unchecked")
    private List<String> fetchAlfrescoGroups(String username) {
        RestTemplate restTemplate = new RestTemplate();

        String url = alfrescoUrl
                + "/alfresco/api/-default-/public/alfresco/versions/1/people/"
                + username + "/groups?skipCount=0&maxItems=1000";

        HttpHeaders headers = new HttpHeaders();
        headers.setAccept(List.of(MediaType.APPLICATION_JSON));
        headers.setBasicAuth(serviceAccountUsername, serviceAccountPassword);

        ResponseEntity<Map> response = restTemplate.exchange(
                url, HttpMethod.GET, new HttpEntity<>(headers), Map.class);

        LinkedHashSet<String> groups = new LinkedHashSet<>();
        if (response.getBody() != null) {
            Map<String, Object> body = response.getBody();
            Map<String, Object> list = (Map<String, Object>) body.get("list");
            if (list != null) {
                List<Map<String, Object>> entries = (List<Map<String, Object>>) list.get("entries");
                if (entries != null) {
                    for (Map<String, Object> entry : entries) {
                        Map<String, Object> entryData = (Map<String, Object>) entry.get("entry");
                        if (entryData != null && entryData.get("id") != null) {
                            groups.add((String) entryData.get("id"));
                        }
                    }
                }
            }
        }

        log.debug("Retrieved {} groups for user {}", groups.size(), username);
        return List.copyOf(groups);
    }

    @SuppressWarnings("unchecked")
    private List<String> fetchNuxeoGroups(String username) {
        RestTemplate restTemplate = new RestTemplate();
        HttpHeaders headers = new HttpHeaders();
        headers.setAccept(List.of(MediaType.APPLICATION_JSON));
        headers.setBasicAuth(nuxeoUsername, nuxeoPassword);

        ResponseEntity<Map> response = restTemplate.exchange(
                buildNuxeoApiUrl() + "/user/{username}",
                HttpMethod.GET,
                new HttpEntity<>(headers),
                Map.class,
                username
        );

        LinkedHashSet<String> groups = new LinkedHashSet<>();
        if (response.getBody() == null) {
            return List.of();
        }

        Map<String, Object> body = response.getBody();
        Object directGroups = body.get("groups");
        if (directGroups instanceof List<?> values) {
            values.forEach(value -> addNuxeoGroup(groups, value));
        }

        Object extendedGroups = body.get("extendedGroups");
        if (extendedGroups instanceof List<?> values) {
            for (Object value : values) {
                if (value instanceof Map<?, ?> map) {
                    addNuxeoGroup(groups, firstString(map.get("name"), map.get("groupname"), map.get("id")));
                }
            }
        }

        Object propertiesObject = body.get("properties");
        if (propertiesObject instanceof Map<?, ?> properties) {
            Object propertyGroups = properties.get("groups");
            if (propertyGroups instanceof List<?> values) {
                values.forEach(value -> addNuxeoGroup(groups, value));
            }
        }

        log.debug("Retrieved {} Nuxeo groups for user {}", groups.size(), username);
        return List.copyOf(groups);
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

    private List<SearchHit> buildSearchHits(List<Embedding> embeddings,
                                            Map<String, SourceDocument> documentCache,
                                            Map<String, SectionMap> sectionMaps,
                                            double minScore) {
        List<SearchHit> hits = new ArrayList<>();
        int rank = 1;

        if (log.isDebugEnabled()) {
            long wouldFilter = embeddings.stream()
                    .filter(e -> (e.getSysembedScore() != null ? e.getSysembedScore() : 0.0) < minScore)
                    .count();
            log.debug("Score filter: minScore={} candidates={} filtered={} passing={}",
                    minScore, embeddings.size(), wouldFilter, embeddings.size() - wouldFilter);
        }

        int candidateIndex = 0;
        for (Embedding embedding : embeddings) {
            double score = embedding.getSysembedScore() != null ? embedding.getSysembedScore() : 0.0;
            candidateIndex++;

            if (log.isDebugEnabled()) {
                String preview = embedding.getSysembedText() != null
                        ? embedding.getSysembedText().substring(0, Math.min(60, embedding.getSysembedText().length()))
                        : "";
                log.debug("  Candidate [{}] docId={} score={} {}\"{}...\"",
                        candidateIndex, embedding.getSysembedDocId(), String.format("%.3f", score),
                        score < minScore ? "[FILTERED] " : "", preview);
            }

            if (score < minScore) {
                continue;
            }

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
        String normalized = normalizeSourceType(sourceType);
        if (normalized == null) {
            return null;
        }
        return "cin_ingestProperties." + ContentLakeIngestProperties.SOURCE_TYPE
                + " = '" + AclFilterBuilder.escapeLiteral(normalized) + "'";
    }

    /**
     * The per-source ACL predicate. The bypass argument is the local policy decision: the
     * administrator group grants full access only when {@code rag.security.admin-bypass.enabled} is
     * set and only on an Alfresco source. The predicate itself belongs to {@link AclFilterBuilder}.
     */
    private String sourcePermissionClause(String sourceId, List<String> authorities) {
        return AclFilterBuilder.sourcePermissionClause(
                sourceId, formatSourceId(sourceId), authorities,
                adminBypassEnabled && isAlfrescoSource(sourceId));
    }

    private String formatSourceId(String sourceId) {
        if (isAlfrescoSource(sourceId)) {
            return "alfresco:" + sourceId;
        }
        if (isNuxeoSource(sourceId)) {
            return "nuxeo:" + sourceId;
        }
        return sourceId;
    }

    /**
     * Logs the resolved permission-source-id configuration once at startup and, when
     * {@code rag.permission.source-ids} is pinned, warns if it fails to cover the source ids
     * actually present in the index. A pinned value that misses an indexed source id silently
     * hides that source's group/user-restricted documents (public {@code __Everyone__} docs still
     * pass), so this turns an invisible ACL failure into a visible diagnostic.
     */
    @PostConstruct
    void logPermissionSourceIdConfiguration() {
        if (permissionSourceIds == null || permissionSourceIds.isBlank()) {
            log.info("rag.permission.source-ids is not set; permission filter uses auto-discovered "
                    + "Alfresco source ids plus configured Nuxeo source id");
            return;
        }

        LinkedHashSet<String> configured = new LinkedHashSet<>();
        for (String candidate : permissionSourceIds.split(",")) {
            addSourceId(configured, candidate);
        }
        log.info("rag.permission.source-ids is pinned to {}; auto-discovery disabled", configured);

        List<String> indexedAlfresco = discoverSourceIdsByType("alfresco");
        List<String> uncovered = indexedAlfresco.stream()
                .filter(id -> !configured.contains(id))
                .toList();
        if (!uncovered.isEmpty()) {
            log.warn("rag.permission.source-ids {} does not cover indexed Alfresco source ids {}; "
                            + "group/user-restricted documents from the missing source(s) will be hidden "
                            + "from search results (leave the property unset to auto-discover)",
                    configured, uncovered);
        }
    }

    private List<String> resolvePermissionSourceIds(String sourceType, String additionalFilter) {
        LinkedHashSet<String> sourceIds = new LinkedHashSet<>();

        if (additionalFilter != null && !additionalFilter.isBlank()) {
            var matcher = SOURCE_ID_EQUALS_PATTERN.matcher(additionalFilter);
            while (matcher.find()) {
                addSourceId(sourceIds, matcher.group(1));
            }
        }

        if (!sourceIds.isEmpty()) {
            return List.copyOf(sourceIds);
        }

        addSourceIdsForType(sourceIds, sourceType);
        if (!sourceIds.isEmpty()) {
            return List.copyOf(sourceIds);
        }

        if (permissionSourceIds != null && !permissionSourceIds.isBlank()) {
            for (String candidate : permissionSourceIds.split(",")) {
                addSourceId(sourceIds, candidate);
            }
        } else {
            addAlfrescoSourceIds(sourceIds);
            addSourceId(sourceIds, nuxeoSourceId);
        }

        return List.copyOf(sourceIds);
    }

    private void addSourceIdsForType(Set<String> sourceIds, String sourceType) {
        String normalized = normalizeSourceType(sourceType);
        if ("alfresco".equals(normalized)) {
            addAlfrescoSourceIds(sourceIds);
        } else if ("nuxeo".equals(normalized)) {
            addSourceId(sourceIds, nuxeoSourceId);
        }
    }

    private void addAlfrescoSourceIds(Set<String> sourceIds) {
        resolveAlfrescoSourceIds().forEach(sourceId -> addSourceId(sourceIds, sourceId));
    }

    private List<String> resolveAlfrescoSourceIds() {
        LinkedHashSet<String> sourceIds = new LinkedHashSet<>();
        addSourceId(sourceIds, alfrescoSourceId);
        if (!sourceIds.isEmpty()) {
            return List.copyOf(sourceIds);
        }

        List<String> cached = cachedAlfrescoSourceIds;
        if (cached != null && !cached.isEmpty()) {
            return cached;
        }

        List<String> discovered = discoverSourceIdsByType("alfresco");
        if (!discovered.isEmpty()) {
            cachedAlfrescoSourceIds = discovered;
        }
        return discovered;
    }

    private List<String> discoverSourceIdsByType(String sourceType) {
        try {
            String hxql = "SELECT * FROM SysContent WHERE cin_ingestProperties."
                    + ContentLakeIngestProperties.SOURCE_TYPE
                    + " = '" + AclFilterBuilder.escapeLiteral(sourceType) + "'";
            HxprDocument.QueryResult result = hxprService.query(hxql, SOURCE_DISCOVERY_LIMIT, 0);
            if (result == null || result.getDocuments() == null) {
                return List.of();
            }

            LinkedHashSet<String> sourceIds = new LinkedHashSet<>();
            for (HxprDocument doc : result.getDocuments()) {
                addSourceId(sourceIds, doc.getCinSourceId());
            }

            if (!sourceIds.isEmpty()) {
                log.debug("Discovered permission source ids for {}: {}", sourceType, sourceIds);
            }
            return List.copyOf(sourceIds);
        } catch (Exception e) {
            log.warn("Failed to discover permission source ids for {}: {}", sourceType, e.getMessage());
            return List.of();
        }
    }

    private String normalizeSourceType(String sourceType) {
        if (sourceType == null) {
            return null;
        }
        String trimmed = sourceType.trim().toLowerCase(Locale.ROOT);
        return trimmed.isBlank() ? null : trimmed;
    }

    private static void addSourceId(Set<String> sourceIds, String candidate) {
        if (candidate == null) {
            return;
        }
        String trimmed = candidate.trim();
        if (trimmed.isBlank()) {
            return;
        }
        int separator = trimmed.indexOf(':');
        sourceIds.add(separator >= 0 && separator < trimmed.length() - 1
                ? trimmed.substring(separator + 1)
                : trimmed);
    }

    private boolean isAlfrescoSource(String sourceId) {
        return sourceId != null && resolveAlfrescoSourceIds().contains(sourceId);
    }

    private boolean isNuxeoSource(String sourceId) {
        return sourceId != null && !sourceId.isBlank() && sourceId.equals(nuxeoSourceId);
    }

    private String buildNuxeoApiUrl() {
        String trimmed = nuxeoUrl.endsWith("/") ? nuxeoUrl.substring(0, nuxeoUrl.length() - 1) : nuxeoUrl;
        return trimmed.endsWith("/api/v1") ? trimmed : trimmed + "/api/v1";
    }

    private void addNuxeoGroup(Set<String> groups, Object candidate) {
        if (candidate == null) {
            return;
        }
        String group = candidate.toString().trim();
        if (group.isBlank()) {
            return;
        }
        groups.add(group.startsWith(GROUP_PREFIX) ? group : GROUP_PREFIX + group);
    }

    private String firstString(Object... candidates) {
        for (Object candidate : candidates) {
            if (candidate instanceof String value && !value.isBlank()) {
                return value;
            }
        }
        return null;
    }
}
