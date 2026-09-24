package org.hyland.contentlake.rag.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.hyland.contentlake.client.HxprService;
import org.hyland.contentlake.client.NamedQueryService;
import org.hyland.contentlake.client.VocabularyService;
import org.hyland.contentlake.hxpr.api.model.Embedding;
import org.hyland.contentlake.hxpr.api.model.VectorSearchResult;
import org.hyland.contentlake.model.ContentLakeIngestProperties;
import org.hyland.contentlake.model.HxprDocument;
import org.hyland.contentlake.model.SectionMap;
import org.hyland.contentlake.rag.cache.RagQueryCache;
import org.hyland.contentlake.rag.config.HybridSearchProperties;
import org.hyland.contentlake.rag.config.RagProperties;
import org.hyland.contentlake.rag.observability.RagObservations;
import org.hyland.contentlake.rag.model.HybridSearchRequest;
import org.hyland.contentlake.rag.security.DualSourceAuthentication;
import org.hyland.contentlake.rag.model.HybridSearchResponse;
import org.hyland.contentlake.rag.model.HybridSearchResponse.HybridHit;
import org.hyland.contentlake.rag.model.SemanticSearchResponse.ChunkMetadata;
import org.hyland.contentlake.rag.model.SemanticSearchResponse.SourceDocument;
import org.hyland.contentlake.rag.security.SourceGroupResolverRegistry;
import org.hyland.contentlake.security.AclFilterBuilder;
import org.hyland.contentlake.security.SecurityContextService;
import org.hyland.contentlake.service.EmbeddingService;
import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Service for hybrid search combining vector (semantic) and keyword (fulltext) retrieval.
 *
 * <p>Supports two fusion strategies:
 * <ul>
 *   <li><strong>RRF (Reciprocal Rank Fusion)</strong> — Merges results by rank position using
 *       the formula {@code 1 / (k + rank)}. Score-scale agnostic.</li>
 *   <li><strong>Weighted</strong> — Normalises vector and keyword scores to [0,1] then
 *       computes {@code vectorWeight * vectorScore + textWeight * keywordScore}.</li>
 * </ul>
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class HybridSearchService {

    static final String STRATEGY_RRF = "rrf";
    static final String STRATEGY_WEIGHTED = "weighted";
    static final String NORMALIZATION_MAX = "max";
    static final String NORMALIZATION_MINMAX = "minmax";

    /** Caller-visible ceiling on {@code candidateCount} and {@code maxResults}. Part of the published contract. */
    private static final int MAX_CANDIDATE_COUNT = 100;

    /** Caller-visible ceiling on {@code topDocuments}, matching the semantic endpoint's. */
    private static final int MAX_TOP_DOCUMENTS = 50;

    /** Caller-visible ceiling on {@code chunksPerDocument}. */
    private static final int MAX_CHUNKS_PER_DOCUMENT = 10;

    /** Ceiling on the chunks a document budget may return, whatever its two factors multiply to. */
    private static final int MAX_CHUNK_LIMIT = 200;

    /**
     * Internal ceiling on the candidate pool, deliberately far above {@link #MAX_CANDIDATE_COUNT}.
     *
     * <p>A document budget is unsatisfiable out of a pool capped at the same number as the answer, so a
     * request that names one raises the candidates the legs retrieve. This bounds retrieval only; what a
     * caller may ask for, and what the response may carry, is unchanged.</p>
     */
    private static final int MAX_CANDIDATE_FETCH = 500;

    private static final String INGEST_PROP_PREFIX = "cin_ingestProperties.";
    private static final String SOURCE_MIME_PROP = INGEST_PROP_PREFIX + ContentLakeIngestProperties.SOURCE_MIME_TYPE;
    private static final String SOURCE_PATH_PROP = INGEST_PROP_PREFIX + ContentLakeIngestProperties.SOURCE_PATH;
    private static final String SOURCE_MODIFIED_PROP = INGEST_PROP_PREFIX + ContentLakeIngestProperties.SOURCE_MODIFIED_AT;
    /**
     * hxpr's analysed full-text index. Matches anywhere in the document and case-insensitively,
     * unlike a {@code LIKE} against the ingest property, whose index truncates at 256 characters.
     */
    private static final String FULLTEXT_FIELD = "sys_fulltext";
    /** Below this length a term matches so much of the corpus that it only costs query time. */
    private static final int MIN_KEYWORD_TERM_LENGTH = 3;
    /** Each term is one OR-ed LIKE; a long question would otherwise build an unbounded query. */
    private static final int MAX_KEYWORD_TERMS = 12;
    /**
     * Terms too common to discriminate. Deliberately short: an aggressive list would discard terms
     * that genuinely do select documents, and this leg exists to catch the literal ones.
     */
    private static final Set<String> KEYWORD_STOP_WORDS = Set.of(
            "and", "are", "but", "can", "did", "does", "for", "has", "have", "how", "its",
            "may", "not", "the", "that", "this", "was", "were", "what", "when", "which", "who",
            "why", "with", "you", "your", "from", "into", "than", "them", "then", "they", "will");
    private static final Pattern CUSTOM_PROP_KEY_PATTERN = Pattern.compile("[A-Za-z0-9_:-]+");

    private final HxprService hxprService;
    private final EmbeddingService embeddingService;
    private final SecurityContextService securityContextService;
    private final HybridSearchProperties properties;
    private final SourceMetadataResolver sourceMetadataResolver;
    private final SectionMapResolver sectionMapResolver;
    private final QueryExpansionService queryExpansionService;
    private final RagProperties ragProperties;
    private final NamedQueryService namedQueryService;
    private final VocabularyService vocabularyService;
    /**
     * Optional (#121): the vector leg over every embedding type the corpus holds.
     *
     * <p>Null in unit tests that construct this service without it, and in that case the vector leg
     * makes the same single call it always did. With one active type the two paths are identical by
     * construction, so a single-model corpus behaves the same either way.</p>
     */
    private final MultiTypeVectorSearchService multiTypeVectorSearch;
    /** Optional (#72): null in unit tests that construct this service without the cache collaborator. */
    private final RagQueryCache queryCache;
    /** Optional (#73): null in unit tests that construct this service without the tracing collaborator. */
    private final RagObservations observations;
    /**
     * Group expansion per source (#143). Optional: null in unit tests that construct this service without
     * it, where {@link #groupResolvers()} falls back to a registry with no resolvers.
     */
    private final SourceGroupResolverRegistry groupResolverRegistry;
    /**
     * The shared source catalogue. Optional: null in unit tests, where {@link #sourceCatalog()} builds a
     * private one.
     */
    private final PermissionSourceCatalog permissionSourceCatalog;

    @Value("${alfresco.source-id:}")
    private String alfrescoSourceId;

    @Value("${rag.permission.source-ids:}")
    private String permissionSourceIds;

    @Value("${rag.security.admin-bypass.enabled:false}")
    private boolean adminBypassEnabled;

    @Value("${nuxeo.source-id:}")
    private String nuxeoSourceId;

    /** Built on first use by {@link #sourceCatalog()} when no bean was injected. */
    private volatile PermissionSourceCatalog fallbackSourceCatalog;

    /**
     * Executes a hybrid search: runs vector and keyword legs in sequence, then fuses the results
     * using the configured (or overridden) strategy.
     *
     * <p>When the query cache (#72) is enabled, an identical query+filters+principal combination seen
     * within the TTL window returns the cached response without re-embedding or re-querying hxpr. The
     * cache key includes the caller's principal scope, so results never cross ACL contexts.</p>
     */
    public HybridSearchResponse search(HybridSearchRequest request) {
        boolean cacheOn = queryCache != null && queryCache.isEnabled();
        String cacheKey = cacheOn ? buildCacheKey(request) : null;
        if (cacheKey != null) {
            HybridSearchResponse cached = queryCache.getResult(cacheKey);
            if (cached != null) {
                log.debug("Hybrid search cache hit for query \"{}\"", request.getQuery());
                return cached;
            }
        }

        HybridSearchResponse response = executeSearch(request);

        if (cacheKey != null) {
            queryCache.putResult(cacheKey, response);
        }
        return response;
    }

    /** Builds the ACL-scoped, filter-aware cache key for a request (see {@link RagQueryCache}). */
    private String buildCacheKey(HybridSearchRequest request) {
        Authentication auth = SecurityContextHolder.getContext().getAuthentication();
        return "hyb" + ' ' + RagQueryCache.principalScope(auth)
                + ' ' + RagQueryCache.normalize(request.getQuery())
                + ' ' + request.getFilter()
                + ' ' + request.getSourceType()
                + ' ' + request.getEmbeddingType()
                + ' ' + request.getNamedQuery()
                + ' ' + request.getStrategy()
                + ' ' + request.getNormalization()
                + ' ' + request.getVectorWeight()
                + ' ' + request.getTextWeight()
                + ' ' + request.getCandidateCount()
                + ' ' + request.getMaxResults()
                + ' ' + request.getTopDocuments()
                + ' ' + request.getChunksPerDocument()
                + ' ' + request.getMinScore()
                + ' ' + buildMetadataFilter(request.getMetadata());
    }

    private HybridSearchResponse executeSearch(HybridSearchRequest request) {
        long startTime = System.currentTimeMillis();

        String sourceTypeFilter = buildSourceTypeFilter(request.getSourceType());
        String metadataFilter = buildMetadataFilter(request.getMetadata());
        String additionalFilter = combineFilters(request.getFilter(), metadataFilter);
        additionalFilter = combineFilters(additionalFilter, sourceTypeFilter);
        // A named query, when supplied, resolves server-side to an HXQL fragment that scopes the
        // search alongside any inline filter; no-op when absent.
        additionalFilter = combineFilters(additionalFilter, namedQueryService.resolveFilter(request.getNamedQuery()));
        Authentication auth = SecurityContextHolder.getContext().getAuthentication();
        String logUser = auth != null ? auth.getName() : "anonymous";
        String permissionFilter = buildCurrentUserPermissionFilter(request.getSourceType(), additionalFilter);

        ResultBudget budget = resolveBudget(request);
        int candidateCount = budget.candidateCount();
        String strategy = resolveStrategy(request);
        String normalization = STRATEGY_WEIGHTED.equals(strategy) ? resolveNormalization(request) : null;
        // An explicit 0.0 means "no threshold" and must survive; only an absent value falls back.
        double minScore = request.getMinScore() != null
                ? request.getMinScore()
                : properties.getDefaultMinScore();

        // --- Expand into query variants (no-op unless multi-query, HyDE or decomposition is on) ---
        List<QueryVariant> expanded = expand(request.getQuery());
        // The verbatim-identifier pass (#122) is appended on top, and is itself a no-op unless the flag
        // is on and the query names an identifier.
        final List<QueryVariant> passes = withVerbatimPass(
                expanded != null ? expanded : List.of(QueryVariant.original(request.getQuery())),
                request);

        // --- Retrieve and leg-fuse once per variant ---
        List<List<FusedResult>> perVariant = new ArrayList<>(passes.size());
        int vectorCandidates = 0;
        int keywordCandidates = 0;
        for (QueryVariant variant : passes) {
            VariantResult result = searchVariant(
                    variant, request, permissionFilter, candidateCount, strategy, normalization, minScore, logUser);
            vectorCandidates += result.vectorCount();
            keywordCandidates += result.keywordCount();
            if (!result.fused().isEmpty()) {
                perVariant.add(result.fused());
            }
        }

        // --- Fuse across variants ---
        // Rank-based, deliberately: a vector-only variant such as HyDE has no keyword contribution, so
        // under weighted fusion its scores top out at vectorWeight while a both-legs variant reaches
        // 1.0. Fusing on rank makes that scale difference irrelevant.
        List<FusedResult> fused = perVariant.size() <= 1
                ? (perVariant.isEmpty() ? List.<FusedResult>of() : perVariant.get(0))
                : fuseAcrossVariants(perVariant, ragProperties.getQueryExpansion().getRrfK());

        // Apply whichever budget the caller asked in. With a document budget the answer is whole documents
        // and may be shorter than the chunk budget allows; without one, cap how much of the result set a
        // single document may occupy before trimming to maxResults. The candidate pool is already deeper
        // than the answer, so there is no separate over-fetch here: under the cap a document's chunks past
        // it are deferred behind other documents' best ones, never dropped. A no-op when the cap is off,
        // which is the default.
        List<FusedResult> filtered = budget.documentBudget()
                ? DocumentGroupSelector.selectGroups(
                        fused, budget.topDocuments(), budget.chunksPerDocument(),
                        HybridSearchService::documentKeyOf)
                : DocumentDiversityLimiter.cap(
                        fused, budget.chunkLimit(), maxChunksPerDocument(request),
                        HybridSearchService::documentKeyOf);

        // --- Enrich with document metadata ---
        Map<String, SectionMap> sectionMaps = new ConcurrentHashMap<>();
        Map<String, SourceDocument> docCache = fetchDocumentMetadata(filtered, sectionMaps);

        // --- Build response ---
        List<HybridHit> hits = buildHits(filtered, docCache, sectionMaps);

        long searchTimeMs = System.currentTimeMillis() - startTime;
        log.info("Hybrid search completed: {} results in {}ms (strategy={}, vector={}, keyword={}, variants={})",
                hits.size(), searchTimeMs, strategy, vectorCandidates, keywordCandidates, passes.size());

        return HybridSearchResponse.builder()
                .query(request.getQuery())
                .strategy(strategy)
                .normalization(normalization)
                .model(embeddingService.getModelName())
                .resultCount(hits.size())
                .vectorCandidates(vectorCandidates)
                .keywordCandidates(keywordCandidates)
                .queryVariants(passes.size() > 1 ? passes.size() : null)
                .documentCount(DocumentGroupSelector.distinctGroups(filtered, HybridSearchService::documentKeyOf))
                .appliedTopDocuments(budget.topDocuments())
                .appliedChunksPerDocument(budget.documentBudget() ? budget.chunksPerDocument() : null)
                .searchTimeMs(searchTimeMs)
                .results(hits)
                .build();
    }

    /** Embeds a query, caching the vector (#72) and spanning the embedding call (#73) when enabled. */
    private List<Double> embedQueryCached(String text, String embeddingType) {
        java.util.function.Supplier<List<Double>> loader =
                () -> traced("rag.embed.query", () -> embeddingService.embedQuery(text));
        if (queryCache != null && queryCache.isEnabled()) {
            return queryCache.embedQuery(text, embeddingType, loader);
        }
        return loader.get();
    }

    /** Runs {@code work} inside a named tracing span when observation is wired; otherwise inline. */
    private <T> T traced(String name, java.util.function.Supplier<T> work) {
        return observations != null ? observations.observe(name, work) : work.get();
    }

    // ---------------------------------------------------------------
    // Verbatim matching for identifier-like queries (#122)
    // ---------------------------------------------------------------

    /**
     * Appends the verbatim pass when the query names an identifier, and returns {@code passes}
     * untouched otherwise.
     *
     * <p>Expressed as an extra query variant rather than as a new fusion stage, which is what buys the
     * "never empty the result set" guarantee structurally instead of by a special case. The original
     * variant always runs; if the verbatim pass matches nothing it contributes an empty result set,
     * cross-variant fusion sees a single contributor, and the ranking is exactly the one the search
     * would have produced with the flag off. Scores stay on the leg-fusion scale and {@code minScore}
     * stays applied per variant, both unchanged.</p>
     *
     * <p>The chunk-level restriction is expressed through {@code VectorQuery.chunkFTS} rather than by
     * post-filtering. That is the only server-side lever available: HXQL {@code LIKE} fails on keyword
     * fields, and a {@code sys_fulltext} predicate selects <em>documents</em>, so neither can say
     * "chunks containing this token".</p>
     */
    private List<QueryVariant> withVerbatimPass(List<QueryVariant> passes, HybridSearchRequest request) {
        RagProperties.RetrievalProperties.VerbatimIdentifierProperties config =
                ragProperties.getRetrieval().getVerbatimIdentifier();
        if (!config.isEnabled()) {
            return passes;
        }

        List<String> identifiers = identifierTerms(request.getQuery(), config);
        if (identifiers.isEmpty()) {
            return passes;
        }
        if (passes.size() >= ragProperties.getQueryExpansion().getMaxVariants()) {
            log.debug("Verbatim pass for {} skipped: already at the {} variant ceiling",
                    identifiers, passes.size());
            return passes;
        }

        // The query's own vector, so the extra pass costs a retrieval call and no extra embedding call.
        List<Double> queryVector = embedQueryCached(request.getQuery(), request.getEmbeddingType());
        if (queryVector == null || queryVector.isEmpty()) {
            return passes;
        }

        log.info("Identifier-like query \"{}\": adding a verbatim pass for {}",
                request.getQuery(), identifiers);
        List<QueryVariant> withVerbatim = new ArrayList<>(passes.size() + 1);
        for (QueryVariant pass : passes) {
            // Any pass embedding this same text reuses the vector just computed, so adding the verbatim
            // pass costs one retrieval call and no second embedding call.
            withVerbatim.add(request.getQuery().equals(pass.vectorText())
                    ? pass.withVector(queryVector)
                    : pass);
        }
        withVerbatim.add(QueryVariant.verbatim(request.getQuery(), queryVector, identifiers));
        return List.copyOf(withVerbatim);
    }

    /**
     * The identifier-like tokens in a query: at least {@code minLength} characters, mixing letters and
     * digits, and made only of characters an identifier can contain.
     *
     * <p>Requiring both a letter and a digit is what separates an identifier from a word and from a
     * number. Internal {@code -} and {@code .} are kept, deliberately: real identifiers carry them
     * ({@code CHG-105402}, {@code AST-3121904}), so a rule that rejected any punctuation would reject
     * the very tokens this exists for, and {@link #sanitizeLikeTerm} preserves both for the same reason.
     * Sentence punctuation is trimmed from the ends only, so a trailing question mark does not stop a
     * token qualifying while an internal one still disqualifies it.</p>
     *
     * <p>{@code _} is <em>not</em> accepted, even though it occurs in real identifiers, because it is a
     * single-character wildcard in HXQL {@code LIKE} and {@link #sanitizeLikeTerm} therefore strips it:
     * the chunk restriction this pass is built on cannot express the token, so accepting it here would
     * produce a pass that silently matches nothing (#129). What the classifier accepts is exactly what
     * the query path can express.</p>
     */
    static List<String> identifierTerms(String queryText,
                                        RagProperties.RetrievalProperties.VerbatimIdentifierProperties config) {
        if (queryText == null || queryText.isBlank()) {
            return List.of();
        }
        return Arrays.stream(queryText.split("\\s+"))
                .map(HybridSearchService::trimEdgePunctuation)
                .filter(term -> term.length() >= config.getMinLength())
                .filter(HybridSearchService::looksLikeIdentifier)
                .map(term -> term.toLowerCase(Locale.ROOT))
                .distinct()
                .limit(Math.max(1, config.getMaxTerms()))
                .toList();
    }

    /**
     * Mixes letters and digits, and carries nothing an identifier could not — where "could not" means
     * both "is not identifier punctuation" and "the chunk filter cannot express it", so {@code _} is
     * rejected here because {@link #sanitizeLikeTerm} strips it.
     */
    private static boolean looksLikeIdentifier(String term) {
        boolean letter = false;
        boolean digit = false;
        for (int i = 0; i < term.length(); i++) {
            char c = term.charAt(i);
            if (Character.isLetter(c)) {
                letter = true;
            } else if (Character.isDigit(c)) {
                digit = true;
            } else if (c != '-' && c != '.') {
                return false;
            }
        }
        return letter && digit;
    }

    private static String trimEdgePunctuation(String term) {
        int start = 0;
        int end = term.length();
        while (start < end && !Character.isLetterOrDigit(term.charAt(start))) {
            start++;
        }
        while (end > start && !Character.isLetterOrDigit(term.charAt(end - 1))) {
            end--;
        }
        return term.substring(start, end);
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

    /**
     * One retrieval pass for one query variant: vector leg, keyword leg, leg fusion, threshold.
     *
     * <p>{@code minScore} is applied here rather than after cross-variant fusion because it is
     * calibrated against the leg-fusion scale (roughly 0.02-0.03 under rrf, 0-1 under
     * weighted/minmax). Applied to a cross-variant RRF score it would mean something else entirely.</p>
     */
    private VariantResult searchVariant(QueryVariant variant,
                                        HybridSearchRequest request,
                                        String permissionFilter,
                                        int candidateCount,
                                        String strategy,
                                        String normalization,
                                        double minScore,
                                        String logUser) {
        // --- Vector (semantic) leg ---
        log.info("Hybrid search vector leg: query=\"{}\", variant={}, candidates={}, user={}",
                variant.vectorText(), variant.label(), candidateCount, logUser);
        // A variant may arrive with its own vector: HyDE embeds its passage document-side, without the
        // query instruction prefix, because the passage is answer-shaped rather than question-shaped.
        List<Double> queryVector = variant.vectorVector();
        if (queryVector == null) {
            queryVector = embedQueryCached(variant.vectorText(), request.getEmbeddingType());
        }
        final List<Double> vector = queryVector;
        List<ScoredChunk> vectorChunks = List.of();

        // #37: when chunk-FTS mode is on, push the keyword terms into the vector call as
        // VectorQuery.chunkFTS so hxpr filters at the chunk level, and skip the separate
        // BM25-rescored keyword leg below. Off by default (behaviour-changing, eval-gated).
        // The verbatim-identifier variant (#122) forces the same mode for itself whatever the global
        // setting is, because restricting to chunks holding the token is what that variant is for.
        boolean chunkFtsMode =
                (properties.isChunkFtsEnabled() || variant.forceChunkFts()) && variant.hasKeywordLeg();
        String chunkFts = chunkFtsMode ? buildChunkFts(variant.keywordText()) : null;

        if (vector != null && !vector.isEmpty()) {
            VectorSearchResult vectorResult = traced("rag.search.vector", () -> vectorLeg(
                    variant, vector, request.getEmbeddingType(), permissionFilter, chunkFts, candidateCount));
            vectorChunks = extractVectorChunks(vectorResult);
        }

        // --- Keyword (fulltext) leg ---
        List<ScoredChunk> keywordChunks = List.of();
        if (variant.hasKeywordLeg() && !chunkFtsMode) {
            log.info("Hybrid search keyword leg: query=\"{}\", candidates={}",
                    variant.keywordText(), candidateCount);
            // The query embedding is reused: the keyword leg needs a vector only because the embeddings
            // endpoint is the sole way to read chunk text, and embedding the same query twice per search
            // would double the inference cost of every hybrid request for nothing.
            keywordChunks = traced("rag.search.keyword", () -> executeKeywordSearch(
                    variant.keywordText(), permissionFilter, candidateCount, vector,
                    request.getEmbeddingType()));
        }

        log.info("Hybrid search candidates: vector={}, keyword={}", vectorChunks.size(), keywordChunks.size());

        // --- Fuse the two legs ---
        List<FusedResult> fused;
        if (STRATEGY_WEIGHTED.equalsIgnoreCase(strategy)) {
            double vectorWeight = request.getVectorWeight() > 0 ? request.getVectorWeight() : properties.getVectorWeight();
            double textWeight = request.getTextWeight() > 0 ? request.getTextWeight() : properties.getTextWeight();
            fused = fuseWeighted(vectorChunks, keywordChunks, vectorWeight, textWeight, normalization);
        } else {
            fused = fuseRRF(vectorChunks, keywordChunks, properties.getRrfK());
        }

        if (log.isDebugEnabled()) {
            long wouldFilter = fused.stream().filter(r -> r.score < minScore).count();
            log.debug("Fusion filter: minScore={} total={} filtered={} passing={}",
                    minScore, fused.size(), wouldFilter, fused.size() - wouldFilter);
            for (int i = 0; i < Math.min(10, fused.size()); i++) {
                FusedResult r = fused.get(i);
                String preview = r.chunk.text() != null
                        ? r.chunk.text().substring(0, Math.min(60, r.chunk.text().length())) : "";
                log.debug("  [{}] score={} v={} k={} \"{}...\"", i + 1,
                        String.format("%.4f", r.score),
                        r.vectorScore != null ? String.format("%.3f", r.vectorScore) : "-",
                        r.keywordScore != null ? String.format("%.3f", r.keywordScore) : "-",
                        preview);
            }
        }

        List<FusedResult> passing = fused.stream()
                .filter(r -> r.score >= minScore)
                .toList();

        return new VariantResult(passing, vectorChunks.size(), keywordChunks.size());
    }

    /** Outcome of a single variant's retrieval pass, thresholded but not yet limited. */
    private record VariantResult(List<FusedResult> fused, int vectorCount, int keywordCount) {
    }

    /**
     * The kNN call, over every embedding type present when multi-type retrieval is wired (#121) and
     * over the {@code *} wildcard when it is not.
     *
     * <p>The per-type embedder mirrors how this variant's own vector was produced: a variant carrying
     * a pre-computed vector had it embedded document-side (HyDE passes an answer-shaped passage, which
     * must not get the query instruction prefix), so a second model has to embed it the same way or
     * the two types would be queried with differently-shaped vectors.</p>
     */
    private VectorSearchResult vectorLeg(QueryVariant variant, List<Double> vector, String embeddingType,
                                         String permissionFilter, String chunkFts, int candidateCount) {
        if (multiTypeVectorSearch == null) {
            // Default path stays on the 4-arg call; only chunk-FTS mode adds the chunkFTS argument.
            return (chunkFts != null)
                    ? hxprService.vectorSearch(vector, embeddingType, permissionFilter, chunkFts, candidateCount)
                    : hxprService.vectorSearch(vector, embeddingType, permissionFilter, candidateCount);
        }

        boolean documentSide = variant.documentSideVector();
        return multiTypeVectorSearch.search(new MultiTypeVectorSearchService.Request(
                vector,
                embedder -> documentSide
                        ? embedder.embed(variant.vectorText())
                        : embedder.embedQuery(variant.vectorText()),
                embeddingType,
                permissionFilter,
                chunkFts,
                candidateCount));
    }

    // ---------------------------------------------------------------
    // Vector leg
    // ---------------------------------------------------------------

    private List<ScoredChunk> extractVectorChunks(VectorSearchResult result) {
        if (result == null || result.getEmbeddings() == null) {
            return List.of();
        }

        List<ScoredChunk> chunks = new ArrayList<>();
        int rank = 1;
        for (Embedding emb : result.getEmbeddings()) {
            Integer page = null;
            Integer paragraph = null;
            if (emb.getSysembedLocation() != null && emb.getSysembedLocation().getText() != null) {
                page = emb.getSysembedLocation().getText().getPage();
                paragraph = emb.getSysembedLocation().getText().getParagraph();
            }
            chunks.add(new ScoredChunk(
                    chunkKey(emb.getSysembedDocId(), emb.getSysembedId()),
                    emb.getSysembedDocId(),
                    emb.getSysembedId(),
                    emb.getSysembedText(),
                    emb.getSysembedType(),
                    emb.getSysembedScore() != null ? emb.getSysembedScore() : 0.0,
                    rank++,
                    page,
                    paragraph,
                    emb.getSysembedVector()
            ));
        }
        return chunks;
    }

    // ---------------------------------------------------------------
    // Keyword leg
    // ---------------------------------------------------------------

    /**
     * Term-matching leg: finds documents by keyword, then scores their chunks by BM25 TF.
     *
     * <p>Chunks are fetched through the embeddings endpoint with the keyword HXQL as its filter,
     * because <b>a plain HXQL query returns documents without their chunks</b>. Chunk text and
     * vectors live in an {@code embeddings.parquet} blob that only that endpoint decodes, so the
     * previous implementation queried documents, found {@code sysembed_embeddings} null on every
     * one of them, skipped them all, and returned an empty list for every query. That is the second
     * half of why {@code keyword_leg_hit_rate} measured 0.0000: even once the query matched
     * documents, none of them could yield a chunk.</p>
     *
     * <p>Using the embeddings endpoint means supplying a vector, and its similarity ordering is
     * discarded: this leg re-scores every returned chunk by term frequency and sorts on that. The
     * vector is only the price of admission for reading chunk text, so a query embedding is reused
     * when one is available rather than computed again.</p>
     */
    List<ScoredChunk> executeKeywordSearch(String queryText, String permissionFilter, int candidateCount) {
        return executeKeywordSearch(queryText, permissionFilter, candidateCount, null, null);
    }

    List<ScoredChunk> executeKeywordSearch(String queryText, String permissionFilter, int candidateCount,
                                           List<Double> queryVector) {
        return executeKeywordSearch(queryText, permissionFilter, candidateCount, queryVector, null);
    }

    List<ScoredChunk> executeKeywordSearch(String queryText, String permissionFilter, int candidateCount,
                                           List<Double> queryVector, String embeddingType) {
        String fulltextClause = buildFulltextClause(queryText);
        if (fulltextClause == null) {
            // Nothing to match on. Returning empty rather than falling back to the permission
            // filter alone, which would hand the whole corpus to the keyword leg and let RRF
            // promote arbitrary documents.
            log.debug("Keyword leg skipped: no usable term in query \"{}\"", queryText);
            return List.of();
        }
        String hxql = buildFulltextQuery(queryText, permissionFilter);

        try {
            List<Double> vector = (queryVector != null && !queryVector.isEmpty())
                    ? queryVector
                    : embeddingService.embedQuery(queryText);
            if (vector.isEmpty()) {
                log.warn("Keyword leg skipped: no query embedding available to read chunks with");
                return List.of();
            }

            VectorSearchResult result = hxprService.vectorSearch(vector, embeddingType, hxql, candidateCount);
            if (result == null || result.getEmbeddings() == null) {
                return List.of();
            }
            return scoreEmbeddingsByTerms(result.getEmbeddings(), queryText);
        } catch (Exception e) {
            log.warn("Keyword search failed (continuing with vector-only): {}", e.getMessage());
            return List.of();
        }
    }

    /**
     * Scores keyword-leg chunks by BM25 term frequency, discarding those no query term appears in.
     *
     * <p>The incoming order is vector similarity against a query the caller did not ask to be ranked
     * that way, so it carries no keyword signal and is not used. Ranks are assigned from the term
     * scores, which is what rank fusion consumes.</p>
     */
    private List<ScoredChunk> scoreEmbeddingsByTerms(List<Embedding> embeddings, String queryText) {
        String[] queryTerms = keywordTerms(queryText).toArray(String[]::new);
        if (queryTerms.length == 0) {
            return List.of();
        }

        List<ScoredChunk> scored = new ArrayList<>();
        for (Embedding emb : embeddings) {
            String text = emb.getSysembedText();
            double chunkTf = computeBm25TfScore(text, queryTerms);
            if (chunkTf == 0) {
                continue;
            }

            Integer page = null;
            Integer paragraph = null;
            if (emb.getSysembedLocation() != null && emb.getSysembedLocation().getText() != null) {
                page = emb.getSysembedLocation().getText().getPage();
                paragraph = emb.getSysembedLocation().getText().getParagraph();
            }

            scored.add(new ScoredChunk(
                    chunkKey(emb.getSysembedDocId(), emb.getSysembedId()),
                    emb.getSysembedDocId(),
                    emb.getSysembedId(),
                    text,
                    emb.getSysembedType(),
                    chunkTf,
                    0,  // rank assigned after sorting on the term score
                    page,
                    paragraph,
                    null  // the vector is incidental here; this leg contributes no vector signal
            ));
        }

        scored.sort(Comparator.comparingDouble(ScoredChunk::score).reversed());
        List<ScoredChunk> ranked = new ArrayList<>(scored.size());
        int rank = 1;
        for (ScoredChunk c : scored) {
            ranked.add(new ScoredChunk(c.key, c.docId, c.embeddingId, c.text, c.embeddingType,
                    c.score, rank++, c.page, c.paragraph, c.vector));
        }
        return ranked;
    }

    String buildFulltextQuery(String queryText, String permissionFilter) {
        String fulltextClause = buildFulltextClause(queryText);
        if (fulltextClause == null) {
            // No usable term: fall back to the permission filter alone rather than emitting a
            // clause that matches everything, which would hand the whole corpus to the keyword leg.
            return permissionFilter != null ? permissionFilter : AclFilterBuilder.BASE_QUERY;
        }

        String prefix = AclFilterBuilder.BASE_QUERY + " WHERE ";
        if (permissionFilter != null && permissionFilter.startsWith(prefix)) {
            String whereClause = permissionFilter.substring(prefix.length());
            return prefix + fulltextClause + " AND " + whereClause;
        }

        return prefix + fulltextClause;
    }

    /**
     * Builds the term-matching half of the keyword query, or {@code null} when the query carries no
     * usable term.
     *
     * <p>One {@code sys_fulltext} predicate per term, OR-ed. {@code sys_fulltext} is hxpr's
     * analysed full-text index, so a term matches anywhere in the document, case-insensitively,
     * and sentinel identifiers such as {@code CHG-105402} match as single tokens. All of this was
     * established by probing a live index; two neighbouring approaches do not work and are worth
     * recording so nobody retries them:</p>
     * <ul>
     *   <li>{@code sys_fulltextBinary} is not exposed to HXQL, so the field the sync writes the
     *       extracted text into cannot itself be queried.</li>
     *   <li>{@code cin_ingestProperties.<key> LIKE '%term%'} does match, but the property index
     *       truncates at <b>256 characters</b>, so anything past the opening lines of a document is
     *       invisible. That silently limits keyword recall to the first paragraph.</li>
     * </ul>
     *
     * <p>hxpr folds {@code cin_ingestProperties} into {@code sys_fulltext} through the full-text
     * index, which carries no such truncation, so mirroring the extracted text into an ingest
     * property (see {@code NodeSyncService}) is what makes this leg work while querying
     * {@code sys_fulltext} rather than the property directly.</p>
     *
     * <p>OR rather than AND: this leg supplies candidates to rank fusion, so a document matching one
     * strong term is a useful candidate. Requiring every term would make a long natural-language
     * question match nothing.</p>
     */
    /** Space-separated keyword terms for {@code VectorQuery.chunkFTS}, or {@code null} if none. */
    static String buildChunkFts(String queryText) {
        List<String> terms = keywordTerms(queryText);
        return terms.isEmpty() ? null : String.join(" ", terms);
    }

    String buildFulltextClause(String queryText) {
        List<String> terms = keywordTerms(queryText);
        if (terms.isEmpty()) {
            return null;
        }
        String clauses = terms.stream()
                .map(term -> FULLTEXT_FIELD + " = '" + term + "'")
                .collect(Collectors.joining(" OR "));
        return terms.size() == 1 ? clauses : "(" + clauses + ")";
    }

    /**
     * Query terms worth matching: lowercased, de-duplicated, stripped of characters that would
     * break the literal or the {@code LIKE} pattern, and shorn of terms too short or too common to
     * discriminate.
     *
     * <p>Stop words are removed because a term appearing in most documents contributes a candidate
     * list the size of the corpus and no ranking signal, and because every extra OR clause is paid
     * for in query time.</p>
     */
    private static List<String> keywordTerms(String queryText) {
        if (queryText == null || queryText.isBlank()) {
            return List.of();
        }
        return Arrays.stream(queryText.toLowerCase(Locale.ROOT).split("\\s+"))
                .map(HybridSearchService::sanitizeLikeTerm)
                .filter(term -> term.length() >= MIN_KEYWORD_TERM_LENGTH)
                .filter(term -> !KEYWORD_STOP_WORDS.contains(term))
                .distinct()
                .limit(MAX_KEYWORD_TERMS)
                .toList();
    }

    /**
     * Strips what HXQL cannot carry inside a {@code LIKE} literal.
     *
     * <p>Quotes and backslashes are removed because the parser does not support escaping them
     * inside a string literal, and {@code %} and {@code _} because they are wildcards: a query
     * containing {@code %} would otherwise match every document. Punctuation is trimmed from the
     * ends only, so a sentinel identifier such as {@code CHG-105402} keeps its hyphen and stays
     * matchable as one term, which is the whole point of the keyword leg.</p>
     *
     * <p>Stripping {@code _} means a token containing one cannot be matched verbatim, which is why
     * {@link #looksLikeIdentifier} rejects such tokens rather than routing them into a chunk
     * restriction that cannot match (#129).</p>
     */
    private static String sanitizeLikeTerm(String term) {
        String stripped = term.replace("\\", "")
                .replace("'", "")
                .replace("%", "")
                .replace("_", "");
        int start = 0;
        int end = stripped.length();
        while (start < end && !Character.isLetterOrDigit(stripped.charAt(start))) {
            start++;
        }
        while (end > start && !Character.isLetterOrDigit(stripped.charAt(end - 1))) {
            end--;
        }
        return stripped.substring(start, end);
    }

    /**
     * BM25 TF component (no IDF — corpus statistics are not available through the hxpr API).
     *
     * <p>Substantially better than the previous binary term-presence ratio because it:
     * <ul>
     *   <li>uses term frequency (mentioning a term 5× scores higher than mentioning it once)</li>
     *   <li>applies document-length normalisation (short chunks are not penalised)</li>
     *   <li>models TF saturation via the k1 parameter (score increases sub-linearly with TF)</li>
     * </ul>
     * The keyword leg re-scores every candidate chunk with this and ranks on the result; hxpr's own
     * ordering is not a keyword signal on that path and is discarded.</p>
     *
     * @param chunkText  text of the chunk to score
     * @param queryTerms lower-cased query tokens
     * @return non-negative score; 0 when no query term appears in the chunk
     */
    static double computeBm25TfScore(String chunkText, String[] queryTerms) {
        if (queryTerms.length == 0 || chunkText == null || chunkText.isBlank()) return 0.0;

        final double k1 = 1.2;
        final double b  = 0.75;
        // avgDocLength ≈ 100 words — matches a typical 512-char chunk (~5 chars/word)
        final double avgDocLength = 100.0;

        String lower = chunkText.toLowerCase();
        int docLength = lower.split("\\s+").length;
        double lengthNorm = 1.0 - b + b * docLength / avgDocLength;

        double score = 0.0;
        for (String term : queryTerms) {
            if (term.isBlank()) continue;
            int tf = countTermFrequency(lower, term);
            if (tf == 0) continue;
            score += tf * (k1 + 1.0) / (tf + k1 * lengthNorm);
        }

        return queryTerms.length > 0 ? score / queryTerms.length : 0.0;
    }

    private static int countTermFrequency(String text, String term) {
        int count = 0;
        int idx = 0;
        while ((idx = text.indexOf(term, idx)) >= 0) {
            count++;
            idx += term.length();
        }
        return count;
    }

    // ---------------------------------------------------------------
    // Fusion: Reciprocal Rank Fusion
    // ---------------------------------------------------------------

    static List<FusedResult> fuseRRF(List<ScoredChunk> vectorChunks, List<ScoredChunk> keywordChunks, int k) {
        Map<String, FusedResult> fused = new LinkedHashMap<>();

        for (ScoredChunk vc : vectorChunks) {
            FusedResult r = fused.computeIfAbsent(vc.key, key -> new FusedResult(vc));
            r.score += 1.0 / (k + vc.rank);
            r.vectorScore = vc.score;
            r.vectorRank = vc.rank;
        }

        for (ScoredChunk kc : keywordChunks) {
            FusedResult r = fused.computeIfAbsent(kc.key, key -> new FusedResult(kc));
            r.score += 1.0 / (k + kc.rank);
            r.keywordScore = kc.score;
            r.keywordRank = kc.rank;
        }

        return fused.values().stream()
                .sorted(Comparator.comparingDouble(FusedResult::getScore).reversed())
                .toList();
    }

    // ---------------------------------------------------------------
    // Fusion: across query variants
    // ---------------------------------------------------------------

    /**
     * Fuses the leg-fused result sets of several query variants into one ranking.
     *
     * <p>One level above {@link #fuseRRF}: that merges the vector and keyword legs of a single query,
     * this merges whole result sets produced by different formulations of the question. Same
     * {@code 1 / (k + rank)} formula, so a chunk that several formulations found outranks one that only
     * a single formulation found at the same depth.</p>
     *
     * <p>Each surviving {@link FusedResult} keeps the score, per-leg scores and per-leg ranks from the
     * variant that ranked it highest. The cross-variant score decides order only and is deliberately
     * not written back: {@code HybridHit.score} is thresholded and displayed downstream on the
     * leg-fusion scale, and an RRF value there would be meaningless.</p>
     */
    static List<FusedResult> fuseAcrossVariants(List<List<FusedResult>> perVariant, int k) {
        Map<String, FusedResult> best = new LinkedHashMap<>();
        Map<String, Double> rrfScores = new HashMap<>();

        for (List<FusedResult> variantResults : perVariant) {
            if (variantResults == null) {
                continue;
            }
            for (int i = 0; i < variantResults.size(); i++) {
                FusedResult result = variantResults.get(i);
                String key = result.chunk.key;
                int rank = i + 1;
                rrfScores.merge(key, 1.0 / (k + rank), Double::sum);
                FusedResult incumbent = best.get(key);
                if (incumbent == null || result.score > incumbent.score) {
                    best.put(key, result);
                }
            }
        }

        return best.entrySet().stream()
                .sorted(Comparator.comparingDouble(
                        (Map.Entry<String, FusedResult> e) -> rrfScores.getOrDefault(e.getKey(), 0.0)).reversed())
                .map(Map.Entry::getValue)
                .toList();
    }

    // ---------------------------------------------------------------
    // Fusion: Weighted combination
    // ---------------------------------------------------------------

    static List<FusedResult> fuseWeighted(List<ScoredChunk> vectorChunks, List<ScoredChunk> keywordChunks,
                                          double vectorWeight, double textWeight) {
        return fuseWeighted(vectorChunks, keywordChunks, vectorWeight, textWeight, NORMALIZATION_MAX);
    }

    static List<FusedResult> fuseWeighted(List<ScoredChunk> vectorChunks, List<ScoredChunk> keywordChunks,
                                          double vectorWeight, double textWeight, String normalization) {
        double maxVector = vectorChunks.stream().mapToDouble(ScoredChunk::score).max().orElse(0.0);
        double minVector = vectorChunks.stream().mapToDouble(ScoredChunk::score).min().orElse(0.0);
        double maxKeyword = keywordChunks.stream().mapToDouble(ScoredChunk::score).max().orElse(0.0);
        double minKeyword = keywordChunks.stream().mapToDouble(ScoredChunk::score).min().orElse(0.0);

        Map<String, FusedResult> fused = new LinkedHashMap<>();

        for (ScoredChunk vc : vectorChunks) {
            double normScore = normalizeScore(vc.score, minVector, maxVector, normalization);
            FusedResult r = fused.computeIfAbsent(vc.key, key -> new FusedResult(vc));
            r.score += vectorWeight * normScore;
            r.vectorScore = vc.score;
            r.vectorRank = vc.rank;
        }

        for (ScoredChunk kc : keywordChunks) {
            double normScore = normalizeScore(kc.score, minKeyword, maxKeyword, normalization);
            FusedResult r = fused.computeIfAbsent(kc.key, key -> new FusedResult(kc));
            r.score += textWeight * normScore;
            r.keywordScore = kc.score;
            r.keywordRank = kc.rank;
        }

        return fused.values().stream()
                .sorted(Comparator.comparingDouble(FusedResult::getScore).reversed())
                .toList();
    }

    private static double normalizeScore(double score, double minScore, double maxScore, String normalization) {
        if (NORMALIZATION_MINMAX.equals(normalization)) {
            if (maxScore <= minScore) {
                return maxScore > 0 ? 1.0 : 0.0;
            }
            return (score - minScore) / (maxScore - minScore);
        }
        if (maxScore <= 0) {
            return 0.0;
        }
        return score / maxScore;
    }

    // ---------------------------------------------------------------
    // Document metadata enrichment
    // ---------------------------------------------------------------

    private Map<String, SourceDocument> fetchDocumentMetadata(List<FusedResult> results,
                                                              Map<String, SectionMap> sectionMaps) {
        Map<String, SourceDocument> cache = new ConcurrentHashMap<>();

        Set<String> docIds = results.stream()
                .map(r -> r.chunk.docId)
                .filter(Objects::nonNull)
                .filter(SemanticSearchService::looksLikeUuid)
                .collect(Collectors.toSet());

        if (docIds.isEmpty()) {
            return cache;
        }

        for (String docId : docIds) {
            try {
                HxprDocument.QueryResult result = hxprService.query(
                        "SELECT * FROM SysContent WHERE sys_id = '" + AclFilterBuilder.escapeLiteral(docId) + "'",
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

        return cache;
    }

    // ---------------------------------------------------------------
    // Response building
    // ---------------------------------------------------------------

    private List<HybridHit> buildHits(List<FusedResult> results, Map<String, SourceDocument> docCache,
                                      Map<String, SectionMap> sectionMaps) {
        List<HybridHit> hits = new ArrayList<>();
        int rank = 1;

        for (FusedResult r : results) {
            ScoredChunk chunk = r.chunk;

            // chunkType (#69) resolved from the document's section map.
            SectionMap sectionMap = chunk.docId != null ? sectionMaps.get(chunk.docId) : null;
            ChunkMetadata.ChunkMetadataBuilder chunkMeta = ChunkMetadata.builder()
                    .embeddingId(chunk.embeddingId)
                    .embeddingType(chunk.embeddingType)
                    .chunkLength(chunk.text != null ? chunk.text.length() : 0)
                    .page(chunk.page)
                    .paragraph(chunk.paragraph)
                    .chunkType(sectionMapResolver.chunkType(sectionMap, chunk.paragraph));

            SourceDocument sourceDoc = (chunk.docId != null && docCache.containsKey(chunk.docId))
                    ? docCache.get(chunk.docId)
                    : SourceDocument.builder().documentId(chunk.docId).build();

            hits.add(HybridHit.builder()
                    .rank(rank++)
                    .score(r.score)
                    .chunkText(chunk.text)
                    .sourceDocument(sourceDoc)
                    .chunkMetadata(chunkMeta.build())
                    .vector(chunk.vector)
                    .vectorScore(r.vectorScore)
                    .keywordScore(r.keywordScore)
                    .vectorRank(r.vectorRank)
                    .keywordRank(r.keywordRank)
                    .build());
        }

        return hits;
    }

    // ---------------------------------------------------------------
    // Metadata filter layer
    // ---------------------------------------------------------------

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

    String buildMetadataFilter(HybridSearchRequest.MetadataFilter metadata) {
        if (metadata == null) {
            return null;
        }

        List<String> clauses = new ArrayList<>();

        if (metadata.getMimeType() != null && !metadata.getMimeType().isBlank()) {
            clauses.add(SOURCE_MIME_PROP + " = '" + AclFilterBuilder.escapeLiteral(metadata.getMimeType().trim()) + "'");
        }

        if (metadata.getPathPrefix() != null && !metadata.getPathPrefix().isBlank()) {
            String escapedPrefix = AclFilterBuilder.escapeLiteral(metadata.getPathPrefix().trim());
            clauses.add("(" + SOURCE_PATH_PROP + " >= '" + escapedPrefix + "' AND "
                    + SOURCE_PATH_PROP + " < '" + escapedPrefix + "\uFFFF')");
        }

        if (metadata.getModifiedAfter() != null && !metadata.getModifiedAfter().isBlank()) {
            clauses.add(SOURCE_MODIFIED_PROP + " >= '" + AclFilterBuilder.escapeLiteral(metadata.getModifiedAfter().trim()) + "'");
        }

        if (metadata.getModifiedBefore() != null && !metadata.getModifiedBefore().isBlank()) {
            clauses.add(SOURCE_MODIFIED_PROP + " <= '" + AclFilterBuilder.escapeLiteral(metadata.getModifiedBefore().trim()) + "'");
        }

        if (metadata.getProperties() != null && !metadata.getProperties().isEmpty()) {
            for (Map.Entry<String, String> entry : metadata.getProperties().entrySet()) {
                String key = normaliseCustomPropertyKey(entry.getKey());
                String value = entry.getValue();
                if (key == null || value == null || value.isBlank()) {
                    continue;
                }
                // Normalize the caller's label to its canonical vocabulary key so the same concept
                // matches across repos; a no-op when no vocabulary maps the value.
                String canonical = vocabularyService.resolve(value.trim());
                clauses.add(INGEST_PROP_PREFIX + key + " = '" + AclFilterBuilder.escapeLiteral(canonical) + "'");
            }
        }

        if (clauses.isEmpty()) {
            return null;
        }
        return String.join(" AND ", clauses);
    }

    private static String normaliseCustomPropertyKey(String key) {
        if (key == null) {
            return null;
        }
        String trimmed = key.trim();
        if (trimmed.isBlank() || !CUSTOM_PROP_KEY_PATTERN.matcher(trimmed).matches()) {
            return null;
        }
        return trimmed;
    }

    // ---------------------------------------------------------------
    // Permission filter (reuses SemanticSearchService logic)
    // ---------------------------------------------------------------

    /**
     * Builds an ACL-scoped HXQL permission filter for the currently authenticated user, honouring
     * dual-source (Alfresco + Nuxeo) authentication. Shared with {@link FacetsService} so facet
     * counts are scoped to what the caller may read, not the whole corpus.
     */
    public String buildCurrentUserPermissionFilter(String sourceType, String additionalFilter) {
        Authentication auth = SecurityContextHolder.getContext().getAuthentication();
        if (auth instanceof DualSourceAuthentication dual) {
            return buildPermissionFilter(
                    dual.getAlfrescoUsername(), dual.getNuxeoUsername(), sourceType, additionalFilter);
        }
        String username = securityContextService.getCurrentUsername();
        return buildPermissionFilter(username, sourceType, additionalFilter);
    }

    String buildPermissionFilter(String username, String additionalFilter) {
        return buildPermissionFilter(username, null, additionalFilter);
    }

    /** Dual-auth variant — see {@link SemanticSearchService#buildPermissionFilter(String, String, String, String)}. */
    String buildPermissionFilter(String alfrescoUser, String nuxeoUser,
                                 String sourceType, String additionalFilter) {
        List<String> sourceIds = resolvePermissionSourceIds(sourceType, additionalFilter);
        Map<String, List<String>> authoritiesBySource =
                resolveAuthoritiesByDualSource(alfrescoUser, nuxeoUser, sourceIds);

        List<String> sourceClauses = new ArrayList<>();
        for (String sourceId : sourceIds) {
            String username = isNuxeoSource(sourceId) ? nuxeoUser : alfrescoUser;
            if (username == null) {
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

        log.debug("Dual-auth permission filter (hybrid): alfrescoUser={}, nuxeoUser={}, sourceIds={}",
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
     * configured policy is to fail closed. An empty list means unknown, not "no groups", and the
     * permission filter drops the source rather than guessing.
     */
    List<String> getUserAuthorities(String username, String sourceId) {
        String sourceType = sourceCatalog().sourceType(configuredSources(), sourceId);
        return groupResolvers().authorities(username, sourceId, sourceType);
    }

    // ---------------------------------------------------------------
    // Config resolution helpers
    // ---------------------------------------------------------------

    private String resolveStrategy(HybridSearchRequest request) {
        String value = request.getStrategy();
        if (value == null || value.isBlank()) {
            value = properties.getStrategy();
        }

        if (STRATEGY_WEIGHTED.equalsIgnoreCase(value)) {
            return STRATEGY_WEIGHTED;
        }
        return STRATEGY_RRF;
    }

    private String resolveNormalization(HybridSearchRequest request) {
        String value = request.getNormalization();
        if (value == null || value.isBlank()) {
            value = properties.getNormalization();
        }

        if (NORMALIZATION_MINMAX.equalsIgnoreCase(value)) {
            return NORMALIZATION_MINMAX;
        }
        return NORMALIZATION_MAX;
    }

    /**
     * The per-document chunk cap for this request, or 0 when it does not apply.
     *
     * <p>Both search paths need the cap: this endpoint is what {@code content-lake-eval} measures its
     * retrieval metrics through, the semantic endpoint is what the deployment E2E suite asserts on, and
     * capping one alone measures as a no-op on whichever harness queries the other.</p>
     *
     * <p>The RAG pipeline retrieves through this endpoint too, and it opts out
     * ({@link HybridSearchRequest#isSkipDocumentDiversity()}): a cap that helps a caller browsing
     * results takes supporting chunks away from the document that is the answer, which was measured as
     * a fall in {@code faithfulness} and {@code citation_accuracy} on the golden set.</p>
     */
    private int maxChunksPerDocument(HybridSearchRequest request) {
        if (request != null && request.isSkipDocumentDiversity()) {
            return 0;
        }
        return configuredChunksPerDocument();
    }

    /** The document a fused chunk belongs to; its own key when the document id is missing. */
    private static String documentKeyOf(FusedResult result) {
        String docId = result.chunk != null ? result.chunk.docId() : null;
        if (docId != null && !docId.isBlank()) {
            return "doc:" + docId;
        }
        String key = result.chunk != null ? result.chunk.key() : null;
        return key != null && !key.isBlank() ? "chunk:" + key : "unkeyed:" + System.identityHashCode(result);
    }

    private int resolveCandidateCount(HybridSearchRequest request) {
        int count = request.getCandidateCount() > 0 ? request.getCandidateCount() : properties.getCandidateCount();
        return Math.min(Math.max(count, 1), MAX_CANDIDATE_COUNT);
    }

    private int resolveMaxResults(HybridSearchRequest request) {
        int max = request.getMaxResults() > 0 ? request.getMaxResults() : properties.getMaxResults();
        return Math.min(Math.max(max, 1), MAX_CANDIDATE_COUNT);
    }

    /**
     * How much to retrieve and how much to return, from either budget the caller may have asked in.
     *
     * <p>With no {@code topDocuments} this is the pre-#135 arithmetic unchanged: {@code candidateCount} and
     * {@code maxResults} as the caller or the deployment set them. With one, the answer becomes a product of
     * two request-visible numbers and the candidate pool is raised to at least twice it, bounded by
     * {@link #MAX_CANDIDATE_FETCH}: unlike the semantic path, deepening here happens once and up front,
     * because re-querying would have to re-run the keyword leg as well as the vector one.</p>
     */
    private ResultBudget resolveBudget(HybridSearchRequest request) {
        Integer requestedDocuments = request.getTopDocuments();
        if (requestedDocuments == null) {
            return new ResultBudget(null, 0, resolveMaxResults(request), resolveCandidateCount(request));
        }

        int documents = clamp(requestedDocuments, 1, MAX_TOP_DOCUMENTS);
        // Falls back to the configured cap rather than to "unlimited", because a document budget with no
        // per-document bound has no defined size: one document could fill the whole answer.
        int perDocument = request.getChunksPerDocument() != null
                ? clamp(request.getChunksPerDocument(), 1, MAX_CHUNKS_PER_DOCUMENT)
                : Math.max(configuredChunksPerDocument(), 1);

        int chunkLimit = Math.min(documents * perDocument, MAX_CHUNK_LIMIT);
        // At least doubled: the selection needs chunks of documents it will not keep to choose between.
        int candidateCount = Math.min(
                Math.max(resolveCandidateCount(request), chunkLimit * 2), MAX_CANDIDATE_FETCH);

        return new ResultBudget(documents, perDocument, chunkLimit, candidateCount);
    }

    private static int clamp(int value, int min, int max) {
        return Math.min(Math.max(value, min), max);
    }

    /** The configured per-document cap, or 0 when document diversity is disabled. */
    private int configuredChunksPerDocument() {
        if (ragProperties == null || ragProperties.getRetrieval() == null) {
            return 0;
        }
        RagProperties.RetrievalProperties.DocumentDiversityProperties diversity =
                ragProperties.getRetrieval().getDocumentDiversity();
        return diversity != null && diversity.isEnabled() ? diversity.getMaxChunksPerDocument() : 0;
    }

    /**
     * What to retrieve and what to return for one request.
     *
     * @param topDocuments      the clamped document budget, or null when the caller asked in chunks
     * @param chunksPerDocument chunks one document may contribute; meaningless without {@code topDocuments}
     * @param chunkLimit        chunks the response may carry
     * @param candidateCount    candidates each leg retrieves per variant
     */
    private record ResultBudget(Integer topDocuments, int chunksPerDocument, int chunkLimit, int candidateCount) {

        boolean documentBudget() {
            return topDocuments != null;
        }
    }

    // ---------------------------------------------------------------
    // Utilities
    // ---------------------------------------------------------------

    private static String chunkKey(String docId, String embeddingId) {
        return (docId != null ? docId : "?") + "::" + (embeddingId != null ? embeddingId : UUID.randomUUID().toString());
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
        return sourceCatalog().qualify(configuredSources(), sourceId);
    }

    /**
     * Startup diagnostic for the pinned permission sources, over every source type. See
     * {@link SemanticSearchService#logPermissionSourceIdConfiguration()}, which logs the same thing for
     * the semantic path; both are kept because either service can be the only one a deployment calls.
     */
    @PostConstruct
    void logPermissionSourceIdConfiguration() {
        if (permissionSourceIds == null || permissionSourceIds.isBlank()) {
            log.info("rag.permission.source-ids is not set; the hybrid permission filter covers every "
                    + "source discovered in the index plus the configured Alfresco and Nuxeo source ids");
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

    private List<String> resolvePermissionSourceIds(String sourceType, String additionalFilter) {
        return sourceCatalog().resolve(configuredSources(), sourceType, additionalFilter);
    }

    /**
     * The catalogue of sources to filter on: the injected bean when there is one, otherwise a private
     * instance built lazily so it is available to a unit test that never runs {@code @PostConstruct}.
     */
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

    /**
     * The group resolvers, or a registry holding none when this service was constructed without one. A
     * registry with no resolvers yields the caller's default authorities for every source.
     */
    private SourceGroupResolverRegistry groupResolvers() {
        return groupResolverRegistry != null
                ? groupResolverRegistry
                : SourceGroupResolverRegistry.withoutResolvers();
    }

    private PermissionSourceCatalog.Configured configuredSources() {
        return PermissionSourceCatalog.Configured.of(alfrescoSourceId, nuxeoSourceId, permissionSourceIds);
    }

    private boolean isAlfrescoSource(String sourceId) {
        return sourceCatalog().isAlfresco(configuredSources(), sourceId);
    }

    private boolean isNuxeoSource(String sourceId) {
        return sourceCatalog().isNuxeo(configuredSources(), sourceId);
    }

    // ---------------------------------------------------------------
    // Internal data carriers
    // ---------------------------------------------------------------

    record ScoredChunk(
            String key,
            String docId,
            String embeddingId,
            String text,
            String embeddingType,
            double score,
            int rank,
            Integer page,
            Integer paragraph,
            List<Double> vector
    ) {}

    static class FusedResult {
        final ScoredChunk chunk;
        double score;
        Double vectorScore;
        Double keywordScore;
        Integer vectorRank;
        Integer keywordRank;

        FusedResult(ScoredChunk chunk) {
            this.chunk = chunk;
        }

        double getScore() {
            return score;
        }
    }
}
