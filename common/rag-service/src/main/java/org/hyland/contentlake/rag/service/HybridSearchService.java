package org.hyland.contentlake.rag.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.hyland.contentlake.client.HxprService;
import org.hyland.contentlake.hxpr.api.model.Embedding;
import org.hyland.contentlake.hxpr.api.model.VectorSearchResult;
import org.hyland.contentlake.model.ContentLakeIngestProperties;
import org.hyland.contentlake.model.HxprDocument;
import org.hyland.contentlake.model.HxprEmbedding;
import org.hyland.contentlake.rag.config.HybridSearchProperties;
import org.hyland.contentlake.rag.model.HybridSearchRequest;
import org.hyland.contentlake.rag.model.HybridSearchResponse;
import org.hyland.contentlake.rag.model.HybridSearchResponse.HybridHit;
import org.hyland.contentlake.rag.model.SemanticSearchResponse.ChunkMetadata;
import org.hyland.contentlake.rag.model.SemanticSearchResponse.SourceDocument;
import org.hyland.contentlake.security.SecurityContextService;
import org.hyland.contentlake.service.EmbeddingService;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.*;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

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

    private static final int MAX_CANDIDATE_COUNT = 50;
    private static final String BASE_QUERY = "SELECT * FROM SysContent";
    private static final String RACL_FIELD = "sys_racl";
    private static final String EVERYONE_PRINCIPAL = "__Everyone__";
    private static final String GROUP_PREFIX = "GROUP_";
    private static final String GROUP_RACL_PREFIX = "g:";
    private static final String SOURCE_ID_SEPARATOR = "_#_";
    private static final String INGEST_PROP_PREFIX = "cin_ingestProperties.";
    private static final String SOURCE_MIME_PROP = INGEST_PROP_PREFIX + ContentLakeIngestProperties.SOURCE_MIME_TYPE;
    private static final String SOURCE_PATH_PROP = INGEST_PROP_PREFIX + ContentLakeIngestProperties.SOURCE_PATH;
    private static final String SOURCE_MODIFIED_PROP = INGEST_PROP_PREFIX + ContentLakeIngestProperties.SOURCE_MODIFIED_AT;
    private static final Pattern CUSTOM_PROP_KEY_PATTERN = Pattern.compile("[A-Za-z0-9_:-]+");
    private static final Pattern SOURCE_ID_EQUALS_PATTERN = Pattern.compile("cin_sourceId\\s*=\\s*'([^']+)'");

    private final HxprService hxprService;
    private final EmbeddingService embeddingService;
    private final SecurityContextService securityContextService;
    private final HybridSearchProperties properties;
    private final SourceMetadataResolver sourceMetadataResolver;

    @Value("${hxpr.repositoryId:default}")
    private String repositoryId;

    @Value("${rag.permission.source-ids:}")
    private String permissionSourceIds;

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

    /**
     * Executes a hybrid search: runs vector and keyword legs in sequence,
     * then fuses the results using the configured (or overridden) strategy.
     */
    public HybridSearchResponse search(HybridSearchRequest request) {
        long startTime = System.currentTimeMillis();

        String username = securityContextService.getCurrentUsername();
        String sourceTypeFilter = buildSourceTypeFilter(request.getSourceType());
        String metadataFilter = buildMetadataFilter(request.getMetadata());
        String additionalFilter = combineFilters(request.getFilter(), metadataFilter);
        additionalFilter = combineFilters(additionalFilter, sourceTypeFilter);
        String permissionFilter = buildPermissionFilter(username, request.getSourceType(), additionalFilter);

        int candidateCount = resolveCandidateCount(request);
        int maxResults = resolveMaxResults(request);
        String strategy = resolveStrategy(request);
        String normalization = STRATEGY_WEIGHTED.equals(strategy) ? resolveNormalization(request) : null;
        double minScore = request.getMinScore() > 0 ? request.getMinScore() : properties.getDefaultMinScore();

        // --- Vector (semantic) leg ---
        log.info("Hybrid search vector leg: query=\"{}\", candidates={}, user={}", request.getQuery(), candidateCount, username);
        List<Double> queryVector = embeddingService.embedQuery(request.getQuery());
        List<ScoredChunk> vectorChunks = List.of();

        if (!queryVector.isEmpty()) {
            VectorSearchResult vectorResult = hxprService.vectorSearch(
                    queryVector, request.getEmbeddingType(), permissionFilter, candidateCount);
            vectorChunks = extractVectorChunks(vectorResult);
        }

        // --- Keyword (fulltext) leg ---
        log.info("Hybrid search keyword leg: query=\"{}\", candidates={}", request.getQuery(), candidateCount);
        List<ScoredChunk> keywordChunks = executeKeywordSearch(request.getQuery(), permissionFilter, candidateCount);

        log.info("Hybrid search candidates: vector={}, keyword={}", vectorChunks.size(), keywordChunks.size());

        // --- Fuse ---
        List<FusedResult> fused;
        if (STRATEGY_WEIGHTED.equalsIgnoreCase(strategy)) {
            double vectorWeight = request.getVectorWeight() > 0 ? request.getVectorWeight() : properties.getVectorWeight();
            double textWeight = request.getTextWeight() > 0 ? request.getTextWeight() : properties.getTextWeight();
            fused = fuseWeighted(vectorChunks, keywordChunks, vectorWeight, textWeight, normalization);
        } else {
            fused = fuseRRF(vectorChunks, keywordChunks, properties.getRrfK());
        }

        // --- Filter and limit ---
        List<FusedResult> filtered = fused.stream()
                .filter(r -> r.score >= minScore)
                .limit(maxResults)
                .toList();

        // --- Enrich with document metadata ---
        Map<String, SourceDocument> docCache = fetchDocumentMetadata(filtered);

        // --- Build response ---
        List<HybridHit> hits = buildHits(filtered, docCache);

        long searchTimeMs = System.currentTimeMillis() - startTime;
        log.info("Hybrid search completed: {} results in {}ms (strategy={}, vector={}, keyword={})",
                hits.size(), searchTimeMs, strategy, vectorChunks.size(), keywordChunks.size());

        return HybridSearchResponse.builder()
                .query(request.getQuery())
                .strategy(strategy)
                .normalization(normalization)
                .model(embeddingService.getModelName())
                .resultCount(hits.size())
                .vectorCandidates(vectorChunks.size())
                .keywordCandidates(keywordChunks.size())
                .searchTimeMs(searchTimeMs)
                .results(hits)
                .build();
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
            chunks.add(new ScoredChunk(
                    chunkKey(emb.getSysembedDocId(), emb.getSysembedId()),
                    emb.getSysembedDocId(),
                    emb.getSysembedId(),
                    emb.getSysembedText(),
                    emb.getSysembedType(),
                    emb.getSysembedScore() != null ? emb.getSysembedScore() : 0.0,
                    rank++,
                    emb.getSysembedLocation()
            ));
        }
        return chunks;
    }

    // ---------------------------------------------------------------
    // Keyword leg
    // ---------------------------------------------------------------

    List<ScoredChunk> executeKeywordSearch(String queryText, String permissionFilter, int candidateCount) {
        String hxql = buildFulltextQuery(queryText, permissionFilter);

        try {
            HxprDocument.QueryResult result = hxprService.query(hxql, candidateCount, 0);
            if (result == null || result.getDocuments() == null) {
                return List.of();
            }

            return extractAndScoreChunks(result.getDocuments(), queryText);
        } catch (Exception e) {
            log.warn("Keyword search failed (continuing with vector-only): {}", e.getMessage());
            return List.of();
        }
    }

    String buildFulltextQuery(String queryText, String permissionFilter) {
        String escaped = escapeHxql(queryText);
        String fulltextClause = "sys_fulltextBinary = '" + escaped + "'";

        if (permissionFilter != null && permissionFilter.startsWith(BASE_QUERY + " WHERE ")) {
            String whereClause = permissionFilter.substring((BASE_QUERY + " WHERE ").length());
            return BASE_QUERY + " WHERE " + fulltextClause + " AND " + whereClause;
        }

        return BASE_QUERY + " WHERE " + fulltextClause;
    }

    private List<ScoredChunk> extractAndScoreChunks(List<HxprDocument> documents, String queryText) {
        List<ScoredChunk> chunks = new ArrayList<>();
        String[] queryTerms = queryText.toLowerCase().split("\\s+");

        for (HxprDocument doc : documents) {
            if (doc.getSysembedEmbeddings() == null || doc.getSysembedEmbeddings().isEmpty()) {
                continue;
            }

            for (HxprEmbedding emb : doc.getSysembedEmbeddings()) {
                if (emb.getText() == null || emb.getText().isBlank()) {
                    continue;
                }

                double score = computeKeywordScore(emb.getText(), queryTerms);
                if (score > 0) {
                    chunks.add(new ScoredChunk(
                            chunkKey(doc.getSysId(), emb.getChunkId()),
                            doc.getSysId(),
                            emb.getChunkId(),
                            emb.getText(),
                            emb.getType(),
                            score,
                            0,  // rank assigned later
                            null
                    ));
                }
            }
        }

        // Sort by score descending and assign ranks
        chunks.sort(Comparator.comparingDouble(ScoredChunk::score).reversed());
        List<ScoredChunk> ranked = new ArrayList<>();
        int rank = 1;
        for (ScoredChunk c : chunks) {
            ranked.add(new ScoredChunk(c.key, c.docId, c.embeddingId, c.text, c.embeddingType, c.score, rank++, c.location));
        }
        return ranked;
    }

    static double computeKeywordScore(String chunkText, String[] queryTerms) {
        if (queryTerms.length == 0) return 0.0;

        String lowerText = chunkText.toLowerCase();
        int matchCount = 0;
        for (String term : queryTerms) {
            if (!term.isBlank() && lowerText.contains(term)) {
                matchCount++;
            }
        }
        return (double) matchCount / queryTerms.length;
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

    private Map<String, SourceDocument> fetchDocumentMetadata(List<FusedResult> results) {
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
                        "SELECT * FROM SysContent WHERE sys_id = '" + escapeHxql(docId) + "'",
                        1, 0);

                if (result != null && result.getDocuments() != null) {
                    result.getDocuments().stream()
                            .findFirst()
                            .ifPresent(doc -> cache.put(docId, sourceMetadataResolver.resolveSourceDocument(docId, doc)));
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

    private List<HybridHit> buildHits(List<FusedResult> results, Map<String, SourceDocument> docCache) {
        List<HybridHit> hits = new ArrayList<>();
        int rank = 1;

        for (FusedResult r : results) {
            ScoredChunk chunk = r.chunk;

            ChunkMetadata.ChunkMetadataBuilder chunkMeta = ChunkMetadata.builder()
                    .embeddingId(chunk.embeddingId)
                    .embeddingType(chunk.embeddingType)
                    .chunkLength(chunk.text != null ? chunk.text.length() : 0);

            if (chunk.location != null && chunk.location.getText() != null) {
                chunkMeta.page(chunk.location.getText().getPage());
                chunkMeta.paragraph(chunk.location.getText().getParagraph());
            }

            SourceDocument sourceDoc = (chunk.docId != null && docCache.containsKey(chunk.docId))
                    ? docCache.get(chunk.docId)
                    : SourceDocument.builder().documentId(chunk.docId).build();

            hits.add(HybridHit.builder()
                    .rank(rank++)
                    .score(r.score)
                    .chunkText(chunk.text)
                    .sourceDocument(sourceDoc)
                    .chunkMetadata(chunkMeta.build())
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
            clauses.add(SOURCE_MIME_PROP + " = '" + escapeHxql(metadata.getMimeType().trim()) + "'");
        }

        if (metadata.getPathPrefix() != null && !metadata.getPathPrefix().isBlank()) {
            String escapedPrefix = escapeHxql(metadata.getPathPrefix().trim());
            clauses.add("(" + SOURCE_PATH_PROP + " >= '" + escapedPrefix + "' AND "
                    + SOURCE_PATH_PROP + " < '" + escapedPrefix + "\uFFFF')");
        }

        if (metadata.getModifiedAfter() != null && !metadata.getModifiedAfter().isBlank()) {
            clauses.add(SOURCE_MODIFIED_PROP + " >= '" + escapeHxql(metadata.getModifiedAfter().trim()) + "'");
        }

        if (metadata.getModifiedBefore() != null && !metadata.getModifiedBefore().isBlank()) {
            clauses.add(SOURCE_MODIFIED_PROP + " <= '" + escapeHxql(metadata.getModifiedBefore().trim()) + "'");
        }

        if (metadata.getProperties() != null && !metadata.getProperties().isEmpty()) {
            for (Map.Entry<String, String> entry : metadata.getProperties().entrySet()) {
                String key = normaliseCustomPropertyKey(entry.getKey());
                String value = entry.getValue();
                if (key == null || value == null || value.isBlank()) {
                    continue;
                }
                clauses.add(INGEST_PROP_PREFIX + key + " = '" + escapeHxql(value.trim()) + "'");
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

    String buildPermissionFilter(String username, String additionalFilter) {
        return buildPermissionFilter(username, null, additionalFilter);
    }

    String buildPermissionFilter(String username, String sourceType, String additionalFilter) {
        StringBuilder hxql = new StringBuilder(BASE_QUERY);
        List<String> conditions = new ArrayList<>();

        List<String> sourceIds = resolvePermissionSourceIds(sourceType, additionalFilter);
        Map<String, List<String>> authoritiesBySource = resolveAuthoritiesBySource(username, sourceIds);

        List<String> raclClauses = new ArrayList<>();
        raclClauses.add(RACL_FIELD + " = '" + escapeHxql(EVERYONE_PRINCIPAL) + "'");

        for (String sourceId : sourceIds) {
            List<String> authorities = authoritiesBySource.getOrDefault(sourceId, defaultAuthorities(username));
            for (String authority : authorities) {
                if ("GROUP_EVERYONE".equals(authority)) {
                    continue;
                }
                raclClauses.add(buildAuthorityClause(authority, sourceId));
            }
        }

        conditions.add("(" + String.join(" OR ", raclClauses) + ")");

        if (additionalFilter != null && !additionalFilter.isBlank()) {
            conditions.add("(" + additionalFilter.trim() + ")");
        }

        hxql.append(" WHERE ").append(String.join(" AND ", conditions));
        return hxql.toString();
    }

    Map<String, List<String>> resolveAuthoritiesBySource(String username, List<String> sourceIds) {
        Map<String, List<String>> authoritiesBySource = new LinkedHashMap<>();
        for (String sourceId : sourceIds) {
            authoritiesBySource.put(sourceId, getUserAuthorities(username, sourceId));
        }
        return authoritiesBySource;
    }

    @SuppressWarnings("unchecked")
    List<String> getUserAuthorities(String username, String sourceId) {
        LinkedHashSet<String> authorities = new LinkedHashSet<>(defaultAuthorities(username));
        try {
            if (isAlfrescoSource(sourceId)) {
                authorities.addAll(fetchAlfrescoGroups(username));
            } else if (isNuxeoSource(sourceId)) {
                authorities.addAll(fetchNuxeoGroups(username));
            }
        } catch (Exception e) {
            log.warn("Failed to retrieve authorities for user {} on source {} (proceeding with username + GROUP_EVERYONE): {}",
                    username, sourceId, e.getMessage());
        }

        return List.copyOf(authorities);
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

    private int resolveCandidateCount(HybridSearchRequest request) {
        int count = request.getCandidateCount() > 0 ? request.getCandidateCount() : properties.getCandidateCount();
        return Math.min(Math.max(count, 1), MAX_CANDIDATE_COUNT);
    }

    private int resolveMaxResults(HybridSearchRequest request) {
        int max = request.getMaxResults() > 0 ? request.getMaxResults() : properties.getMaxResults();
        return Math.min(Math.max(max, 1), MAX_CANDIDATE_COUNT);
    }

    // ---------------------------------------------------------------
    // Utilities
    // ---------------------------------------------------------------

    private static String chunkKey(String docId, String embeddingId) {
        return (docId != null ? docId : "?") + "::" + (embeddingId != null ? embeddingId : UUID.randomUUID().toString());
    }

    private static String escapeHxql(String value) {
        return value == null ? "" : value.replace("'", "''");
    }

    private String buildSourceTypeFilter(String sourceType) {
        String normalized = normalizeSourceType(sourceType);
        if (normalized == null) {
            return null;
        }
        return "cin_ingestProperties." + ContentLakeIngestProperties.SOURCE_TYPE
                + " = '" + escapeHxql(normalized) + "'";
    }

    private String buildAuthorityClause(String authority, String sourceId) {
        String namespaced = authority + SOURCE_ID_SEPARATOR + sourceId;
        String principal = authority.startsWith(GROUP_PREFIX)
                ? GROUP_RACL_PREFIX + namespaced
                : namespaced;
        return RACL_FIELD + " = '" + escapeHxql(principal) + "'";
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
            addSourceId(sourceIds, repositoryId);
            addSourceId(sourceIds, nuxeoSourceId);
        }

        return List.copyOf(sourceIds);
    }

    private void addSourceIdsForType(Set<String> sourceIds, String sourceType) {
        String normalized = normalizeSourceType(sourceType);
        if ("alfresco".equals(normalized)) {
            addSourceId(sourceIds, repositoryId);
        } else if ("nuxeo".equals(normalized)) {
            addSourceId(sourceIds, nuxeoSourceId);
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

        return List.copyOf(groups);
    }

    private List<String> defaultAuthorities(String username) {
        return List.of(username, "GROUP_EVERYONE");
    }

    private boolean isAlfrescoSource(String sourceId) {
        return sourceId != null && sourceId.equals(repositoryId);
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
            org.hyland.contentlake.hxpr.api.model.LocationModel location
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
