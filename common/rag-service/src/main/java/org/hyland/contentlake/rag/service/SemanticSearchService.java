package org.hyland.contentlake.rag.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.hyland.contentlake.rag.model.SemanticSearchRequest;
import org.hyland.contentlake.rag.model.SemanticSearchResponse;
import org.hyland.contentlake.rag.model.SemanticSearchResponse.ChunkMetadata;
import org.hyland.contentlake.rag.model.SemanticSearchResponse.SearchHit;
import org.hyland.contentlake.rag.model.SemanticSearchResponse.SourceDocument;
import org.hyland.contentlake.security.SecurityContextService;
import org.hyland.contentlake.client.HxprService;
import org.hyland.contentlake.hxpr.api.model.Embedding;
import org.hyland.contentlake.hxpr.api.model.VectorSearchResult;
import org.hyland.contentlake.model.ContentLakeIngestProperties;
import org.hyland.contentlake.model.HxprDocument;
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
    private static final String BASE_QUERY = "SELECT * FROM SysContent";

    private static final String RACL_FIELD = "sys_racl";
    private static final String EVERYONE_PRINCIPAL = "__Everyone__";
    private static final String GROUP_PREFIX = "GROUP_";
    private static final String GROUP_RACL_PREFIX = "g:";
    private static final String SOURCE_ID_SEPARATOR = "_#_";
    private static final Pattern SOURCE_ID_EQUALS_PATTERN = Pattern.compile("cin_sourceId\\s*=\\s*'([^']+)'");

    private static final double FALLBACK_MIN_SCORE = 0.5d;

    private final HxprService hxprService;
    private final EmbeddingService embeddingService;
    private final SecurityContextService securityContextService;
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

    @Value("${semantic-search.default-min-score:" + FALLBACK_MIN_SCORE + "}")
    private double defaultMinScore;

    public SemanticSearchResponse search(SemanticSearchRequest request) {
        long startTime = System.currentTimeMillis();

        int topK = Math.min(Math.max(request.getTopK(), 1), MAX_TOP_K);
        String username = securityContextService.getCurrentUsername();

        double minScore = resolveMinScore(request);

        // 1) Embed (using query-specific instruction prefix for asymmetric models)
        log.info("Embedding query: \"{}\" (topK={}, minScore={}, user={})", request.getQuery(), topK, minScore, username);

        List<Double> queryVector = embeddingService.embedQuery(request.getQuery());

        if (queryVector.isEmpty()) {
            log.warn("Empty embedding vector for query: {}", request.getQuery());
            return emptyResponse(request, 0, System.currentTimeMillis() - startTime);
        }

        // 2) Build permission filter using sys_racl
        String sourceTypeFilter = buildSourceTypeFilter(request.getSourceType());
        String additionalFilter = combineFilters(request.getFilter(), sourceTypeFilter);
        String hxqlFilter = buildPermissionFilter(username, request.getSourceType(), additionalFilter);

        // 3) Vector search
        log.debug("Executing vector search with filter: {}", hxqlFilter);
        VectorSearchResult vectorResult = hxprService.vectorSearch(
                queryVector,
                request.getEmbeddingType(),
                hxqlFilter,
                topK
        );

        if (vectorResult == null || vectorResult.getEmbeddings() == null || vectorResult.getEmbeddings().isEmpty()) {
            log.info("No results for query: \"{}\"", request.getQuery());
            return emptyResponse(request, queryVector.size(), System.currentTimeMillis() - startTime);
        }

        // 4) Enrich with parent document metadata
        Map<String, SourceDocument> documentCache = fetchDocumentMetadata(vectorResult.getEmbeddings());

        // 5) Build response (apply minScore)
        List<SearchHit> hits = buildSearchHits(vectorResult.getEmbeddings(), documentCache, minScore);

        long searchTimeMs = System.currentTimeMillis() - startTime;

        log.info("Semantic search completed: {} results in {}ms for query: \"{}\" (minScore={})",
                hits.size(), searchTimeMs, request.getQuery(), minScore);

        return SemanticSearchResponse.builder()
                .query(request.getQuery())
                .model(embeddingService.getModelName())
                .vectorDimension(queryVector.size())
                .resultCount(hits.size())
                .totalCount(vectorResult.getTotalCount() != null ? vectorResult.getTotalCount() : hits.size())
                .searchTimeMs(searchTimeMs)
                .results(hits)
                .build();
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

        log.debug("Permission filter with source-scoped authorities for user {} (sourceIds={})", username, sourceIds);

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

    List<String> getUserAuthorities(String username, String sourceId) {
        LinkedHashSet<String> authorities = new LinkedHashSet<>(defaultAuthorities(username));
        try {
            if (isAlfrescoSource(sourceId)) {
                authorities.addAll(fetchAlfrescoGroups(username));
            } else if (isNuxeoSource(sourceId)) {
                authorities.addAll(fetchNuxeoGroups(username));
            }
            log.debug("Resolved {} authorities for user {} on source {}", authorities.size(), username, sourceId);
        } catch (Exception e) {
            log.warn("Failed to retrieve authorities for user {} on source {} (proceeding with username + GROUP_EVERYONE): {}",
                    username, sourceId, e.getMessage());
        }

        return List.copyOf(authorities);
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

    private Map<String, SourceDocument> fetchDocumentMetadata(List<Embedding> embeddings) {
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

        log.debug("Enriched {} / {} document references", cache.size(), docIds.size());
        return cache;
    }

    // ---------------------------------------------------------------
    // Result building
    // ---------------------------------------------------------------

    private List<SearchHit> buildSearchHits(List<Embedding> embeddings,
                                            Map<String, SourceDocument> documentCache,
                                            double minScore) {
        List<SearchHit> hits = new ArrayList<>();
        int rank = 1;

        for (Embedding embedding : embeddings) {
            double score = embedding.getSysembedScore() != null ? embedding.getSysembedScore() : 0.0;

            if (score < minScore) {
                continue;
            }

            String chunkText = embedding.getSysembedText();
            String docId = embedding.getSysembedDocId();

            ChunkMetadata.ChunkMetadataBuilder chunkMeta = ChunkMetadata.builder()
                    .embeddingId(embedding.getSysembedId())
                    .embeddingType(embedding.getSysembedType())
                    .chunkLength(chunkText.length());

            if (embedding.getSysembedLocation() != null
                    && embedding.getSysembedLocation().getText() != null) {
                chunkMeta.page(embedding.getSysembedLocation().getText().getPage());
                chunkMeta.paragraph(embedding.getSysembedLocation().getText().getParagraph());
            }

            SourceDocument sourceDoc = (docId != null && documentCache.containsKey(docId))
                    ? documentCache.get(docId)
                    : SourceDocument.builder().documentId(docId).build();

            hits.add(SearchHit.builder()
                    .rank(rank++)
                    .score(score)
                    .chunkText(chunkText)
                    .sourceDocument(sourceDoc)
                    .chunkMetadata(chunkMeta.build())
                    .build());
        }

        return hits;
    }

    private SemanticSearchResponse emptyResponse(SemanticSearchRequest request, int vectorDim, long timeMs) {
        return SemanticSearchResponse.builder()
                .query(request.getQuery())
                .model(embeddingService.getModelName())
                .vectorDimension(vectorDim)
                .resultCount(0)
                .totalCount(0)
                .searchTimeMs(timeMs)
                .results(List.of())
                .build();
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
}
