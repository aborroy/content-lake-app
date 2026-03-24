package org.alfresco.contentlake.rag.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.alfresco.contentlake.rag.model.SemanticSearchRequest;
import org.alfresco.contentlake.rag.model.SemanticSearchResponse;
import org.alfresco.contentlake.rag.model.SemanticSearchResponse.ChunkMetadata;
import org.alfresco.contentlake.rag.model.SemanticSearchResponse.SearchHit;
import org.alfresco.contentlake.rag.model.SemanticSearchResponse.SourceDocument;
import org.alfresco.contentlake.security.SecurityContextService;
import org.alfresco.contentlake.client.HxprService;
import org.alfresco.contentlake.hxpr.api.model.Embedding;
import org.alfresco.contentlake.hxpr.api.model.VectorSearchResult;
import org.alfresco.contentlake.model.HxprDocument;
import org.alfresco.contentlake.service.EmbeddingService;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.*;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Service for executing permission-aware semantic searches against the HXPR vector index.
 *
 * <p>Workflow:
 * <ol>
 *   <li>Embed the query text using the same model used at ingestion time</li>
 *   <li>Retrieve the authenticated user group memberships from Alfresco</li>
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

    private static final double FALLBACK_MIN_SCORE = 0.5d;

    private final HxprService hxprService;
    private final EmbeddingService embeddingService;
    private final SecurityContextService securityContextService;

    @Value("${hxpr.repositoryId:default}")
    private String repositoryId;

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
        String hxqlFilter = buildPermissionFilter(username, request.getFilter());

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
        StringBuilder hxql = new StringBuilder(BASE_QUERY);
        List<String> conditions = new ArrayList<>();

        List<String> authorities = getUserAuthorities(username);

        authorities = authorities.stream()
                .filter(Objects::nonNull)
                .map(String::trim)
                .filter(s -> !s.isBlank())
                .distinct()
                .toList();

        String suffix = buildSourceSystemSuffix();

        List<String> raclClauses = new ArrayList<>();
        raclClauses.add(RACL_FIELD + " = '" + escapeHxql(EVERYONE_PRINCIPAL) + "'");

        if (!authorities.isEmpty()) {
            for (String authority : authorities) {
                if ("GROUP_EVERYONE".equals(authority)) {
                    continue;
                } else if (authority.startsWith(GROUP_PREFIX)) {
                    raclClauses.add(RACL_FIELD + " = '" + escapeHxql(GROUP_RACL_PREFIX + authority + suffix) + "'");
                } else {
                    raclClauses.add(RACL_FIELD + " = '" + escapeHxql(authority + suffix) + "'");
                }
            }
            log.debug("Permission filter with {} authorities for user: {} (suffix='{}')",
                    authorities.size(), username, suffix);
        } else {
            raclClauses.add(RACL_FIELD + " = '" + escapeHxql(username + suffix) + "'");
            log.debug("No authorities resolved, falling back to username only for user: {}", username);
        }

        conditions.add("(" + String.join(" OR ", raclClauses) + ")");

        if (additionalFilter != null && !additionalFilter.isBlank()) {
            conditions.add("(" + additionalFilter.trim() + ")");
        }

        hxql.append(" WHERE ").append(String.join(" AND ", conditions));

        return hxql.toString();
    }

    List<String> getUserAuthorities(String username) {
        List<String> authorities = new ArrayList<>();
        authorities.add(username);
        authorities.add("GROUP_EVERYONE");

        try {
            List<String> groups = fetchUserGroups(username);
            authorities.addAll(groups);
            log.debug("Resolved {} authorities for user {}", authorities.size(), username);
        } catch (Exception e) {
            log.warn("Failed to retrieve groups for user {} (proceeding with username + GROUP_EVERYONE): {}",
                    username, e.getMessage());
        }

        return authorities;
    }

    @SuppressWarnings("unchecked")
    private List<String> fetchUserGroups(String username) {
        RestTemplate restTemplate = new RestTemplate();

        String url = alfrescoUrl
                + "/alfresco/api/-default-/public/alfresco/versions/1/people/"
                + username + "/groups?skipCount=0&maxItems=1000";

        HttpHeaders headers = new HttpHeaders();
        headers.setAccept(List.of(MediaType.APPLICATION_JSON));
        headers.setBasicAuth(serviceAccountUsername, serviceAccountPassword);

        ResponseEntity<Map> response = restTemplate.exchange(
                url, HttpMethod.GET, new HttpEntity<>(headers), Map.class);

        List<String> groups = new ArrayList<>();
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
        return groups;
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
                            .ifPresent(doc -> cache.put(docId, buildSourceDocument(docId, doc)));
                }
            } catch (Exception e) {
                log.warn("Failed to fetch metadata for document {}: {}", docId, e.getMessage());
            }
        }

        log.debug("Enriched {} / {} document references", cache.size(), docIds.size());
        return cache;
    }

    private SourceDocument buildSourceDocument(String docId, HxprDocument doc) {
        String path = (doc.getCinPaths() != null && !doc.getCinPaths().isEmpty())
                ? doc.getCinPaths().getFirst() : null;

        String name = null;
        String mimeType = null;
        if (doc.getCinIngestProperties() != null) {
            Object nameObj = doc.getCinIngestProperties().get("alfresco_name");
            if (nameObj != null) name = nameObj.toString();
            Object mimeObj = doc.getCinIngestProperties().get("alfresco_mimeType");
            if (mimeObj != null) mimeType = mimeObj.toString();
        }

        return SourceDocument.builder()
                .documentId(docId)
                .nodeId(doc.getCinId() != null ? doc.getCinId() : doc.getSysName())
                .sourceId(doc.getCinSourceId())
                .name(name)
                .path(path)
                .mimeType(mimeType)
                .build();
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

    private static String escapeHxql(String value) {
        return value == null ? "" : value.replace("'", "''");
    }

    private String buildSourceSystemSuffix() {
        return "_#_" + repositoryId;
    }
}
