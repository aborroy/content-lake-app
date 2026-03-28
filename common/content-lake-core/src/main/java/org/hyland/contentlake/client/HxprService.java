package org.hyland.contentlake.client;

import lombok.extern.slf4j.Slf4j;
import org.hyland.contentlake.hxpr.api.model.Query;
import org.hyland.contentlake.hxpr.api.model.VectorQuery;
import org.hyland.contentlake.hxpr.api.model.VectorSearchResult;
import org.hyland.contentlake.model.HxprDocument;
import org.hyland.contentlake.model.HxprEmbedding;
import org.springframework.http.MediaType;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientResponseException;
import org.springframework.web.util.UriUtils;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Business-logic layer on top of the hxpr REST API.
 *
 * <p>Contains orchestration helpers (folder creation, embedding management, queries).
 * Path-based document operations use {@link RestClient} directly since Spring HTTP
 * Interface encodes slashes in {@code @PathVariable} values.</p>
 */
@Slf4j
public class HxprService {

    private static final String EMBED_MIXIN = "SysEmbed";
    private static final String SYS_FOLDER = "SysFolder";
    private static final String SYS_FILE = "SysFile";
    private static final String DEFAULT_QUERY = "SELECT * FROM SysContent";

    private final HxprDocumentApi documentApi;
    private final HxprQueryApi queryApi;
    private final RestClient restClient;

    public HxprService(
            HxprDocumentApi documentApi,
            HxprQueryApi queryApi,
            RestClient restClient
    ) {
        this.documentApi = documentApi;
        this.queryApi = queryApi;
        this.restClient = restClient;
    }

    /**
     * Checks whether a document exists at the given absolute path.
     *
     * @param absolutePath absolute path (with or without leading slash)
     * @return {@code true} if the document exists
     */
    public boolean existsByPath(String absolutePath) {
        String cleanPath = stripLeadingSlash(absolutePath);
        try {
            restClient.get()
                    .uri(buildDocumentPathUri(cleanPath, null))
                    .retrieve()
                    .toBodilessEntity();
            return true;
        } catch (HttpClientErrorException.NotFound e) {
            return false;
        } catch (RestClientResponseException e) {
            if (e.getStatusCode().value() == 404) {
                return false;
            }
            throw e;
        }
    }

    /**
     * Finds a document by its absolute repository path.
     *
     * @param absolutePath absolute path (with or without leading slash)
     * @return matching document, or {@code null} when no document exists at that path
     */
    public HxprDocument findByPath(String absolutePath) {
        String cleanPath = stripLeadingSlash(absolutePath);
        try {
            return restClient.get()
                    .uri(buildDocumentPathUri(cleanPath, null))
                    .retrieve()
                    .body(HxprDocument.class);
        } catch (HttpClientErrorException.NotFound e) {
            return null;
        } catch (RestClientResponseException e) {
            if (e.getStatusCode().value() == 404) {
                return null;
            }
            throw e;
        }
    }

    /**
     * Creates a document under the given parent path.
     *
     * @param parentPath parent path (with or without leading slash)
     * @param document document payload
     * @return created document
     */
    public HxprDocument createDocument(String parentPath, HxprDocument document) {
        String cleanPath = stripLeadingSlash(parentPath);
        log.debug("Creating document at path: {}", cleanPath);
        return restClient.post()
                .uri(buildDocumentPathUri(cleanPath, "enforceSysName=true"))
                .contentType(MediaType.APPLICATION_JSON)
                .body(document)
                .retrieve()
                .body(HxprDocument.class);
    }

    /**
     * Creates a folder under the given parent path.
     *
     * <p>Ignores 409 Conflict (folder already exists).</p>
     *
     * @param parentPath parent path (with or without leading slash)
     * @param folderName folder sysname
     */
    public void createFolder(String parentPath, String folderName) {
        String cleanParent = (parentPath == null) ? "" : stripLeadingSlash(parentPath);

        HxprDocument folder = new HxprDocument();
        folder.setSysPrimaryType(SYS_FOLDER);
        folder.setSysName(folderName);

        try {
            restClient.post()
                    .uri(buildDocumentPathUri(cleanParent, "enforceSysName=true"))
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(folder)
                    .retrieve()
                    .toBodilessEntity();
        } catch (HttpClientErrorException.Conflict e) {
            // Folder already exists.
        }
    }

    /**
     * Ensures that the full folder path exists by creating segments sequentially.
     *
     * @param absolutePath absolute folder path
     */
    public void ensureFolder(String absolutePath) {
        String normalized = normalizeAbsolutePath(absolutePath);
        ensureFolderCreateOnly(normalized);
    }

    private void ensureFolderCreateOnly(String absolutePath) {
        String cleanPath = stripLeadingSlash(normalizeAbsolutePath(absolutePath));
        if (cleanPath == null || cleanPath.isBlank()) {
            return;
        }

        String parent = "";
        for (String segment : cleanPath.split("/")) {
            if (segment == null || segment.isBlank()) {
                continue;
            }
            String currentPath = parent.isEmpty() ? "/" + segment : "/" + parent + "/" + segment;
            try {
                createFolder(parent, segment);
            } catch (HttpClientErrorException.Unauthorized | HttpClientErrorException.Forbidden e) {
                throw new IllegalStateException("HXPR denied folder creation at path '" + currentPath + "'", e);
            }
            parent = parent.isEmpty() ? segment : parent + "/" + segment;
        }
    }

    /**
     * Stores embeddings in {@code sysembed_embeddings} and ensures the {@code SysEmbed} mixin is present.
     *
     * @param documentId hxpr document identifier
     * @param embeddings embeddings to store
     */
    public void updateEmbeddings(String documentId, List<HxprEmbedding> embeddings) {
        log.info("Updating {} embeddings for document: {}", embeddings.size(), documentId);

        HxprDocument currentDoc = documentApi.getById(documentId);
        ensureSysEmbedMixin(documentId, currentDoc);

        documentApi.updateById(documentId, Map.of("sysembed_embeddings", embeddings));

        int vectorDim = embeddings.isEmpty() || embeddings.get(0).getVector() == null
                ? 0
                : embeddings.get(0).getVector().size();

        log.info("Updated document {} with {} embeddings (vector dim: {})",
                documentId, embeddings.size(), vectorDim);
    }

    /**
     * Clears {@code sysembed_embeddings} for the document.
     *
     * @param documentId hxpr document identifier
     */
    public void deleteEmbeddings(String documentId) {
        log.info("Clearing embeddings for document: {}", documentId);

        try {
            HxprDocument doc = documentApi.getById(documentId);
            if (doc == null) {
                log.warn("Document not found: {}", documentId);
                return;
            }

            if (!hasSysEmbedMixin(doc)) {
                log.debug("Document {} does not have {} mixin, nothing to clear", documentId, EMBED_MIXIN);
                return;
            }

            documentApi.updateById(documentId, Map.of("sysembed_embeddings", List.of()));
            log.info("Cleared embeddings for document: {}", documentId);

        } catch (Exception e) {
            log.warn("Failed to clear embeddings for document {}: {}", documentId, e.getMessage());
        }
    }

    /**
     * Finds a document by its source identifier stored in {@code (cin_sourceId, cin_id)}.
     *
     * <p><b>Migration compatibility:</b> {@code sourceId} should be supplied in the
     * {@code "type:rawId"} format introduced in Issue 20 (e.g. {@code "alfresco:abc-uuid"}).
     * When the colon-prefixed format is detected, the query also accepts the legacy raw-id
     * format so that documents indexed before the migration remain discoverable during the
     * transition window.</p>
     *
     * @param nodeId   source-system node identifier stored in {@code cin_id}
     * @param sourceId formatted source identifier ({@code "type:rawId"}) or legacy raw id
     * @return matching document, or {@code null} if not found
     */
    public HxprDocument findByNodeId(String nodeId, String sourceId) {
        try {
            String hxql = "SELECT * FROM SysContent WHERE sys_primaryType = '" + SYS_FILE + "' AND cin_id = '"
                    + escapeHxql(nodeId) + "'";
            if (sourceId != null && !sourceId.isBlank()) {
                hxql += " AND " + buildSourceIdPredicate(sourceId);
            }

            Query query = newQuery(hxql, 2, 0);

            HxprDocument.QueryResult result = queryApi.query(query);
            if (result != null && result.getDocuments() != null && !result.getDocuments().isEmpty()) {
                return selectPreferredDocument(result.getDocuments(), sourceId);
            }
        } catch (Exception e) {
            log.warn("Failed to query hxpr for cin_sourceId={}, cin_id={} (will create new document): {}",
                    sourceId, nodeId, e.getMessage());
        }

        return null;
    }

    /**
     * Finds multiple documents keyed by source-system node identifier.
     *
     * <p>When the supplied {@code sourceId} uses the Issue 20 {@code "type:rawId"}
     * format, the query also matches the legacy raw-id form so pre-migration
     * Alfresco documents remain visible during the transition window.</p>
     */
    public Map<String, HxprDocument> findByNodeIds(Collection<String> nodeIds, String sourceId) {
        List<String> sanitizedIds = nodeIds == null
                ? List.of()
                : nodeIds.stream()
                .filter(id -> id != null && !id.isBlank())
                .distinct()
                .toList();

        if (sanitizedIds.isEmpty()) {
            return Map.of();
        }

        try {
            String idPredicate = sanitizedIds.stream()
                    .map(id -> "cin_id = '" + escapeHxql(id) + "'")
                    .collect(Collectors.joining(" OR ", "(", ")"));

            String hxql = "SELECT * FROM SysContent WHERE sys_primaryType = '" + SYS_FILE + "' AND " + idPredicate;
            if (sourceId != null && !sourceId.isBlank()) {
                hxql += " AND " + buildSourceIdPredicate(sourceId);
            }

            HxprDocument.QueryResult result = query(hxql, sanitizedIds.size() * 2, 0);
            if (result == null || result.getDocuments() == null) {
                return Map.of();
            }

            Map<String, HxprDocument> documentsByNodeId = new LinkedHashMap<>();
            for (HxprDocument document : result.getDocuments()) {
                if (document.getCinId() != null && !document.getCinId().isBlank()) {
                    documentsByNodeId.merge(
                            document.getCinId(),
                            document,
                            (current, candidate) -> preferDocument(current, candidate, sourceId)
                    );
                }
            }
            return documentsByNodeId;
        } catch (Exception e) {
            log.warn("Failed to query hxpr for {} node ids: {}", sanitizedIds.size(), e.getMessage());
            return Map.of();
        }
    }

    /**
     * Finds a document by Alfresco node identifier only.
     *
     * <p>Kept for backward compatibility when the source repository identifier is
     * not available, but callers should prefer {@link #findByNodeId(String, String)}.</p>
     */
    public HxprDocument findByNodeId(String nodeId) {
        return findByNodeId(nodeId, null);
    }

    /**
     * Executes an HXQL query.
     *
     * @param hxql hxql query string
     * @param limit max results
     * @param offset result offset
     * @return query result
     */
    public HxprDocument.QueryResult query(String hxql, int limit, int offset) {
        return queryApi.query(newQuery(hxql, limit, offset));
    }

    /**
     * Performs a vector similarity search (kNN).
     *
     * @param vector query vector
     * @param embeddingType embedding type, or {@code "*"} when {@code null}
     * @param hxqlFilter hxql filter, or a default query when {@code null}
     * @param limit max results
     * @return vector search result
     */
    public VectorSearchResult vectorSearch(List<Double> vector, String embeddingType, String hxqlFilter, int limit) {
        VectorQuery vq = new VectorQuery();
        vq.setVector(vector);
        vq.setEmbeddingType(embeddingType != null ? embeddingType : "*");
        vq.setQuery(hxqlFilter != null ? hxqlFilter : DEFAULT_QUERY);
        vq.setLimit((long) limit);
        vq.setOffset(0L);
        vq.setTrackTotalCount(true);
        return queryApi.vectorSearch(vq);
    }

    /**
     * Performs a semantic search by embedding the query text and running vector search.
     *
     * @param queryText free text query
     * @param embeddingType embedding type, or {@code "*"} when {@code null}
     * @param hxqlFilter hxql filter, or a default query when {@code null}
     * @param limit max results
     * @param embedder function that produces an embedding vector
     * @return vector search result
     */
    public VectorSearchResult semanticSearch(
            String queryText,
            String embeddingType,
            String hxqlFilter,
            int limit,
            Function<String, List<Double>> embedder
    ) {
        return vectorSearch(embedder.apply(queryText), embeddingType, hxqlFilter, limit);
    }

    private void ensureSysEmbedMixin(String documentId, HxprDocument currentDoc) {
        if (currentDoc == null) {
            return;
        }
        if (hasSysEmbedMixin(currentDoc)) {
            return;
        }

        log.debug("Adding {} mixin to document {}", EMBED_MIXIN, documentId);
        documentApi.patchById(documentId, List.of(Map.of(
                "op", "add",
                "path", "/sys_mixinTypes/-",
                "value", EMBED_MIXIN
        )));
    }

    private boolean hasSysEmbedMixin(HxprDocument doc) {
        List<String> mixins = doc.getSysMixinTypes();
        return mixins != null && mixins.contains(EMBED_MIXIN);
    }

    private Query newQuery(String hxql, int limit, int offset) {
        Query query = new Query();
        query.setQuery(hxql);
        query.setLimit((long) limit);
        query.setOffset((long) offset);
        return query;
    }

    /**
     * Encodes each segment of a slash-delimited path using RFC 3986 path-segment
     * encoding (spaces -> {@code %20}, etc.) while leaving the {@code /} separators
     * as literal characters so Tomcat does not reject the request with
     * "encoded slash character is not allowed".
     *
     * @param path slash-delimited path, without leading slash
     * @return encoded path safe to embed in a URI string
     */
    private static String encodePathSegments(String path) {
        if (path == null || path.isEmpty()) {
            return "";
        }
        return Arrays.stream(path.split("/", -1))
                .map(segment -> UriUtils.encodePathSegment(segment, StandardCharsets.UTF_8))
                .collect(Collectors.joining("/"));
    }

    private static URI buildDocumentPathUri(String cleanPath, String query) {
        String path = "/api/documents/path/" + encodePathSegments(cleanPath);
        if (query == null || query.isBlank()) {
            return URI.create(path);
        }
        return URI.create(path + "?" + query);
    }

    private static String normalizeAbsolutePath(String path) {
        if (path == null || path.isBlank()) {
            return "/";
        }
        return path.startsWith("/") ? path : "/" + path;
    }

    private static String stripLeadingSlash(String path) {
        return (path != null && path.startsWith("/")) ? path.substring(1) : path;
    }

    private static String buildSourceIdPredicate(String sourceId) {
        List<String> variants = sourceIdVariants(sourceId);
        if (variants.size() == 1) {
            return "cin_sourceId = '" + escapeHxql(variants.get(0)) + "'";
        }

        return variants.stream()
                .map(variant -> "cin_sourceId = '" + escapeHxql(variant) + "'")
                .collect(Collectors.joining(" OR ", "(", ")"));
    }

    private static List<String> sourceIdVariants(String sourceId) {
        if (sourceId == null || sourceId.isBlank()) {
            return List.of();
        }

        LinkedHashSet<String> variants = new LinkedHashSet<>();
        variants.add(sourceId);

        int separator = sourceId.indexOf(':');
        if (separator > 0 && separator < sourceId.length() - 1) {
            variants.add(sourceId.substring(separator + 1));
        }

        return new ArrayList<>(variants);
    }

    private static HxprDocument selectPreferredDocument(List<HxprDocument> documents, String sourceId) {
        if (documents == null || documents.isEmpty()) {
            return null;
        }

        HxprDocument preferred = documents.get(0);
        for (int i = 1; i < documents.size(); i++) {
            preferred = preferDocument(preferred, documents.get(i), sourceId);
        }
        return preferred;
    }

    private static HxprDocument preferDocument(HxprDocument current, HxprDocument candidate, String sourceId) {
        if (current == null) {
            return candidate;
        }
        if (candidate == null) {
            return current;
        }

        if (sourceId == null || sourceId.isBlank()) {
            return current;
        }

        boolean currentExact = sourceId.equals(current.getCinSourceId());
        boolean candidateExact = sourceId.equals(candidate.getCinSourceId());

        if (candidateExact && !currentExact) {
            return candidate;
        }

        return current;
    }

    private static String escapeHxql(String value) {
        return value == null ? "" : value.replace("'", "''");
    }
}
