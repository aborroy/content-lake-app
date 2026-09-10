package org.hyland.contentlake.client;

import lombok.extern.slf4j.Slf4j;
import org.hyland.contentlake.hxpr.api.model.AdvancedQuery;
import org.hyland.contentlake.hxpr.api.model.NamedQuery;
import org.hyland.contentlake.hxpr.api.model.Query;
import org.hyland.contentlake.hxpr.api.model.TermsAggregationsQuery;
import org.hyland.contentlake.hxpr.api.model.VectorQuery;
import org.hyland.contentlake.hxpr.api.model.VectorSearchResult;
import org.hyland.contentlake.model.HxprNamedQueries;
import org.hyland.contentlake.model.HxprTermsAggregationResult;
import org.hyland.contentlake.model.HxprDocument;
import org.hyland.contentlake.model.HxprEmbedding;
import org.hyland.contentlake.security.AclFilterBuilder;
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
import java.util.function.Consumer;
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
    private static final String EMBEDDING_PARENT_MIXIN = "SysHasEmbeddings";
    private static final String SYS_FOLDER = "SysFolder";
    private static final String SYS_FILE = "SysFile";
    private static final String DEFAULT_QUERY = "SELECT * FROM SysContent";
    private static final int INDEX_WAIT_TIMEOUT_SECONDS = 30;

    /** Name prefix every embedding child carries, per the hxpr Content Lake specification. */
    private static final String EMBEDDING_CHILD_PREFIX = "_e_";

    /** Page size for the embedding-child lookup. Children per document are few; paging is a guard. */
    private static final int EMBEDDING_CHILD_PAGE_SIZE = 100;

    /** Upper bound on embedding-child pages, so a non-advancing offset cannot spin forever. */
    private static final int EMBEDDING_CHILD_MAX_PAGES = 20;

    /** Page cap for a whole-source scan when hxpr reports no total count. */
    private static final int MAX_SOURCE_SCAN_PAGES = 5_000;

    private final HxprDocumentApi documentApi;
    private final HxprQueryApi queryApi;
    private final RestClient restClient;

    /**
     * Embedding type derived from the configured model, used to name embedding children.
     *
     * <p>Derived rather than constant: a hardcoded type does not follow the configured model, so
     * children written under a previous configuration could no longer be named by the clear path.
     * They survived a re-sync and kept answering queries through the {@code *} wildcard the read
     * path substitutes, letting a retired model's vectors compete with the current ones (#113).</p>
     */
    private final String embeddingType;

    public HxprService(
            HxprDocumentApi documentApi,
            HxprQueryApi queryApi,
            RestClient restClient,
            String embeddingType
    ) {
        this.documentApi = documentApi;
        this.queryApi = queryApi;
        this.restClient = restClient;
        this.embeddingType = embeddingType;
    }

    /** The embedding type this instance writes under, derived from the configured model. */
    public String getEmbeddingType() {
        return embeddingType;
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
     * Stores embeddings as a Parquet file in a {@code SysEmbeddings} child document, net-replacing
     * any previously stored embeddings for the same embedding type.
     *
     * <p>The parent is marked with the {@code SysHasEmbeddings} mixin and any existing embedding
     * child (including auto-suffixed duplicates) is removed before the new child is created, so a
     * re-sync of an unchanged node leaves the embedding/chunk count unchanged rather than
     * accumulating duplicates.</p>
     *
     * @param documentId hxpr document identifier
     * @param embeddings embeddings to store
     */
    public void updateEmbeddings(String documentId, List<HxprEmbedding> embeddings) {
        updateEmbeddings(documentId, embeddings, null);
    }

    /**
     * Stores embeddings, reusing an embedding-child list the caller already holds.
     *
     * <p>The replace step has to know the document's existing children, and looking them up costs an
     * index wait of up to {@link #INDEX_WAIT_TIMEOUT_SECONDS} seconds per call. A caller that walks the
     * whole corpus waits for the index once and lists children without waiting, so paying the wait again
     * here made a configured rate limit meaningless: the wait, not the limit, set the pace (#127). Such a
     * caller passes the list it already has and no second lookup happens.</p>
     *
     * <p>The list is trusted as given. That is safe for the caller it exists for, whose list was read
     * after a job-level index wait: if it is nonetheless stale for the target type, the child create that
     * follows enforces {@code sys_name} uniqueness and fails with a 409, so the document is reported as
     * failed rather than ending up with two children.</p>
     *
     * @param knownChildren the document's embedding children, or {@code null} to look them up (which
     *                      waits for the index first)
     */
    public void updateEmbeddings(String documentId,
                                 List<HxprEmbedding> embeddings,
                                 List<EmbeddingChild> knownChildren) {
        log.info("Updating {} embeddings for document: {}", embeddings.size(), documentId);

        // Always use Parquet storage for all embeddings
        updateEmbeddingsInBatches(documentId, embeddings, knownChildren);

        int vectorDim = embeddings.isEmpty() || embeddings.get(0).getVector() == null
                ? 0
                : embeddings.get(0).getVector().size();

        log.info("Updated document {} with {} embeddings (vector dim: {})",
                documentId, embeddings.size(), vectorDim);
    }

    /**
     * Stores embeddings as Parquet file in a child document to avoid MongoDB's 16MB document size limit.
     *
     * <p>This method implements the HXPR Content Lake approach (CIN-6680) where embeddings are stored
     * as Parquet files attached to child documents. The parent document is marked with the
     * CIN_HasEmbeddingVectors mixin to indicate it has embeddings stored as children.</p>
     *
     * @param documentId hxpr document identifier
     * @param embeddings complete list of embeddings to store
     * @param knownChildren the document's embedding children when the caller already has them,
     *                      {@code null} to look them up
     */
    private void updateEmbeddingsInBatches(String documentId,
                                           List<HxprEmbedding> embeddings,
                                           List<EmbeddingChild> knownChildren) {
        log.info("Document {} has {} embeddings. Storing as Parquet file in child document (embedding type: {})",
                documentId, embeddings.size(), embeddingType);

        try {
            // 1. Generate Parquet file
            byte[] parquetContent = ParquetEmbeddingWriter.writeToParquet(embeddings, embeddingType);

            // 2. Add parent mixin to indicate it has embedding children
            ensureEmbeddingParentMixin(documentId);

            // 3. Delete old embedding child if exists
            deleteEmbeddingChildren(documentId, embeddingType, knownChildren);

            // 4. Create child document with Parquet file
            createEmbeddingChild(documentId, embeddingType, parquetContent);

            log.info("Successfully stored {} embeddings as Parquet file ({} bytes) for document {}",
                    embeddings.size(), parquetContent.length, documentId);

        } catch (Exception e) {
            log.error("Failed to store embeddings as Parquet for document {}: {}", documentId, e.getMessage(), e);
            throw new RuntimeException("Failed to store embeddings as Parquet for document " + documentId, e);
        }
    }

    /**
     * Ensures the parent document has the CIN_HasEmbeddingVectors mixin.
     */
    private void ensureEmbeddingParentMixin(String documentId) {
        try {
            HxprDocument doc = documentApi.getById(documentId);
            List<String> mixins = doc.getSysMixinTypes();

            if (mixins == null || !mixins.contains(EMBEDDING_PARENT_MIXIN)) {
                log.debug("Adding {} mixin to document {}", EMBEDDING_PARENT_MIXIN, documentId);

                List<String> newMixins = mixins != null ? new ArrayList<>(mixins) : new ArrayList<>();
                newMixins.add(EMBEDDING_PARENT_MIXIN);

                documentApi.updateById(documentId, Map.of("sys_mixinTypes", newMixins));
            }
        } catch (Exception e) {
            log.warn("Failed to add parent embedding mixin to {}: {}", documentId, e.getMessage());
        }
    }

    /**
     * One embedding child of a document.
     *
     * @param sysId         hxpr identifier of the child document
     * @param sysName       child name, always {@code _e_{embeddingType}} possibly with an
     *                      auto-suffix hxpr appended for a name collision
     * @param embeddingType the type recovered from {@code sysName}
     */
    public record EmbeddingChild(String sysId, String sysName, String embeddingType) {
    }

    /**
     * Lists every embedding child of a document, whatever embedding type it was written under.
     *
     * <p>Type-agnostic on purpose: it is what makes an orphaned child from a retired model visible,
     * and what lets the clear path remove children it did not write. Children are matched on the
     * {@code _e_} name prefix client-side rather than through an HXQL {@code LIKE}, because
     * {@code _} is a single-character wildcard in {@code LIKE} and
     * {@link AclFilterBuilder#escapeLiteral} escapes only {@code \} and {@code '} -- so
     * {@code LIKE '_e_%'} matches any three-characters-then-anything name.</p>
     *
     * @param documentId hxpr document identifier
     * @return the document's embedding children, empty when it has none
     * @throws RuntimeException if the lookup fails. Callers about to write a replacement child must
     *         abort, otherwise a stale child would survive alongside the new one.
     */
    public List<EmbeddingChild> listEmbeddingChildren(String documentId) {
        return listEmbeddingChildren(documentId, true);
    }

    /**
     * Lists a document's embedding children, optionally without first waiting for the index.
     *
     * <p>The wait costs up to {@link #INDEX_WAIT_TIMEOUT_SECONDS} seconds <em>per call</em>, which is
     * the right trade for a single sync about to replace a child but dominates a job that walks the
     * whole corpus. Such a caller waits once before it starts and passes {@code false} here.</p>
     *
     * @param documentId  hxpr document identifier
     * @param waitForIndex whether to wait for full-text indexing before querying. Pass {@code true}
     *                     when a stale answer would let a duplicate child be created.
     */
    public List<EmbeddingChild> listEmbeddingChildren(String documentId, boolean waitForIndex) {
        // hxpr's query index is eventually consistent: a child created on a prior sync may not yet
        // be visible here, so the lookup would miss it and a re-sync would create a duplicate. Wait
        // for the index to catch up before querying (same eventual-consistency class as #78).
        if (waitForIndex) {
            awaitIndex(documentId);
        }

        String hxql = String.format(
                "SELECT * FROM SysContent WHERE sys_parentId = '%s'",
                AclFilterBuilder.escapeLiteral(documentId)
        );

        List<EmbeddingChild> children = new ArrayList<>();
        for (int page = 0; page < EMBEDDING_CHILD_MAX_PAGES; page++) {
            // hxpr treats limit=0 (the Query default) as "return no rows", so an explicit
            // positive limit is required or the lookup silently matches nothing and the old
            // child is never deleted -- the root cause of duplicated embeddings on re-sync.
            HxprDocument.QueryResult queryResult = queryApi.query(
                    newQuery(hxql, EMBEDDING_CHILD_PAGE_SIZE, page * EMBEDDING_CHILD_PAGE_SIZE));
            List<HxprDocument> results = queryResult == null ? null : queryResult.getDocuments();

            if (results == null || results.isEmpty()) {
                break;
            }

            for (HxprDocument child : results) {
                String name = child.getSysName();
                if (name != null && name.startsWith(EMBEDDING_CHILD_PREFIX)) {
                    children.add(new EmbeddingChild(child.getSysId(), name,
                            name.substring(EMBEDDING_CHILD_PREFIX.length())));
                }
            }

            if (results.size() < EMBEDDING_CHILD_PAGE_SIZE) {
                break;
            }
        }

        return children;
    }

    /**
     * Waits for hxpr's full-text index to catch up, so a child written by a prior call is visible.
     *
     * <p>Best effort: a failed wait is logged and the lookup proceeds, because a stale answer is
     * better than no answer for every caller that has one.</p>
     */
    public void awaitIndex(String context) {
        try {
            queryApi.waitForFullTextSearchIndexing(true, INDEX_WAIT_TIMEOUT_SECONDS);
        } catch (Exception e) {
            log.warn("waitForFullTextSearchIndexing failed for {}: {}", context, e.getMessage());
        }
    }

    /**
     * Deletes every embedding child of a document, whatever type it was written under.
     *
     * @throws RuntimeException if the lookup or any delete fails; the caller must abort
     *         before creating a new child, otherwise duplicates would survive.
     */
    private void deleteEmbeddingChildren(String documentId) {
        deleteEmbeddingChildren(documentId, null, null);
    }

    /**
     * Deletes a document's embedding children, optionally narrowed to one embedding type.
     *
     * <p>A null {@code onlyType} removes every child, whatever type it carries. A non-null one removes
     * only the child whose type matches <em>exactly</em>, which is precisely what the write path needs:
     * the create that follows uses {@code enforceSysName=true} and so would 409 on a surviving
     * {@code _e_{onlyType}}, and no other child can block it.</p>
     *
     * <p>The match is deliberately exact rather than a prefix. A prefix match cannot tell a sibling type
     * from a suffixed version of this one: with {@code onlyType} of {@code ai-mxbai-embed-large}, an
     * existing {@code _e_ai-mxbai-embed-large-v2} child starts with it and was therefore deleted, so a
     * sync under one model silently destroyed another model's vectors. That is the opposite of what
     * multi-type retrieval exists for, and it fails in the dangerous direction: the deletion succeeds,
     * the sync reports success, and the loss only shows up as missing search results. Derived types are
     * not reserved prefixes of one another -- {@code nomic-embed-text-v1} and
     * {@code nomic-embed-text-v1.5} are both legitimate and one prefixes the other -- so there is no
     * prefix rule that is safe.</p>
     *
     * <p>What the prefix match also did, and this no longer does, is mop up children hxpr auto-suffixed
     * as {@code _e_{type}.{random}} when a name collided. Those cannot be created any more: the child
     * create has enforced {@code sys_name} uniqueness since #80 and returns 409 instead. Any left in an
     * index written before that are removed by clearing the document's embeddings, which is
     * type-agnostic ({@link #deleteEmbeddings}), rather than by guessing from a name which type a child
     * belongs to. Guessing is what lost data.</p>
     *
     * <p>{@code knownChildren} lets a caller that has already listed the children skip the lookup, and
     * with it the per-call index wait (#127). A null value lists them here, waiting for the index first.</p>
     *
     * @throws RuntimeException if the lookup or any delete fails; the caller must abort
     *         before creating a new child, otherwise duplicates would survive.
     */
    private void deleteEmbeddingChildren(String documentId, String onlyType,
                                         List<EmbeddingChild> knownChildren) {
        List<EmbeddingChild> children =
                knownChildren != null ? knownChildren : listEmbeddingChildren(documentId);
        for (EmbeddingChild child : children) {
            if (!isDeletable(child.embeddingType(), onlyType)) {
                continue;
            }
            log.debug("Deleting existing embedding child: {} ({})", child.sysId(), child.sysName());
            documentApi.deleteById(child.sysId());
        }
    }

    /**
     * Whether a child of {@code childType} is in scope for a delete narrowed to {@code onlyType}.
     *
     * <p>Exact equality, or everything when {@code onlyType} is null. Package-private so the rule can be
     * asserted directly: it is one comparison, but getting it wrong deletes another model's vectors.</p>
     */
    static boolean isDeletable(String childType, String onlyType) {
        return onlyType == null || onlyType.equals(childType);
    }

    /**
     * Creates a child document containing the Parquet file with embeddings.
     *
     * Follows the HXPR Content Lake specification:
     * 1. Create upload slot via POST /api/upload/create
     * 2. Upload Parquet bytes via POST /api/upload?id={uploadId}
     * 3. Create SysEmbeddings child document referencing the uploadId
     */
    private void createEmbeddingChild(String documentId, String embeddingType, byte[] parquetContent) {
        // Child document name MUST start with "_e_" prefix per specification
        String childName = EMBEDDING_CHILD_PREFIX + embeddingType;

        try {
            // Step 1: Create upload slot (no request body needed)
            log.info("Creating upload slot for embedding Parquet file");
            Map<String, String> uploadSlotResponse = restClient.post()
                    .uri("/api/upload/create")
                    .retrieve()
                    .body(new org.springframework.core.ParameterizedTypeReference<Map<String, String>>() {});

            String uploadId = uploadSlotResponse.get("id");
            if (uploadId == null) {
                throw new RuntimeException("Failed to get uploadId from upload/create response");
            }

            log.info("Created upload slot: {}", uploadId);

            // Step 2: Upload Parquet bytes
            log.info("Uploading Parquet file ({} bytes) to uploadId: {}", parquetContent.length, uploadId);
            restClient.post()
                    .uri("/api/upload?id=" + uploadId +
                         "&fileName=embeddings.parquet" +
                         "&mimeType=application/x-parquet")
                    .contentType(MediaType.APPLICATION_OCTET_STREAM)
                    .body(parquetContent)
                    .retrieve()
                    .toBodilessEntity();

            log.info("Successfully uploaded Parquet file");

            // Step 3: Create SysEmbeddings child document
            Map<String, Object> childDoc = Map.of(
                    "sys_primaryType", "SysEmbeddings",
                    "sys_name", childName,
                    "sys_title", "Embeddings",
                    "sysemb_embeddings", Map.of("uploadId", uploadId)
            );

            // enforceSysName=true makes hxpr reject a duplicate child name instead of
            // silently auto-suffixing it (e.g. _e_mxbai-embed-large.<n>), which would
            // otherwise let a stale child survive alongside the new one after a re-sync.
            log.info("Creating SysEmbeddings child document: {} with payload: {}", childName, childDoc);
            restClient.post()
                    .uri("/api/documents/" + documentId + "?enforceSysName=true")
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(childDoc)
                    .retrieve()
                    .toBodilessEntity();

            log.info("Successfully created SysEmbeddings child document: {} (uploadId: {})", childName, uploadId);
        } catch (Exception e) {
            log.error("Failed to create embedding child for {}: {}", documentId, e.getMessage(), e);
            throw new RuntimeException("Failed to create embedding child document", e);
        }
    }

    /**
     * Removes every stored embedding for a document, regardless of the embedding type it was
     * written under.
     *
     * <p>Deletes the Parquet {@code SysEmbeddings} child document(s) where embeddings are stored,
     * and also clears the deprecated inline {@code sysembed_embeddings} array when the legacy
     * {@code SysEmbed} mixin is present, so documents indexed before the Parquet migration are
     * cleaned up too.</p>
     *
     * <p>Type-agnostic deliberately: a document may carry children from a previously configured
     * model, including the literal {@code _e_mxbai-embed-large} written before the type was derived.
     * Naming only the current type would leave those behind, and an orphaned child stays searchable
     * because the read path substitutes the {@code *} embedding-type wildcard (#113).</p>
     *
     * @param documentId hxpr document identifier
     * @throws RuntimeException if the lookup or any delete fails
     */
    public void deleteEmbeddings(String documentId) {
        log.info("Clearing embeddings for document: {}", documentId);

        // Current storage: Parquet child document(s), any embedding type.
        deleteEmbeddingChildren(documentId);

        // Legacy storage: inline sysembed_embeddings array (pre-Parquet documents).
        HxprDocument doc = documentApi.getById(documentId);
        if (doc != null && hasSysEmbedMixin(doc)) {
            documentApi.updateById(documentId, Map.of("sysembed_embeddings", List.of()));
        }

        log.info("Cleared embeddings for document: {}", documentId);
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
            // Each predicate is an independent quick-filter clause AND-ed by hxpr, rather than a
            // single concatenated HXQL WHERE string.
            List<String> clauses = new ArrayList<>();
            clauses.add("sys_primaryType = '" + SYS_FILE + "'");
            clauses.add("cin_id = '" + AclFilterBuilder.escapeLiteral(nodeId) + "'");
            if (sourceId != null && !sourceId.isBlank()) {
                clauses.add(buildSourceIdPredicate(sourceId));
            }

            HxprDocument.QueryResult result = advancedQuery(DEFAULT_QUERY, clauses, 2, 0);
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
                    .map(id -> "cin_id = '" + AclFilterBuilder.escapeLiteral(id) + "'")
                    .collect(Collectors.joining(" OR ", "(", ")"));

            // Independent quick-filter clauses (the id predicate is a single OR-clause).
            List<String> clauses = new ArrayList<>();
            clauses.add("sys_primaryType = '" + SYS_FILE + "'");
            clauses.add(idPredicate);
            if (sourceId != null && !sourceId.isBlank()) {
                clauses.add(buildSourceIdPredicate(sourceId));
            }

            HxprDocument.QueryResult result = advancedQuery(DEFAULT_QUERY, clauses, sanitizedIds.size() * 2, 0);
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
     * Visits every indexed document of one source, paging by offset. For reconciliation sweeps that
     * compare the index against what a discovery pass saw.
     *
     * <p>Matches on an <em>equality</em> clause over {@code cin_sourceId}: HXQL answers HTTP 400 for
     * {@code LIKE} on keyword-mapped fields and most {@code cin_*} fields are keyword, so prefix
     * matching is unavailable and the caller must supply its exact source id. Both the
     * {@code type:rawId} form and the legacy bare raw id are matched, so documents indexed before
     * that migration are visited too.</p>
     *
     * <p>{@code SysEmbeddings} children are never visited: they carry no {@code cin_sourceId}.</p>
     *
     * @param sourceId exact {@code "<sourceType>:<sourceId>"}
     * @param pageSize documents per request
     * @param consumer receives each document once
     * @return the number of documents visited
     * @throws RuntimeException if a page query fails. A sweep must not treat a partial scan as a
     *         complete one: that would delete every document the failed pages would have covered.
     */
    public int forEachDocumentOfSource(String sourceId, int pageSize, Consumer<HxprDocument> consumer) {
        int effectivePageSize = Math.max(1, pageSize);
        int visited = 0;
        long reportedTotal = -1;

        for (int offset = 0; ; offset += effectivePageSize) {
            HxprDocument.QueryResult result = advancedQuery(
                    DEFAULT_QUERY, List.of(buildSourceIdPredicate(sourceId)), effectivePageSize, offset);

            List<HxprDocument> documents = result == null ? null : result.getDocuments();
            if (documents == null || documents.isEmpty()) {
                return visited;
            }

            // QueryResult.totalCount is a primitive, so an untracked total is indistinguishable from
            // zero. A zero alongside a non-empty page means "not reported", and the page cap below
            // takes over as the loop bound.
            if (reportedTotal < 0 && result.getTotalCount() > 0) {
                reportedTotal = result.getTotalCount();
            }

            for (HxprDocument document : documents) {
                consumer.accept(document);
                visited++;
            }

            if (documents.size() < effectivePageSize) {
                return visited;
            }

            // A non-advancing offset would otherwise loop forever. The reported total bounds the
            // scan; without one, fall back to a page cap so the sweep cannot hang a sync.
            if (reportedTotal >= 0 && visited >= reportedTotal) {
                return visited;
            }
            if (reportedTotal < 0 && offset / effectivePageSize >= MAX_SOURCE_SCAN_PAGES) {
                log.warn("Source scan for {} stopped after {} pages with no reported total; "
                        + "treat the result as partial", sourceId, MAX_SOURCE_SCAN_PAGES);
                return visited;
            }
        }
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
     * Executes an advanced query: a base HXQL query plus a list of independent quick-filter
     * clauses, each of which hxpr AND-s onto the result. Prefer this over string-concatenating
     * predicates into a single HXQL {@code WHERE}.
     *
     * @param baseQuery         base HXQL query, or a default query when {@code null}
     * @param quickFilterClauses independent filter clauses (each AND-ed), may be empty
     * @param limit             max results
     * @param offset            result offset
     * @return query result
     */
    public HxprDocument.QueryResult advancedQuery(String baseQuery, List<String> quickFilterClauses,
                                                  int limit, int offset) {
        AdvancedQuery aq = new AdvancedQuery();
        aq.setQuery(baseQuery != null ? baseQuery : DEFAULT_QUERY);
        if (quickFilterClauses != null && !quickFilterClauses.isEmpty()) {
            aq.setQuickFilterClauses(quickFilterClauses);
        }
        aq.setLimit((long) limit);
        aq.setOffset((long) offset);
        aq.setTrackTotalCount(true);
        return queryApi.advancedQuery(aq);
    }

    /**
     * Executes a pre-registered named query.
     *
     * @param queryName            name of a named-query definition registered in hxpr
     * @param selectedQuickFilters names of the definition's quick filters to apply, may be empty
     * @param limit                max results
     * @param offset               result offset
     * @return query result
     */
    public HxprDocument.QueryResult namedQuery(String queryName, List<String> selectedQuickFilters,
                                               int limit, int offset) {
        NamedQuery nq = new NamedQuery();
        nq.setQueryName(queryName);
        if (selectedQuickFilters != null && !selectedQuickFilters.isEmpty()) {
            nq.setSelectedQuickFilters(selectedQuickFilters);
        }
        nq.setLimit((long) limit);
        nq.setOffset((long) offset);
        nq.setTrackTotalCount(true);
        return queryApi.namedQuery(nq);
    }

    /** Returns the names of the named-query definitions registered in hxpr. */
    public List<String> listNamedQueries() {
        HxprNamedQueries result = queryApi.listNamedQueries();
        return (result != null && result.getNamedQueries() != null)
                ? result.getNamedQueries()
                : List.of();
    }

    /** Returns the full definition of a named query, or {@code null} if it is not registered. */
    public org.hyland.contentlake.hxpr.api.model.NamedQueryDefinition getNamedQuery(String queryName) {
        return queryApi.getNamedQuery(queryName);
    }

    /**
     * Terms aggregation: returns the top-N distinct values of {@code property} with their document
     * counts, scoped by an HXQL query (typically a permission filter).
     *
     * @param hxqlQuery  base HXQL scoping query, or a default query when {@code null}
     * @param property   the property to aggregate on (required by hxpr)
     * @param searchTerm optional term to filter the aggregated values, may be {@code null}
     * @param limit      max number of buckets
     * @return the aggregation buckets
     */
    public HxprTermsAggregationResult termsAggregation(String hxqlQuery, String property,
                                                       String searchTerm, int limit) {
        TermsAggregationsQuery taq = new TermsAggregationsQuery();
        taq.setTermsAggregationProperty(property);
        taq.setQuery(newQuery(hxqlQuery != null ? hxqlQuery : DEFAULT_QUERY, limit, 0));
        if (searchTerm != null && !searchTerm.isBlank()) {
            taq.setSearchTerm(searchTerm);
        }
        taq.setLimit(limit);
        return queryApi.termsAggregation(taq);
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
        return vectorSearch(vector, embeddingType, hxqlFilter, null, limit);
    }

    /**
     * Vector similarity search with an optional chunk-level fulltext filter.
     *
     * @param vector        query vector
     * @param embeddingType embedding type, or {@code "*"} when {@code null}
     * @param hxqlFilter    hxql filter, or a default query when {@code null}
     * @param chunkFTS      space-separated terms matched against chunk text by hxpr, or {@code null}
     * @param limit         max results
     * @return vector search result
     */
    public VectorSearchResult vectorSearch(List<Double> vector, String embeddingType, String hxqlFilter,
                                           String chunkFTS, int limit) {
        return vectorSearch(vector, embeddingType, hxqlFilter, chunkFTS, limit, 0);
    }

    /**
     * Vector similarity search over one page of embedding rows.
     *
     * <p>The offset exists for scans that walk the embeddings index rather than answer a query, which
     * is the only way to read the rows' own {@code sysembed_type}: there is no aggregation endpoint over
     * the embeddings index, {@code termsAggregation} covering the document index only.</p>
     *
     * @param vector        query vector
     * @param embeddingType embedding type, or {@code "*"} when {@code null}
     * @param hxqlFilter    hxql filter, or a default query when {@code null}
     * @param chunkFTS      space-separated terms matched against chunk text by hxpr, or {@code null}
     * @param limit         max results in this page
     * @param offset        index of the first row to return
     * @return vector search result
     */
    public VectorSearchResult vectorSearch(List<Double> vector, String embeddingType, String hxqlFilter,
                                           String chunkFTS, int limit, int offset) {
        VectorQuery vq = new VectorQuery();
        vq.setVector(vector);
        vq.setEmbeddingType(embeddingType != null ? embeddingType : "*");
        vq.setQuery(hxqlFilter != null ? hxqlFilter : DEFAULT_QUERY);
        if (chunkFTS != null && !chunkFTS.isBlank()) {
            vq.setChunkFTS(chunkFTS);
        }
        vq.setLimit((long) limit);
        vq.setOffset((long) offset);
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
            return "cin_sourceId = '" + AclFilterBuilder.escapeLiteral(variants.get(0)) + "'";
        }

        return variants.stream()
                .map(variant -> "cin_sourceId = '" + AclFilterBuilder.escapeLiteral(variant) + "'")
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

}
