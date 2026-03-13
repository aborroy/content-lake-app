package org.alfresco.contentlake.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.alfresco.contentlake.client.AlfrescoClient;
import org.alfresco.contentlake.client.HxprDocumentApi;
import org.alfresco.contentlake.client.HxprService;
import org.alfresco.contentlake.client.TransformClient;
import org.alfresco.contentlake.hxpr.api.model.ACE;
import org.alfresco.contentlake.hxpr.api.model.Group;
import org.alfresco.contentlake.model.ContentLakeIngestProperties;
import org.alfresco.contentlake.model.ContentLakeNodeStatus;
import org.alfresco.contentlake.hxpr.api.model.User;
import org.alfresco.contentlake.model.Chunk;
import org.alfresco.contentlake.model.HxprDocument;
import org.alfresco.contentlake.model.HxprEmbedding;
import org.alfresco.contentlake.service.chunking.SimpleChunkingService;
import org.alfresco.core.model.Node;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.http.HttpStatus;
import org.springframework.web.client.HttpClientErrorException;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.OffsetDateTime;
import java.util.*;

/**
 * Shared synchronisation pipeline used by both the batch-ingester and the
 * live-ingester to process a single Alfresco node into the Content Lake.
 *
 * <h3>Processing steps</h3>
 * <ol>
 *   <li>Fetch (or receive) the Alfresco {@link Node} with metadata + permissions</li>
 *   <li>Create or update the corresponding hxpr document (metadata phase)</li>
 *   <li>Extract plain text via the Transform Service</li>
 *   <li>Chunk the text and generate embeddings via Spring AI</li>
 *   <li>Store embeddings and fulltext in the hxpr document</li>
 * </ol>
 *
 * <h3>Idempotency</h3>
 * Every write is guarded by an {@code alfresco_modifiedAt} staleness check:
 * if the Content Lake already holds a version that is equal to or newer than
 * the incoming node, the write is skipped. This makes it safe to run both
 * ingesters concurrently against the same node.
 */
@Slf4j
@RequiredArgsConstructor
public class NodeSyncService {

    /* ---- hxpr type / mixin constants ---- */
    private static final String SYS_FILE         = "SysFile";
    private static final String MIXIN_CIN_REMOTE = "CinRemote";

    /* ---- cin_ingestProperties keys ---- */
    private static final String P_ALF_NODE_ID     = ContentLakeIngestProperties.ALFRESCO_NODE_ID;
    private static final String P_ALF_REPO_ID     = ContentLakeIngestProperties.ALFRESCO_REPOSITORY_ID;
    private static final String P_ALF_PATH        = ContentLakeIngestProperties.ALFRESCO_PATH;
    private static final String P_ALF_NAME        = ContentLakeIngestProperties.ALFRESCO_NAME;
    private static final String P_ALF_MIME        = ContentLakeIngestProperties.ALFRESCO_MIME_TYPE;
    private static final String P_ALF_MODIFIED_AT = ContentLakeIngestProperties.ALFRESCO_MODIFIED_AT;
    private static final String P_CL_SYNC_STATUS  = ContentLakeIngestProperties.CONTENT_LAKE_SYNC_STATUS;
    private static final String P_CL_SYNC_ERROR   = ContentLakeIngestProperties.CONTENT_LAKE_SYNC_ERROR;

    /* ---- ACL constants ---- */
    private static final String EVERYONE_PRINCIPAL = "__Everyone__";
    private static final String GROUP_PREFIX       = "GROUP_";
    private static final String PERMISSION_READ    = "Read";

    /* ---- text extraction helpers ---- */
    private static final String TARGET_MIME_TYPE = "text/plain";
    private static final String ERR_NO_EXTRACTABLE_TEXT = "No extractable text produced for mimeType=%s";
    private static final String ERR_NO_CHUNKS = "No chunks produced from extracted text";
    private static final Set<String> TEXT_MIME_TYPES = Set.of(
            "text/plain", "text/html", "text/xml", "text/csv",
            "text/markdown", "application/json", "application/xml",
            "application/javascript"
    );

    /* ---- dependencies ---- */
    private final AlfrescoClient alfrescoClient;
    private final HxprDocumentApi documentApi;
    private final HxprService hxprService;
    private final TransformClient transformClient;
    private final EmbeddingService embeddingService;
    private final SimpleChunkingService chunkingService;

    /* ---- hxpr path configuration ---- */
    private final String hxprTargetPath;
    private final String hxprPathRepositoryId;

    // ──────────────────────────────────────────────────────────────────────
    // Public pipeline entry-points
    // ──────────────────────────────────────────────────────────────────────

    /**
     * Full sync: metadata ingestion + content transformation + embedding.
     *
     * @param node Alfresco node (must include properties, path, and permissions)
     * @return the hxpr document id, or {@code null} if skipped due to staleness
     */
    public String syncNode(Node node) {
        String nodeId = node.getId();
        String sourceId = alfrescoClient.getRepositoryId();

        HxprDocument existing = hxprService.findByNodeId(nodeId, sourceId);
        if (existing != null && isStale(existing, node)) {
            log.debug("Skipping node {} — Content Lake version is already current", nodeId);
            return existing.getSysId();
        }

        HxprDocument doc = (existing != null)
                ? updateDocument(existing, node)
                : createDocument(node);

        String mimeType = node.getContent() != null ? node.getContent().getMimeType() : null;
        String documentPath = node.getPath() != null ? node.getPath().getName() : null;

        try {
            processContent(doc.getSysId(), doc.getCinIngestProperties(), nodeId, mimeType, node.getName(), documentPath);
        } catch (Exception e) {
            log.error("Content processing failed for node {}: {}", nodeId, e.getMessage(), e);
            // Metadata is already persisted; content will be retried on next event/batch.
        }

        return doc.getSysId();
    }

    /**
     * Metadata-only sync (Phase 1 of the batch pipeline).
     *
     * <p>Returns a lightweight descriptor that the caller can enqueue for
     * asynchronous content processing. This preserves backward compatibility
     * with {@code TransformationQueue} in the batch-ingester.</p>
     *
     * @param node Alfresco node
     * @return sync result with hxpr document id and node metadata
     */
    public SyncResult ingestMetadata(Node node) {
        String nodeId = node.getId();
        String sourceId = alfrescoClient.getRepositoryId();

        HxprDocument existing = hxprService.findByNodeId(nodeId, sourceId);
        if (existing != null && isStale(existing, node)) {
            log.debug("Skipping metadata for node {} — already current", nodeId);
            return new SyncResult(existing.getSysId(), nodeId,
                    node.getContent() != null ? node.getContent().getMimeType() : null,
                    node.getName(),
                    node.getPath() != null ? node.getPath().getName() : null,
                    true,
                    null);
        }

        HxprDocument doc = (existing != null)
                ? updateDocument(existing, node)
                : createDocument(node);

        return new SyncResult(doc.getSysId(), nodeId,
                node.getContent() != null ? node.getContent().getMimeType() : null,
                node.getName(),
                node.getPath() != null ? node.getPath().getName() : null,
                false,
                doc.getCinIngestProperties());
    }

    /**
     * Content processing: extract text, chunk, embed, store.
     *
     * <p>Can be called standalone when the caller already has the hxpr document
     * id from a prior metadata ingestion (batch-ingester's TransformationWorker).</p>
     *
     * @param baseIngestProps the {@code cin_ingestProperties} map from the metadata
     *                        phase; used to build the status patch without an extra GET
     */
    public void processContent(String hxprDocId, Map<String, Object> baseIngestProps,
                               String nodeId, String mimeType,
                               String documentName, String documentPath) {
        try {
            String text = extractText(nodeId, mimeType, documentName);
            if (text == null || text.isBlank()) {
                log.warn("Empty text for node {} ({})", nodeId, mimeType);
                patchSyncState(
                        hxprDocId,
                        baseIngestProps,
                        ContentLakeNodeStatus.Status.FAILED,
                        String.format(ERR_NO_EXTRACTABLE_TEXT, safeMimeType(mimeType))
                );
                return;
            }

            List<Chunk> chunks = chunkingService.chunk(text, nodeId, mimeType);
            if (chunks.isEmpty()) {
                log.warn("No chunks for node {}", nodeId);
                patchSyncState(hxprDocId, baseIngestProps, ContentLakeNodeStatus.Status.FAILED, ERR_NO_CHUNKS);
                return;
            }

            String docContext = buildDocumentContext(documentName, documentPath);
            List<EmbeddingService.ChunkWithEmbedding> embedded =
                    embeddingService.embedChunks(chunks, docContext);

            clearEmbeddings(hxprDocId);

            List<HxprEmbedding> hxprEmbeddings = toHxprEmbeddings(embedded);
            hxprService.updateEmbeddings(hxprDocId, hxprEmbeddings);

            updateFulltextWithStatus(hxprDocId, text, baseIngestProps);

            log.info("Completed sync for node {}: {} embeddings", nodeId, hxprEmbeddings.size());
        } catch (Exception e) {
            patchSyncState(hxprDocId, baseIngestProps, ContentLakeNodeStatus.Status.FAILED, e.getMessage());
            log.error("Content processing failed for node {}", nodeId, e);
            throw new RuntimeException("Content processing failed", e);
        }
    }

    /**
     * Deletes the Content Lake document (and its embeddings) for a given node.
     *
     * @param nodeId Alfresco node identifier
     */
    public void deleteNode(String nodeId) {
        deleteNode(nodeId, null);
    }

    /**
     * Deletes the Content Lake document for a given node when the delete event is
     * not older than the version already stored in the lake.
     *
     * @param nodeId Alfresco node identifier
     * @param deletedAt timestamp associated with the delete/update-to-out-of-scope event
     */
    public void deleteNode(String nodeId, OffsetDateTime deletedAt) {
        String sourceId = alfrescoClient.getRepositoryId();
        HxprDocument existing = hxprService.findByNodeId(nodeId, sourceId);
        if (existing == null) {
            log.debug("No Content Lake document found for deleted node {}", nodeId);
            return;
        }

        OffsetDateTime storedModifiedAt = getStoredModifiedAt(existing);
        if (deletedAt != null && storedModifiedAt != null && storedModifiedAt.isAfter(deletedAt)) {
            log.info("Skipping delete for node {} — Content Lake document is newer than delete event", nodeId);
            return;
        }

        try {
            documentApi.deleteById(existing.getSysId());
            log.info("Deleted Content Lake document {} for node {}", existing.getSysId(), nodeId);
        } catch (Exception e) {
            log.error("Failed to delete Content Lake document for node {}: {}", nodeId, e.getMessage());
        }
    }

    /**
     * Updates only the ACL on an existing Content Lake document.
     *
     * @param nodeId Alfresco node identifier
     * @param node   node with updated permissions
     */
    public void updatePermissions(String nodeId, Node node) {
        String sourceId = alfrescoClient.getRepositoryId();
        HxprDocument existing = hxprService.findByNodeId(nodeId, sourceId);
        if (existing == null) {
            log.debug("No Content Lake document found for permission update on node {}", nodeId);
            return;
        }

        List<String> readerList = new ArrayList<>(alfrescoClient.extractReadAuthorities(node));
        List<ACE> sysAcl = buildSysAcl(readerList);

        HxprDocument update = new HxprDocument();
        update.setSysAcl(sysAcl);
        documentApi.updateById(existing.getSysId(), update);

        log.info("Updated ACL for Content Lake document {} (node {})", existing.getSysId(), nodeId);
    }

    // ──────────────────────────────────────────────────────────────────────
    // Staleness check
    // ──────────────────────────────────────────────────────────────────────

    /**
     * Returns {@code true} when the Content Lake version is already at or beyond
     * the incoming node's {@code modifiedAt} timestamp.
     */
    private boolean isStale(HxprDocument existing, Node incoming) {
        if (incoming.getModifiedAt() == null) {
            return false;
        }

        OffsetDateTime storedDate = getStoredModifiedAt(existing);
        if (storedDate == null) {
            return false;
        }

        OffsetDateTime incomingDate = incoming.getModifiedAt();
        return !incomingDate.isAfter(storedDate);
    }

    // ──────────────────────────────────────────────────────────────────────
    // Document CRUD helpers
    // ──────────────────────────────────────────────────────────────────────

    private HxprDocument createDocument(Node node) {
        String pathRepoId = resolvePathRepositoryId();
        String parentPath = buildContentLakeParentPath(node, pathRepoId);
        hxprService.ensureFolder(parentPath);

        HxprDocument doc = buildDocument(node);
        String documentPath = buildDocumentPath(parentPath, node);
        doc.setCinPaths(List.of(documentPath));

        HxprDocument existingAtPath = hxprService.findByPath(documentPath);
        if (existingAtPath != null) {
            log.info("Reusing existing hxpr document {} for node {} at {}",
                    existingAtPath.getSysId(), node.getId(), documentPath);
            return updateDocument(existingAtPath, node);
        }

        try {
            HxprDocument created = hxprService.createDocument(parentPath, doc);
            log.info("Created hxpr document {} for node {} at {}",
                    created.getSysId(), node.getId(), parentPath);
            return created;
        } catch (HttpClientErrorException.Conflict e) {
            HxprDocument conflicted = hxprService.findByPath(documentPath);
            if (conflicted != null) {
                log.warn("Recovered from create conflict by reusing hxpr document {} for node {} at {}",
                        conflicted.getSysId(), node.getId(), documentPath);
                return updateDocument(conflicted, node);
            }
            throw e;
        }
    }

    private HxprDocument updateDocument(HxprDocument existing, Node node) {
        HxprDocument doc = buildDocument(node);
        doc.setSysId(existing.getSysId());
        doc.setSysMixinTypes(mergeMixinTypes(existing.getSysMixinTypes(), doc.getSysMixinTypes()));
        HxprDocument updated = documentApi.updateById(existing.getSysId(), doc);
        log.info("Updated hxpr document {} for node {}", updated.getSysId(), node.getId());
        return updated;
    }

    private HxprDocument buildDocument(Node node) {
        HxprDocument doc = new HxprDocument();
        doc.setSysPrimaryType(SYS_FILE);
        doc.setSysName(resolveDocumentName(node));
        doc.setSysMixinTypes(List.of(MIXIN_CIN_REMOTE));

        doc.setCinId(node.getId());
        doc.setCinSourceId(alfrescoClient.getRepositoryId());
        doc.setCinPaths(buildCinPaths(node));

        List<String> readerList = new ArrayList<>(alfrescoClient.extractReadAuthorities(node));
        doc.setSysAcl(buildSysAcl(readerList));

        Map<String, Object> props = buildIngestProperties(node, doc.getCinSourceId());
        doc.setCinIngestProperties(props);
        doc.setCinIngestPropertyNames(new ArrayList<>(props.keySet()));

        applySyncState(doc, ContentLakeNodeStatus.Status.PENDING, null);

        applyFlattenedAlfrescoFields(doc, node, doc.getCinSourceId(), readerList);
        return doc;
    }

    // ──────────────────────────────────────────────────────────────────────
    // ACL mapping
    // ──────────────────────────────────────────────────────────────────────

    private List<ACE> buildSysAcl(List<String> authorities) {
        List<ACE> acl = new ArrayList<>();
        String suffix = "_#_" + alfrescoClient.getRepositoryId();

        for (String authority : authorities) {
            if ("GROUP_EVERYONE".equals(authority)) {
                acl.add(buildUserAce(EVERYONE_PRINCIPAL));
            } else if (authority.startsWith(GROUP_PREFIX)) {
                acl.add(buildGroupAce(authority + suffix));
            } else {
                acl.add(buildUserAce(authority + suffix));
            }
        }
        return acl;
    }

    private ACE buildUserAce(String userId) {
        ACE ace = new ACE();
        ace.setGranted(true);
        ace.setPermission(PERMISSION_READ);
        User user = new User();
        user.setId(userId);
        ace.setUser(user);
        return ace;
    }

    private ACE buildGroupAce(String groupId) {
        ACE ace = new ACE();
        ace.setGranted(true);
        ace.setPermission(PERMISSION_READ);
        Group group = new Group();
        group.setId(groupId);
        ace.setGroup(group);
        return ace;
    }

    // ──────────────────────────────────────────────────────────────────────
    // Text extraction
    // ──────────────────────────────────────────────────────────────────────

    private String extractText(String nodeId, String mimeType, String documentName) throws IOException {
        if (mimeType == null || mimeType.isBlank()) {
            log.info("Skipping content extraction for node {}: missing MIME type", nodeId);
            return null;
        }

        if (isTextMimeType(mimeType)) {
            byte[] content = alfrescoClient.getContent(nodeId);
            return new String(content, StandardCharsets.UTF_8);
        }

        if (!transformClient.isTransformSupported(mimeType, TARGET_MIME_TYPE)) {
            log.info("Skipping content extraction for node {}: unsupported transform {} -> {}",
                    nodeId, mimeType, TARGET_MIME_TYPE);
            return null;
        }

        String tempFileName = resolveTempFileName(nodeId, documentName, mimeType);
        Resource temp = alfrescoClient.downloadContentToTempFile(nodeId, tempFileName);
        try {
            byte[] out = transformClient.transformSync(temp, mimeType, TARGET_MIME_TYPE);
            return out == null ? null : new String(out, StandardCharsets.UTF_8);
        } catch (HttpClientErrorException e) {
            if (isUnsupportedTransformError(e)) {
                log.info("Skipping content extraction for node {}: transform service does not support {} -> {}",
                        nodeId, mimeType, TARGET_MIME_TYPE);
                return null;
            }
            throw e;
        } finally {
            deleteTempFile(temp);
        }
    }

    private boolean isUnsupportedTransformError(HttpClientErrorException e) {
        return e.getStatusCode() == HttpStatus.BAD_REQUEST
                && e.getResponseBodyAsString().contains("No transforms for:");
    }

    // ──────────────────────────────────────────────────────────────────────
    // Embedding helpers
    // ──────────────────────────────────────────────────────────────────────

    private List<HxprEmbedding> toHxprEmbeddings(List<EmbeddingService.ChunkWithEmbedding> embeddings) {
        List<HxprEmbedding> result = new ArrayList<>(embeddings.size());
        for (EmbeddingService.ChunkWithEmbedding cwe : embeddings) {
            HxprEmbedding emb = new HxprEmbedding();
            emb.setText(cwe.chunk().getText());
            emb.setVector(cwe.embedding());
            emb.setType(embeddingService.getModelName());
            emb.setLocation(buildLocation(cwe.chunk().getIndex()));
            emb.setChunkId(cwe.chunk().getId());
            result.add(emb);
        }
        return result;
    }

    private HxprEmbedding.EmbeddingLocation buildLocation(int paragraphIndex) {
        HxprEmbedding.EmbeddingLocation loc = new HxprEmbedding.EmbeddingLocation();
        HxprEmbedding.EmbeddingLocation.TextLocation txt = new HxprEmbedding.EmbeddingLocation.TextLocation();
        txt.setParagraph(paragraphIndex);
        loc.setText(txt);
        return loc;
    }

    private void clearEmbeddings(String hxprDocId) {
        try {
            hxprService.deleteEmbeddings(hxprDocId);
        } catch (Exception e) {
            log.debug("No existing embeddings to clear for {}", hxprDocId);
        }
    }

    /**
     * Writes fulltext and INDEXED status in a single PATCH, eliminating a separate
     * round-trip compared to calling updateFulltext + a status update separately.
     */
    private void updateFulltextWithStatus(String hxprDocId, String text, Map<String, Object> baseIngestProps) {
        Map<String, Object> props = buildStatusedProps(baseIngestProps, ContentLakeNodeStatus.Status.INDEXED, null);
        HxprDocument update = new HxprDocument();
        update.setSysFulltextBinary(text);
        update.setSyncStatus(HxprDocument.SyncStatus.INDEXED);
        update.setSyncError(null);
        update.setCinIngestProperties(props);
        update.setCinIngestPropertyNames(new ArrayList<>(props.keySet()));
        documentApi.updateById(hxprDocId, update);
    }

    /**
     * Patches only the sync-status fields without fetching the document first.
     * {@code baseIngestProps} carries all non-status properties from the metadata phase.
     */
    private void patchSyncState(String hxprDocId, Map<String, Object> baseIngestProps,
                                ContentLakeNodeStatus.Status status, String error) {
        try {
            Map<String, Object> props = buildStatusedProps(baseIngestProps, status, error);
            HxprDocument update = new HxprDocument();
            update.setSyncStatus(toInternalStatus(status));
            update.setSyncError(error);
            update.setCinIngestProperties(props);
            update.setCinIngestPropertyNames(new ArrayList<>(props.keySet()));
            documentApi.updateById(hxprDocId, update);
        } catch (Exception e) {
            log.warn("Failed to update sync status {} for document {}: {}", status, hxprDocId, e.getMessage());
        }
    }

    private Map<String, Object> buildStatusedProps(Map<String, Object> baseProps,
                                                   ContentLakeNodeStatus.Status status, String error) {
        Map<String, Object> props = baseProps != null ? new LinkedHashMap<>(baseProps) : new LinkedHashMap<>();
        props.put(P_CL_SYNC_STATUS, status.name());
        if (error == null || error.isBlank()) {
            props.remove(P_CL_SYNC_ERROR);
        } else {
            props.put(P_CL_SYNC_ERROR, error);
        }
        return props;
    }

    private void applySyncState(HxprDocument doc, ContentLakeNodeStatus.Status status, String error) {
        doc.setSyncStatus(toInternalStatus(status));
        doc.setSyncError(error);

        Map<String, Object> props = doc.getCinIngestProperties() != null
                ? new LinkedHashMap<>(doc.getCinIngestProperties())
                : new LinkedHashMap<>();

        props.put(P_CL_SYNC_STATUS, status.name());
        if (error == null || error.isBlank()) {
            props.remove(P_CL_SYNC_ERROR);
        } else {
            props.put(P_CL_SYNC_ERROR, error);
        }

        doc.setCinIngestProperties(props);
        doc.setCinIngestPropertyNames(new ArrayList<>(props.keySet()));
    }

    private HxprDocument.SyncStatus toInternalStatus(ContentLakeNodeStatus.Status status) {
        return switch (status) {
            case PENDING -> HxprDocument.SyncStatus.PENDING;
            case INDEXED -> HxprDocument.SyncStatus.INDEXED;
            case FAILED -> HxprDocument.SyncStatus.FAILED;
        };
    }

    private String buildDocumentContext(String documentName, String documentPath) {
        StringBuilder ctx = new StringBuilder();
        if (documentName != null && !documentName.isBlank()) {
            ctx.append("Document: ").append(documentName);
        }
        if (documentPath != null && !documentPath.isBlank()) {
            if (!ctx.isEmpty()) ctx.append(" | ");
            ctx.append("Path: ").append(documentPath);
        }
        return ctx.isEmpty() ? null : ctx.toString();
    }

    // ──────────────────────────────────────────────────────────────────────
    // Path helpers
    // ──────────────────────────────────────────────────────────────────────

    private String buildContentLakeParentPath(Node node, String repositoryId) {
        String base = buildRepositoryRootPath(repositoryId);
        if (node.getPath() == null || node.getPath().getName() == null
                || node.getPath().getName().isBlank()) {
            return base;
        }
        String alfrescoPath = normalizeAbsolutePath(node.getPath().getName());
        return "/".equals(base) ? alfrescoPath : base + alfrescoPath;
    }

    private String buildRepositoryRootPath(String repositoryId) {
        String targetPath = normalizeAbsolutePath(hxprTargetPath);
        if (repositoryId == null || repositoryId.isBlank()) return targetPath;
        String clean = repositoryId.startsWith("/") ? repositoryId.substring(1) : repositoryId;
        return joinPath(targetPath, clean);
    }

    private String resolvePathRepositoryId() {
        if (hxprPathRepositoryId != null && !hxprPathRepositoryId.isBlank()) {
            return hxprPathRepositoryId.trim();
        }
        return alfrescoClient.getRepositoryId();
    }

    private Map<String, Object> buildIngestProperties(Node node, String repositoryId) {
        Map<String, Object> props = new LinkedHashMap<>();
        props.put(P_ALF_NODE_ID, node.getId());
        props.put(P_ALF_REPO_ID, repositoryId);
        props.put(P_ALF_NAME, node.getName());
        props.put(P_ALF_PATH, node.getPath() != null ? node.getPath().getName() : null);
        props.put(P_ALF_MIME, node.getContent() != null ? node.getContent().getMimeType() : null);
        props.put(P_ALF_MODIFIED_AT, node.getModifiedAt() != null ? node.getModifiedAt().toString() : null);
        props.values().removeIf(Objects::isNull);
        return props;
    }

    private void applyFlattenedAlfrescoFields(HxprDocument doc, Node node, String repositoryId, List<String> readerList) {
        doc.setAlfrescoNodeId(node.getId());
        doc.setAlfrescoRepositoryId(repositoryId);
        doc.setAlfrescoName(node.getName());
        doc.setAlfrescoPath(node.getPath() != null ? node.getPath().getName() : null);
        doc.setAlfrescoMimeType(node.getContent() != null ? node.getContent().getMimeType() : null);
        doc.setAlfrescoModifiedAt(node.getModifiedAt() != null ? node.getModifiedAt().toString() : null);
        doc.setAlfrescoReadAuthorities(readerList);
    }

    private List<String> buildCinPaths(Node node) {
        String repoId = resolvePathRepositoryId();
        String parentPath = buildContentLakeParentPath(node, repoId);
        return List.of(buildDocumentPath(parentPath, node));
    }

    private String buildDocumentPath(String parentPath, Node node) {
        return joinPath(parentPath, resolveDocumentName(node));
    }

    private String resolveDocumentName(Node node) {
        return (node.getName() != null && !node.getName().isBlank()) ? node.getName() : node.getId();
    }

    private static String normalizeAbsolutePath(String path) {
        if (path == null || path.isBlank()) return "/";
        String n = path.startsWith("/") ? path : "/" + path;
        return (n.length() > 1 && n.endsWith("/")) ? n.substring(0, n.length() - 1) : n;
    }

    private static String joinPath(String parent, String leaf) {
        String p = normalizeAbsolutePath(parent);
        return "/".equals(p) ? "/" + leaf : p + "/" + leaf;
    }

    private String safeMimeType(String mimeType) {
        if (mimeType == null || mimeType.isBlank()) {
            return "unknown";
        }
        return mimeType;
    }

    private List<String> mergeMixinTypes(List<String> existingMixins, List<String> desiredMixins) {
        LinkedHashSet<String> merged = new LinkedHashSet<>();
        if (existingMixins != null) {
            merged.addAll(existingMixins);
        }
        if (desiredMixins != null) {
            merged.addAll(desiredMixins);
        }
        return new ArrayList<>(merged);
    }

    // ──────────────────────────────────────────────────────────────────────
    // Utility
    // ──────────────────────────────────────────────────────────────────────

    private boolean isTextMimeType(String mimeType) {
        if (mimeType == null) return false;
        if (TEXT_MIME_TYPES.contains(mimeType)) return true;
        return mimeType.startsWith("text/") || mimeType.endsWith("+xml") || mimeType.endsWith("+json");
    }

    private String extensionForMimeType(String mimeType) {
        if (mimeType == null) return "";
        return switch (mimeType) {
            case "application/pdf" -> ".pdf";
            case "text/plain" -> ".txt";
            case "text/html" -> ".html";
            default -> "";
        };
    }

    private String resolveTempFileName(String nodeId, String documentName, String mimeType) {
        if (documentName != null && !documentName.isBlank()) {
            return documentName;
        }
        return nodeId + extensionForMimeType(mimeType);
    }

    private void deleteTempFile(Resource resource) {
        if (resource instanceof FileSystemResource fsr) {
            try { Files.deleteIfExists(fsr.getFile().toPath()); } catch (Exception ignored) {}
        }
    }

    private OffsetDateTime getStoredModifiedAt(HxprDocument existing) {
        Map<String, Object> ingestProps = existing.getCinIngestProperties();
        if (ingestProps == null) {
            return null;
        }

        Object stored = ingestProps.get(P_ALF_MODIFIED_AT);
        if (stored == null) {
            return null;
        }

        try {
            return OffsetDateTime.parse(stored.toString());
        } catch (Exception e) {
            log.debug("Could not parse stored modifiedAt '{}' — will re-process", stored);
            return null;
        }
    }

    // ──────────────────────────────────────────────────────────────────────
    // Result DTO
    // ──────────────────────────────────────────────────────────────────────

    /**
     * Lightweight result from metadata ingestion.
     *
     * @param hxprDocId        Content Lake document identifier
     * @param nodeId           Alfresco node identifier
     * @param mimeType         source MIME type
     * @param documentName     node name
     * @param documentPath     repository path
     * @param skipped          {@code true} when the node was skipped (already current)
     * @param ingestProperties {@code cin_ingestProperties} snapshot from the metadata
     *                         phase; forwarded to {@link #processContent} so the status
     *                         patch does not need a prior GET
     */
    public record SyncResult(
            String hxprDocId,
            String nodeId,
            String mimeType,
            String documentName,
            String documentPath,
            boolean skipped,
            Map<String, Object> ingestProperties
    ) {}
}
