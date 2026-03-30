package org.hyland.contentlake.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.hyland.contentlake.client.HxprDocumentApi;
import org.hyland.contentlake.client.HxprService;
import org.hyland.contentlake.hxpr.api.model.ACE;
import org.hyland.contentlake.hxpr.api.model.Group;
import org.hyland.contentlake.hxpr.api.model.User;
import org.hyland.contentlake.model.Chunk;
import org.hyland.contentlake.model.ContentLakeIngestProperties;
import org.hyland.contentlake.model.ContentLakeNodeStatus;
import org.hyland.contentlake.model.HxprDocument;
import org.hyland.contentlake.model.HxprEmbedding;
import org.hyland.contentlake.service.chunking.SimpleChunkingService;
import org.hyland.contentlake.spi.ContentSourceClient;
import org.hyland.contentlake.spi.SourceNode;
import org.hyland.contentlake.spi.TextExtractor;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.web.client.HttpClientErrorException;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.OffsetDateTime;
import java.util.*;

/**
 * Shared synchronisation pipeline used by both the batch-ingester and the
 * live-ingester to process a single content node into the Content Lake.
 *
 * <p>This service is source-agnostic: it operates on {@link SourceNode} and
 * delegates to the {@link ContentSourceClient} and {@link TextExtractor} SPI
 * interfaces. All Alfresco-specific logic has been moved to the adapter layer.</p>
 *
 * <h3>Processing steps</h3>
 * <ol>
 *   <li>Receive the source-agnostic {@link SourceNode} with metadata + permissions</li>
 *   <li>Create or update the corresponding hxpr document (metadata phase)</li>
 *   <li>Extract plain text via the {@link TextExtractor}</li>
 *   <li>Chunk the text and generate embeddings via Spring AI</li>
 *   <li>Store embeddings and fulltext in the hxpr document</li>
 * </ol>
 *
 * <h3>Idempotency</h3>
 * Every write is guarded by a {@code modifiedAt} staleness check:
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
    private static final String P_SOURCE_MODIFIED_AT = ContentLakeIngestProperties.SOURCE_MODIFIED_AT;
    private static final String P_LEGACY_ALF_MODIFIED_AT = ContentLakeIngestProperties.ALFRESCO_MODIFIED_AT;
    private static final String P_CL_SYNC_STATUS    = ContentLakeIngestProperties.CONTENT_LAKE_SYNC_STATUS;
    private static final String P_CL_SYNC_ERROR     = ContentLakeIngestProperties.CONTENT_LAKE_SYNC_ERROR;

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
    private final ContentSourceClient sourceClient;
    private final HxprDocumentApi documentApi;
    private final HxprService hxprService;
    private final TextExtractor textExtractor;
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
     * @param node source-agnostic node (must include metadata, path, and read principals)
     * @return the hxpr document id, or {@code null} if skipped due to staleness
     */
    public String syncNode(SourceNode node) {
        String nodeId = node.nodeId();
        String sourceId = formatSourceId(node);

        HxprDocument existing = hxprService.findByNodeId(nodeId, sourceId);
        if (existing != null && isStale(existing, node)) {
            refreshPermissions(existing.getSysId(), node);
            log.debug("Skipping content for node {} — Content Lake version is already current; permissions refreshed", nodeId);
            return existing.getSysId();
        }

        HxprDocument doc = (existing != null)
                ? updateDocument(existing, node)
                : createDocument(node);

        try {
            processContent(doc.getSysId(), doc.getCinIngestProperties(),
                    nodeId, node.mimeType(), node.name(), node.path());
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
     * @param node source-agnostic node
     * @return sync result with hxpr document id and node metadata
     */
    public SyncResult ingestMetadata(SourceNode node) {
        String nodeId = node.nodeId();
        String sourceId = formatSourceId(node);

        HxprDocument existing = hxprService.findByNodeId(nodeId, sourceId);
        if (existing != null && isStale(existing, node)) {
            refreshPermissions(existing.getSysId(), node);
            log.debug("Skipping metadata for node {} — already current; permissions refreshed", nodeId);
            return new SyncResult(existing.getSysId(), nodeId,
                    node.mimeType(), node.name(), node.path(), true, null);
        }

        HxprDocument doc = (existing != null)
                ? updateDocument(existing, node)
                : createDocument(node);

        return new SyncResult(doc.getSysId(), nodeId,
                node.mimeType(), node.name(), node.path(),
                false, doc.getCinIngestProperties());
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
     * @param nodeId source-system node identifier
     * @return {@code true} if a record existed and was successfully deleted, {@code false} otherwise
     */
    public boolean deleteNode(String nodeId) {
        return deleteNode(nodeId, null);
    }

    /**
     * Deletes the Content Lake document for a given node when the delete event is
     * not older than the version already stored in the lake.
     *
     * @param nodeId    source-system node identifier
     * @param deletedAt timestamp associated with the delete/update-to-out-of-scope event
     * @return {@code true} if a record existed and was successfully deleted, {@code false} otherwise
     */
    public boolean deleteNode(String nodeId, OffsetDateTime deletedAt) {
        String sourceId = formatSourceId(sourceClient.getSourceType(), sourceClient.getSourceId());
        HxprDocument existing = hxprService.findByNodeId(nodeId, sourceId);
        if (existing == null) {
            log.debug("No Content Lake document found for deleted node {}", nodeId);
            return false;
        }

        OffsetDateTime storedModifiedAt = getStoredModifiedAt(existing);
        if (deletedAt != null && storedModifiedAt != null && storedModifiedAt.isAfter(deletedAt)) {
            log.info("Skipping delete for node {} — Content Lake document is newer than delete event", nodeId);
            return false;
        }

        try {
            documentApi.deleteById(existing.getSysId());
            log.info("Deleted Content Lake document {} for node {}", existing.getSysId(), nodeId);
            return true;
        } catch (Exception e) {
            log.error("Failed to delete Content Lake document for node {}: {}", nodeId, e.getMessage());
            return false;
        }
    }

    /**
     * Updates only the ACL on an existing Content Lake document.
     *
     * @param node source node carrying the updated read principals
     */
    public void updatePermissions(SourceNode node) {
        HxprDocument existing = hxprService.findByNodeId(node.nodeId(), formatSourceId(node));
        if (existing == null) {
            if (node.folder()) {
                log.debug("Skipping permission-only fallback for folder node {} with no Content Lake document",
                        node.nodeId());
                return;
            }

            HxprDocument created = createDocument(node);
            log.info("Created metadata-only Content Lake document {} during permission update for node {}",
                    created.getSysId(), node.nodeId());
            return;
        }

        refreshPermissions(existing.getSysId(), node);
    }

    private void refreshPermissions(String hxprDocId, SourceNode node) {
        String sourceId = node.sourceId();
        List<String> readerList = toSortedPrincipals(node.readPrincipals());
        List<String> denyList = toSortedPrincipals(node.denyPrincipals());
        List<ACE> sysAcl = buildSysAcl(readerList, sourceId);

        HxprDocument update = new HxprDocument();
        update.setSysAcl(sysAcl);
        update.setCinRead(readerList);
        update.setCinDeny(denyList);
        documentApi.updateById(hxprDocId, update);

        log.info("Updated ACL for Content Lake document {} (node {})", hxprDocId, node.nodeId());
    }

    // ──────────────────────────────────────────────────────────────────────
    // Staleness check
    // ──────────────────────────────────────────────────────────────────────

    private boolean isStale(HxprDocument existing, SourceNode incoming) {
        if (incoming.modifiedAt() == null) {
            return false;
        }

        OffsetDateTime storedDate = getStoredModifiedAt(existing);
        if (storedDate == null) {
            return false;
        }

        return !incoming.modifiedAt().isAfter(storedDate);
    }

    // ──────────────────────────────────────────────────────────────────────
    // Document CRUD helpers
    // ──────────────────────────────────────────────────────────────────────

    private HxprDocument createDocument(SourceNode node) {
        String pathRepoId = resolvePathRepositoryId(node.sourceId());
        String parentPath = buildContentLakeParentPath(node, pathRepoId);
        hxprService.ensureFolder(parentPath);

        HxprDocument doc = buildDocument(node);
        String documentPath = buildDocumentPath(parentPath, node);
        doc.setCinPaths(List.of(documentPath));

        HxprDocument existingAtPath = hxprService.findByPath(documentPath);
        if (existingAtPath != null) {
            log.info("Reusing existing hxpr document {} for node {} at {}",
                    existingAtPath.getSysId(), node.nodeId(), documentPath);
            return updateDocument(existingAtPath, node);
        }

        try {
            HxprDocument created = hxprService.createDocument(parentPath, doc);
            log.info("Created hxpr document {} for node {} at {}",
                    created.getSysId(), node.nodeId(), parentPath);
            return created;
        } catch (HttpClientErrorException.Conflict e) {
            HxprDocument conflicted = hxprService.findByPath(documentPath);
            if (conflicted != null) {
                log.warn("Recovered from create conflict by reusing hxpr document {} for node {} at {}",
                        conflicted.getSysId(), node.nodeId(), documentPath);
                return updateDocument(conflicted, node);
            }
            throw e;
        }
    }

    private HxprDocument updateDocument(HxprDocument existing, SourceNode node) {
        HxprDocument doc = buildDocument(node);
        doc.setSysId(existing.getSysId());
        doc.setSysMixinTypes(mergeMixinTypes(existing.getSysMixinTypes(), doc.getSysMixinTypes()));
        HxprDocument updated = documentApi.updateById(existing.getSysId(), doc);
        log.info("Updated hxpr document {} for node {}", updated.getSysId(), node.nodeId());
        return updated;
    }

    private HxprDocument buildDocument(SourceNode node) {
        HxprDocument doc = new HxprDocument();
        doc.setSysPrimaryType(SYS_FILE);
        doc.setSysName(resolveDocumentName(node));
        doc.setSysMixinTypes(List.of(MIXIN_CIN_REMOTE));

        doc.setCinId(node.nodeId());
        doc.setCinSourceId(formatSourceId(node));
        doc.setCinPaths(buildCinPaths(node));

        List<String> readerList = toSortedPrincipals(node.readPrincipals());
        List<String> denyList = toSortedPrincipals(node.denyPrincipals());
        doc.setCinRead(readerList);
        doc.setCinDeny(denyList);
        doc.setSysAcl(buildSysAcl(readerList, node.sourceId()));

        Map<String, Object> props = buildIngestProperties(node);
        doc.setCinIngestProperties(props);
        doc.setCinIngestPropertyNames(new ArrayList<>(props.keySet()));

        applySyncState(doc, ContentLakeNodeStatus.Status.PENDING, null);

        applyFlattenedSourceNodeFields(doc, node, readerList);
        return doc;
    }

    // ──────────────────────────────────────────────────────────────────────
    // ACL mapping
    // ──────────────────────────────────────────────────────────────────────

    private List<ACE> buildSysAcl(List<String> authorities, String sourceId) {
        List<ACE> acl = new ArrayList<>();
        String suffix = "_#_" + sourceId;

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

    private List<String> toSortedPrincipals(Set<String> principals) {
        if (principals == null || principals.isEmpty()) {
            return new ArrayList<>();
        }
        return principals.stream()
                .filter(Objects::nonNull)
                .map(String::trim)
                .filter(s -> !s.isBlank())
                .sorted()
                .toList();
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

    private String extractText(String nodeId, String mimeType, String documentName) {
        if (mimeType == null || mimeType.isBlank()) {
            log.info("Skipping content extraction for node {}: missing MIME type", nodeId);
            return null;
        }

        if (isTextMimeType(mimeType)) {
            byte[] content = sourceClient.getContent(nodeId);
            return new String(content, StandardCharsets.UTF_8);
        }

        if (textExtractor.supportsSourceReference(mimeType)) {
            return textExtractor.extractText(nodeId, mimeType);
        }

        if (!textExtractor.supports(mimeType)) {
            log.info("Skipping content extraction for node {}: unsupported extraction {} -> {}",
                    nodeId, mimeType, TARGET_MIME_TYPE);
            return null;
        }

        String tempFileName = resolveTempFileName(nodeId, documentName, mimeType);
        Resource temp = sourceClient.downloadContent(nodeId, tempFileName);
        try {
            return textExtractor.extractText(temp, mimeType);
        } finally {
            deleteTempFile(temp);
        }
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
            case FAILED  -> HxprDocument.SyncStatus.FAILED;
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

    private String buildContentLakeParentPath(SourceNode node, String repositoryId) {
        String base = buildRepositoryRootPath(repositoryId);
        if (node.path() == null || node.path().isBlank()) {
            return base;
        }
        String sourcePath = normalizeAbsolutePath(node.path());
        return "/".equals(base) ? sourcePath : base + sourcePath;
    }

    private String buildRepositoryRootPath(String repositoryId) {
        String targetPath = normalizeAbsolutePath(hxprTargetPath);
        if (repositoryId == null || repositoryId.isBlank()) return targetPath;
        String clean = repositoryId.startsWith("/") ? repositoryId.substring(1) : repositoryId;
        return joinPath(targetPath, clean);
    }

    private String resolvePathRepositoryId(String sourceId) {
        if (hxprPathRepositoryId != null && !hxprPathRepositoryId.isBlank()) {
            return hxprPathRepositoryId.trim();
        }
        return sourceId;
    }

    private Map<String, Object> buildIngestProperties(SourceNode node) {
        Map<String, Object> props = new LinkedHashMap<>(node.sourceProperties());
        props.values().removeIf(Objects::isNull);
        return props;
    }

    private String formatSourceId(SourceNode node) {
        return formatSourceId(node.sourceType(), node.sourceId());
    }

    private String formatSourceId(String sourceType, String sourceId) {
        if (sourceId == null || sourceId.isBlank()) {
            return sourceId;
        }
        if (sourceType == null || sourceType.isBlank() || sourceId.contains(":")) {
            return sourceId;
        }
        return sourceType + ":" + sourceId;
    }

    private void applyFlattenedSourceNodeFields(HxprDocument doc, SourceNode node, List<String> readerList) {
        doc.setAlfrescoNodeId(node.nodeId());
        doc.setAlfrescoRepositoryId(node.sourceId());
        doc.setAlfrescoName(node.name());
        doc.setAlfrescoPath(node.path());
        doc.setAlfrescoMimeType(node.mimeType());
        doc.setAlfrescoModifiedAt(node.modifiedAt() != null ? node.modifiedAt().toString() : null);
        doc.setAlfrescoReadAuthorities(readerList);
    }

    private List<String> buildCinPaths(SourceNode node) {
        String repoId = resolvePathRepositoryId(node.sourceId());
        String parentPath = buildContentLakeParentPath(node, repoId);
        return List.of(buildDocumentPath(parentPath, node));
    }

    private String buildDocumentPath(String parentPath, SourceNode node) {
        return joinPath(parentPath, resolveDocumentName(node));
    }

    private String resolveDocumentName(SourceNode node) {
        return (node.name() != null && !node.name().isBlank()) ? node.name() : node.nodeId();
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
        return (mimeType == null || mimeType.isBlank()) ? "unknown" : mimeType;
    }

    private List<String> mergeMixinTypes(List<String> existingMixins, List<String> desiredMixins) {
        LinkedHashSet<String> merged = new LinkedHashSet<>();
        if (existingMixins != null) merged.addAll(existingMixins);
        if (desiredMixins  != null) merged.addAll(desiredMixins);
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
            case "text/plain"      -> ".txt";
            case "text/html"       -> ".html";
            default                -> "";
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
        if (ingestProps == null) return null;

        Object stored = ingestProps.get(P_SOURCE_MODIFIED_AT);
        if (stored == null) {
            stored = ingestProps.get(P_LEGACY_ALF_MODIFIED_AT);
        }
        if (stored == null) return null;

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
     * @param nodeId           source-system node identifier
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
