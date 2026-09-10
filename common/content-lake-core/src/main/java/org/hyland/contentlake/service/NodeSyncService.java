package org.hyland.contentlake.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import com.fasterxml.jackson.databind.ObjectMapper;
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
import org.hyland.contentlake.model.SectionMap;
import org.hyland.contentlake.extractor.MarkdownToPlainText;
import org.hyland.contentlake.security.AclFilterBuilder;
import org.hyland.contentlake.service.chunking.SimpleChunkingService;
import org.hyland.contentlake.spi.ContentSourceClient;
import org.hyland.contentlake.spi.ExtractedText;
import org.hyland.contentlake.spi.SourceNode;
import org.hyland.contentlake.spi.SourceTombstone;
import org.hyland.contentlake.spi.TextExtractor;
import org.hyland.contentlake.spi.TextFormat;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.web.client.HttpClientErrorException;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.OffsetDateTime;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

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
 *
 * <h3>Content reuse</h3>
 * The staleness check stops a stale write from overwriting a newer one, but it cannot tell that
 * content is byte-identical, so a permission change, a property edit or a folder move all leave it
 * with a newer {@code modifiedAt} and re-run the whole pipeline. A content fingerprint recorded per
 * document closes that gap: when it still matches after extraction, chunking and embedding are
 * skipped and only the sync state is written. Extraction is still paid for, since the fingerprint is
 * taken over the extracted text rather than the binary.
 */
@Slf4j
@RequiredArgsConstructor
public class NodeSyncService {

    /* ---- hxpr type / mixin constants ---- */
    private static final String SYS_FILE         = "SysFile";
    private static final String MIXIN_CIN_REMOTE = HxprDocument.MIXIN_CIN_REMOTE;

    /* ---- cin_ingestProperties keys ---- */
    private static final String P_SOURCE_MODIFIED_AT = ContentLakeIngestProperties.SOURCE_MODIFIED_AT;
    private static final String P_LEGACY_ALF_MODIFIED_AT = ContentLakeIngestProperties.ALFRESCO_MODIFIED_AT;
    private static final String P_CL_SYNC_STATUS    = ContentLakeIngestProperties.CONTENT_LAKE_SYNC_STATUS;
    private static final String P_CL_SYNC_ERROR     = ContentLakeIngestProperties.CONTENT_LAKE_SYNC_ERROR;
    private static final String P_CL_EXTRACTED_TEXT = ContentLakeIngestProperties.CONTENT_LAKE_EXTRACTED_TEXT;
    private static final String P_CL_SECTION_MAP    = ContentLakeIngestProperties.CONTENT_LAKE_SECTION_MAP;
    private static final String P_CL_FINGERPRINT    = ContentLakeIngestProperties.CONTENT_LAKE_CONTENT_FINGERPRINT;

    /**
     * Ingest properties derived from the document's content rather than from its source metadata.
     *
     * <p>A metadata write rebuilds {@code cin_ingestProperties} from the source node's properties
     * alone, so without carrying these forward every property edit would drop the keyword-leg mirror
     * and the section map until content processing rewrote them. Carrying them means a content short
     * circuit can also write them back unchanged, which is what makes the short circuit safe whether
     * hxpr replaces or merges a map-valued field on update.</p>
     */
    private static final List<String> CONTENT_DERIVED_PROPERTY_KEYS =
            List.of(P_CL_EXTRACTED_TEXT, P_CL_SECTION_MAP, P_CL_FINGERPRINT);

    /**
     * Upper bound on the serialized section map stored per document. The map duplicates section text
     * to make small-to-big expansion a single property read; beyond this size the duplication is not
     * worth it, so the map is skipped and expansion falls back to the raw chunk for that document.
     */
    private static final int MAX_SECTION_MAP_CHARS = 96_000;

    private static final ObjectMapper SECTION_MAP_MAPPER = new ObjectMapper();

    /**
     * Cap on the extracted text copied into {@code cin_ingestProperties} for keyword search.
     *
     * <p>hxpr accepts much larger values, but this property travels in every document read and
     * every query response, so an uncapped copy of a large document is paid for on every hit. The
     * cap trades tail-of-document keyword recall for bounded documents; the vector leg is
     * unaffected because it indexes chunks rather than this field.</p>
     */
    private static final int MAX_EXTRACTED_TEXT_CHARS = 64_000;

    /* ---- ACL constants ---- */
    private static final String GROUP_PREFIX       = "GROUP_";
    private static final String PERMISSION_READ    = "Read";

    /* ---- text extraction helpers ---- */
    private static final String TARGET_MIME_TYPE = "text/plain";
    private static final String MARKDOWN_MIME_TYPE = "text/markdown";
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

    /**
     * When true, the document-context prefix is also prepended to the keyword-leg extracted-text
     * mirror (#66). Off by default and eval-gated: the vector leg is always enriched
     * ({@link EmbeddingService#embedChunks(List, String)}), but the keyword-leg enrichment changes
     * what {@code sys_fulltext} indexes, so it ships behind a flag until a sweep shows its delta.
     */
    private final boolean keywordContextEnrichmentEnabled;

    /**
     * When true (the default), a document whose recomputed content fingerprint matches the stored one
     * skips chunking and embedding (#120).
     *
     * <p>The kill switch exists because the failure mode is silent in the dangerous direction: a
     * fingerprint defect would stop changed content being re-embedded, and an operator needs one
     * setting to rule that out without a rebuild.</p>
     */
    private final boolean contentReuseEnabled;

    /** Short circuits versus full reprocesses, so the saving this service exists for is measurable. */
    private final ContentReuseCounters contentReuseCounters = new ContentReuseCounters();

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
            ExtractedText extracted = extractText(nodeId, mimeType, documentName);
            if (extracted == null || extracted.text() == null || extracted.text().isBlank()) {
                log.warn("Empty text for node {} ({})", nodeId, mimeType);
                String noTextError = String.format(ERR_NO_EXTRACTABLE_TEXT, safeMimeType(mimeType));
                patchSyncState(hxprDocId, baseIngestProps, ContentLakeNodeStatus.Status.FAILED, noTextError, nodeId);
                return;
            }

            Map<String, Object> ingestProps = baseIngestProps;

            String fingerprint = ContentFingerprint.of(extracted.text(), hxprService.getEmbeddingType(),
                    chunkingService.getConfig(), keywordContextEnrichmentEnabled);
            if (canReuseContent(baseIngestProps, fingerprint)) {
                reuseContent(hxprDocId, baseIngestProps, nodeId);
                return;
            }
            contentReuseCounters.reprocesses.incrementAndGet();

            // Chunking gets the extracted representation as-is: when it is markdown, segmentation
            // splits on heading and table boundaries and a table is kept atomic as ChunkType.TABLE.
            // The fulltext mirrors below get markup-stripped text instead, because they feed an
            // analysed keyword index where markdown punctuation is noise, not terms.
            String text = extracted.text();
            List<Chunk> chunks = chunkingService.chunk(text, nodeId, mimeType);
            if (chunks.isEmpty()) {
                log.warn("No chunks for node {}", nodeId);
                patchSyncState(hxprDocId, baseIngestProps, ContentLakeNodeStatus.Status.FAILED, ERR_NO_CHUNKS, nodeId);
                return;
            }

            String docContext = buildDocumentContext(documentName, documentPath);
            List<EmbeddingService.ChunkWithEmbedding> embedded =
                    embeddingService.embedChunks(chunks, docContext);

            // updateEmbeddings net-replaces any existing embedding child, so no separate
            // clear step is needed here (a redundant clear would just re-delete the same child).
            List<HxprEmbedding> hxprEmbeddings = toHxprEmbeddings(embedded);
            log.info("About to update embeddings for hxprDocId: {}, nodeId: {}, count: {}", hxprDocId, nodeId, hxprEmbeddings.size());
            hxprService.updateEmbeddings(hxprDocId, hxprEmbeddings);
            log.info("Successfully updated embeddings for hxprDocId: {}, nodeId: {}", hxprDocId, nodeId);

            String sectionMapJson = buildSectionMapJson(chunks);

            // Vector leg is always enriched (docContext above); the keyword leg is enriched only when
            // the flag is on (#66, eval-gated). When off, the extracted-text mirror stays the raw body.
            String keywordContext = keywordContextEnrichmentEnabled ? docContext : null;

            log.info("About to update fulltext and status for hxprDocId: {}, nodeId: {}", hxprDocId, nodeId);
            String keywordText = MarkdownToPlainText.toPlainText(extracted);
            updateFulltextWithStatus(hxprDocId, keywordText, keywordContext, sectionMapJson,
                    withFingerprint(ingestProps, fingerprint), nodeId);
            log.info("Successfully updated fulltext and status for hxprDocId: {}, nodeId: {}", hxprDocId, nodeId);

            log.info("Completed sync for node {}: {} embeddings", nodeId, hxprEmbeddings.size());
        } catch (Exception e) {
            patchSyncState(hxprDocId, baseIngestProps, ContentLakeNodeStatus.Status.FAILED, e.getMessage(), nodeId);
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
        return delete(new SourceTombstone(nodeId, deletedAt, SourceTombstone.Reason.DELETED))
                == DeleteOutcome.DELETED;
    }

    /** What a delete attempt did. Distinguishes the three cases a boolean return conflates. */
    public enum DeleteOutcome {
        /** The document existed and was removed. */
        DELETED,
        /** No document existed for the node; nothing to do. */
        NOT_FOUND,
        /** A document existed but is newer than the event, so the event is stale and was ignored. */
        SKIPPED_NEWER,
        /** A document existed but could not be removed. */
        FAILED
    }

    /**
     * Removes the Content Lake document for a tombstoned node: clears its embedding children, then
     * deletes the document itself.
     *
     * <p>The single delete implementation, so deletion is testable once rather than per source.
     * {@link #deleteNode(String, OffsetDateTime)} delegates here.</p>
     *
     * <p>Never throws: a caller sweeping many nodes must be able to record a per-node outcome and
     * continue.</p>
     *
     * @param tombstone the node to remove
     * @return what happened
     */
    public DeleteOutcome delete(SourceTombstone tombstone) {
        String nodeId = tombstone.nodeId();
        String sourceId = formatSourceId(sourceClient.getSourceType(), sourceClient.getSourceId());
        HxprDocument existing = hxprService.findByNodeId(nodeId, sourceId);
        if (existing == null) {
            log.debug("No Content Lake document found for deleted node {}", nodeId);
            return DeleteOutcome.NOT_FOUND;
        }

        OffsetDateTime deletedAt = tombstone.deletedAt();
        OffsetDateTime storedModifiedAt = getStoredModifiedAt(existing);
        if (deletedAt != null && storedModifiedAt != null && storedModifiedAt.isAfter(deletedAt)) {
            log.info("Skipping delete for node {}: Content Lake document is newer than delete event", nodeId);
            return DeleteOutcome.SKIPPED_NEWER;
        }

        String documentId = existing.getSysId();

        // Children first, and best effort. hxpr's cascade on document delete is not contractual, and
        // an embedding child left behind is still searchable because the read path substitutes the
        // '*' embedding-type wildcard -- the same phantom-result failure this delete exists to
        // prevent, one level down. A failed child clear must not stop the parent delete, because a
        // surviving parent is the worse of the two outcomes.
        try {
            hxprService.deleteEmbeddings(documentId);
        } catch (Exception e) {
            log.warn("Failed to clear embeddings for document {} before deleting it: {}",
                    documentId, e.getMessage());
        }

        try {
            documentApi.deleteById(documentId);
            log.info("Deleted Content Lake document {} for node {} ({})",
                    documentId, nodeId, tombstone.reason());
            return DeleteOutcome.DELETED;
        } catch (Exception e) {
            log.error("Failed to delete Content Lake document for node {}: {}", nodeId, e.getMessage());
            return DeleteOutcome.FAILED;
        }
    }

    /**
     * Maps a source path to the {@code cin_paths} prefix under which documents from it are indexed.
     *
     * <p>Reconciliation scope predicates need this because {@code cin_paths} holds the hxpr path
     * ({@code <hxprTargetPath>/<repositoryId><sourcePath>}), not the raw source path. A predicate
     * written against an Alfresco or Nuxeo root path directly matches nothing, deletes nothing, and
     * looks like a working feature.</p>
     *
     * @param sourceId   raw source id, not the {@code type:id} form
     * @param sourcePath source-system folder path, or {@code null}/blank for the repository root
     * @return the {@code cin_paths} prefix, always absolute and without a trailing slash
     */
    public String contentLakePathPrefix(String sourceId, String sourcePath) {
        String base = buildRepositoryRootPath(resolvePathRepositoryId(sourceId));
        if (sourcePath == null || sourcePath.isBlank()) {
            return base;
        }
        String normalized = normalizeAbsolutePath(sourcePath);
        return "/".equals(base) ? normalized : base + normalized;
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
        carryForwardContentProperties(existing, doc);
        HxprDocument updated = documentApi.updateById(existing.getSysId(), doc);
        // The properties written are the ones just built, not whatever the response happened to echo:
        // the caller compares the content fingerprint against this map, and a response that omitted
        // cin_ingestProperties would read as "no fingerprint stored" and reprocess every document.
        updated.setCinIngestProperties(doc.getCinIngestProperties());
        updated.setCinIngestPropertyNames(doc.getCinIngestPropertyNames());
        log.info("Updated hxpr document {} for node {}", updated.getSysId(), node.nodeId());
        return updated;
    }

    /**
     * Copies the content-derived ingest properties from the stored document onto an update built from
     * source metadata, together with the sync status they belong to.
     *
     * <p>{@link #buildIngestProperties} builds the map from the source node's own properties, so
     * without this an update would drop the extracted-text mirror, the section map and the content
     * fingerprint on every property edit. Dropping the fingerprint would defeat the short circuit it
     * exists for, and dropping the mirror would silently remove the document from keyword matching
     * until content processing rewrote it.</p>
     */
    private void carryForwardContentProperties(HxprDocument existing, HxprDocument doc) {
        Map<String, Object> previous = existing.getCinIngestProperties();
        if (previous == null || previous.isEmpty()) {
            return;
        }

        Map<String, Object> props = doc.getCinIngestProperties() != null
                ? new LinkedHashMap<>(doc.getCinIngestProperties())
                : new LinkedHashMap<>();

        for (String key : CONTENT_DERIVED_PROPERTY_KEYS) {
            Object value = previous.get(key);
            if (value != null) {
                props.put(key, value);
            }
        }

        doc.setCinIngestProperties(props);
        doc.setCinIngestPropertyNames(new ArrayList<>(props.keySet()));
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

    /**
     * The write side of the ACL. It takes its principal encoding from {@link AclFilterBuilder}, which
     * owns the read side, so a change to the namespacing cannot land on one side only.
     */
    private List<ACE> buildSysAcl(List<String> authorities, String sourceId) {
        List<ACE> acl = new ArrayList<>();

        for (String authority : authorities) {
            if (AclFilterBuilder.EVERYONE_AUTHORITY.equals(authority)) {
                acl.add(buildUserAce(AclFilterBuilder.EVERYONE_PRINCIPAL));
            } else if (authority.startsWith(GROUP_PREFIX)) {
                acl.add(buildGroupAce(AclFilterBuilder.namespace(authority, sourceId)));
            } else {
                acl.add(buildUserAce(AclFilterBuilder.namespace(authority, sourceId)));
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

    private ExtractedText extractText(String nodeId, String mimeType, String documentName) {
        if (mimeType == null || mimeType.isBlank()) {
            log.info("Skipping content extraction for node {}: missing MIME type", nodeId);
            return null;
        }

        if (isTextMimeType(mimeType)) {
            // Text arrives as-is; no extractor is involved. Markdown source content therefore
            // reaches chunking with its headings and table rows intact, so it is reported as such
            // rather than as PLAIN, which would leave the fulltext mirrors holding raw markup.
            byte[] content = sourceClient.getContent(nodeId);
            String text = new String(content, StandardCharsets.UTF_8);
            return ExtractedText.of(text, formatOfTextMimeType(mimeType));
        }

        if (textExtractor.supportsSourceReference(mimeType)) {
            return textExtractor.extract(nodeId, mimeType);
        }

        if (!textExtractor.supports(mimeType)) {
            log.info("Skipping content extraction for node {}: unsupported extraction {} -> {}",
                    nodeId, mimeType, TARGET_MIME_TYPE);
            return null;
        }

        String tempFileName = resolveTempFileName(nodeId, documentName, mimeType);
        Resource temp = sourceClient.downloadContent(nodeId, tempFileName);
        try {
            return textExtractor.extract(temp, mimeType);
        } finally {
            deleteTempFile(temp);
        }
    }

    /** Markdown source content is markdown; every other text type is flattened prose. */
    private static TextFormat formatOfTextMimeType(String mimeType) {
        String normalized = mimeType.toLowerCase();
        return (normalized.startsWith(MARKDOWN_MIME_TYPE) || normalized.startsWith("text/x-markdown"))
                ? TextFormat.MARKDOWN
                : TextFormat.PLAIN;
    }

    // ──────────────────────────────────────────────────────────────────────
    // Content reuse (#120)
    // ──────────────────────────────────────────────────────────────────────

    /**
     * Whether reprocessing this document would produce exactly what is already stored.
     *
     * <p>Three conditions, all required:</p>
     * <ol>
     *   <li>the feature is enabled;</li>
     *   <li>the stored fingerprint equals the recomputed one, so the text, the embedding type and the
     *       chunking parameters are all unchanged;</li>
     *   <li>the extracted-text mirror is present.</li>
     * </ol>
     *
     * <p>The third condition is what stops a document from becoming permanently unrepairable behind a
     * fingerprint match. {@code processContent} writes the mirror and the fingerprint together in its
     * final step, after the embeddings have been stored, so the pair is evidence that a content pass
     * ran to completion. A fingerprint without a mirror is not, and a document indexed with no
     * embeddings at all is invisible to search while looking finished to monitoring, which is exactly
     * the state that must not be made permanent. The bias is deliberately towards doing the work.</p>
     *
     * <p>The section map is not required: it is skipped by design for documents whose serialized map
     * would exceed its size cap.</p>
     */
    private boolean canReuseContent(Map<String, Object> baseIngestProps, String fingerprint) {
        if (!contentReuseEnabled || baseIngestProps == null) {
            return false;
        }
        if (!ContentFingerprint.matches(asText(baseIngestProps.get(P_CL_FINGERPRINT)), fingerprint)) {
            return false;
        }
        return asText(baseIngestProps.get(P_CL_EXTRACTED_TEXT)) != null;
    }

    /**
     * Completes a sync whose content is unchanged: flips the document to {@code INDEXED} and reports
     * the same status back to the source, without chunking or embedding.
     *
     * <p>The properties written are the ones the metadata phase already carried forward, so the
     * mirror, the section map and the fingerprint survive whether hxpr replaces or merges a
     * map-valued field. {@code source_modifiedAt} was advanced by that same metadata write, which is
     * what makes a metadata-only change cost nothing beyond extraction.</p>
     *
     * <p>Embedding children are deliberately untouched. {@code updateEmbeddings} net-replaces them
     * and is precisely what is being skipped, so the existing child stays as the current one.</p>
     */
    private void reuseContent(String hxprDocId, Map<String, Object> baseIngestProps, String nodeId) {
        contentReuseCounters.shortCircuits.incrementAndGet();
        ContentReuseStats stats = contentReuseCounters.snapshot();

        Map<String, Object> props = buildStatusedProps(baseIngestProps, ContentLakeNodeStatus.Status.INDEXED, null);
        HxprDocument update = new HxprDocument();
        update.setSyncStatus(HxprDocument.SyncStatus.INDEXED);
        update.setSyncError(null);
        update.setCinIngestProperties(props);
        update.setCinIngestPropertyNames(new ArrayList<>(props.keySet()));
        documentApi.updateById(hxprDocId, update);
        sourceClient.writeSyncStatus(nodeId, ContentLakeNodeStatus.Status.INDEXED.name(), null);

        log.info("Content unchanged for node {}: skipped chunking and embedding "
                        + "(shortCircuits={}, reprocesses={})",
                nodeId, stats.shortCircuits(), stats.reprocesses());
    }

    /** Returns {@code props} with the fingerprint of the content just processed added. */
    private Map<String, Object> withFingerprint(Map<String, Object> props, String fingerprint) {
        Map<String, Object> withFingerprint = props != null
                ? new LinkedHashMap<>(props)
                : new LinkedHashMap<>();
        withFingerprint.put(P_CL_FINGERPRINT, fingerprint);
        return withFingerprint;
    }

    /** A property value as non-blank text, or {@code null} when it is absent or blank. */
    private static String asText(Object value) {
        if (value == null) {
            return null;
        }
        String text = value.toString();
        return text.isBlank() ? null : text;
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

    /**
     * Writes both fulltext mirrors and flips the document to INDEXED.
     *
     * @param text markup-stripped text. Both mirrors feed keyword matching, so callers pass plain
     *             text even when the chunks hold markdown.
     */
    private void updateFulltextWithStatus(String hxprDocId, String text, String docContext,
                                          String sectionMapJson, Map<String, Object> baseIngestProps,
                                          String nodeId) {
        log.info("updateFulltextWithStatus called for hxprDocId: {}, textLength: {}, baseIngestProps: {}",
                hxprDocId, text != null ? text.length() : 0, baseIngestProps);
        Map<String, Object> props = buildStatusedProps(baseIngestProps, ContentLakeNodeStatus.Status.INDEXED, null);
        // The keyword leg of hybrid search cannot read sys_fulltextBinary: hxpr does not expose that
        // field to HXQL, so a query against it matches nothing. Mirroring the text into an ingest
        // property is what makes term matching work at all, and hxpr also folds ingest properties
        // into sys_fulltext.
        putExtractedText(props, text, docContext);
        putSectionMap(props, sectionMapJson);
        // Logged by key rather than by value: the extracted text is now one of the values.
        log.info("Built statused props for hxprDocId: {}, propertyNames: {}", hxprDocId, props.keySet());
        HxprDocument update = new HxprDocument();
        update.setSysFulltextBinary(text);
        update.setSyncStatus(HxprDocument.SyncStatus.INDEXED);
        update.setSyncError(null);
        update.setCinIngestProperties(props);
        update.setCinIngestPropertyNames(new ArrayList<>(props.keySet()));
        log.info("About to call documentApi.updateById for hxprDocId: {}", hxprDocId);
        documentApi.updateById(hxprDocId, update);
        log.info("Successfully called documentApi.updateById for hxprDocId: {}", hxprDocId);
        sourceClient.writeSyncStatus(nodeId, ContentLakeNodeStatus.Status.INDEXED.name(), null);
    }

    private void patchSyncState(String hxprDocId, Map<String, Object> baseIngestProps,
                                ContentLakeNodeStatus.Status status, String error, String nodeId) {
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
        sourceClient.writeSyncStatus(nodeId, status.name(), error);
    }

    /**
     * Copies extracted text into the ingest properties for the keyword leg, truncated to
     * {@link #MAX_EXTRACTED_TEXT_CHARS}.
     *
     * <p>The document-context prefix ({@code Document: <name> | Path: <path>}) is prepended so the
     * keyword leg gets the same contextual-retrieval disambiguation the vector leg already gets from
     * {@link EmbeddingService#embedChunks(List, String)}: acronym- and term-heavy queries that lean
     * on the keyword leg can then match the document name/path, not just the raw body. This is
     * additive - the entire extracted body still follows the prefix - and the per-chunk text stored
     * in embeddings is left untouched.</p>
     *
     * <p>Blank text removes the key rather than storing an empty string, so a document whose
     * extraction produced nothing does not advertise a searchable-but-empty body.</p>
     *
     * <p>Stored as extracted, not case-folded: hxpr's {@code sys_fulltext} index analyses this text
     * and matches it case-insensitively, so folding it here would lose information for nothing.</p>
     */
    private void putExtractedText(Map<String, Object> props, String text, String docContext) {
        if (text == null || text.isBlank()) {
            props.remove(P_CL_EXTRACTED_TEXT);
            return;
        }
        String enriched = (docContext != null && !docContext.isBlank())
                ? docContext + "\n\n" + text
                : text;
        String value = enriched.length() > MAX_EXTRACTED_TEXT_CHARS
                ? enriched.substring(0, MAX_EXTRACTED_TEXT_CHARS)
                : enriched;
        if (value.length() < enriched.length()) {
            log.debug("Truncated extracted text for keyword search from {} to {} chars",
                    enriched.length(), value.length());
        }
        props.put(P_CL_EXTRACTED_TEXT, value);
    }

    /**
     * Builds the JSON section map for small-to-big retrieval from the produced chunks, or returns
     * {@code null} when there is nothing to store or the map would exceed
     * {@link #MAX_SECTION_MAP_CHARS}. Section text is the concatenation of that section's chunk text,
     * so a matched chunk can be expanded to its parent section by a single property read.
     */
    private String buildSectionMapJson(List<Chunk> chunks) {
        if (chunks == null || chunks.isEmpty()) {
            return null;
        }

        List<Integer> chunkSections = new ArrayList<>(chunks.size());
        // Preserve first-seen section order; a section is homogeneous in type (tables are their own).
        LinkedHashMap<Integer, StringBuilder> sectionText = new LinkedHashMap<>();
        LinkedHashMap<Integer, String> sectionType = new LinkedHashMap<>();

        for (Chunk chunk : chunks) {
            int section = chunk.getSectionIndex();
            chunkSections.add(section);
            sectionText.computeIfAbsent(section, k -> new StringBuilder());
            StringBuilder sb = sectionText.get(section);
            if (sb.length() > 0) {
                sb.append('\n');
            }
            sb.append(chunk.getText() != null ? chunk.getText() : "");
            sectionType.putIfAbsent(section,
                    chunk.getChunkType() != null ? chunk.getChunkType().name() : "PROSE");
        }

        List<SectionMap.Section> sections = new ArrayList<>(sectionText.size());
        for (Map.Entry<Integer, StringBuilder> entry : sectionText.entrySet()) {
            sections.add(new SectionMap.Section(
                    entry.getKey(),
                    sectionType.getOrDefault(entry.getKey(), "PROSE"),
                    entry.getValue().toString()));
        }

        try {
            String json = SECTION_MAP_MAPPER.writeValueAsString(new SectionMap(chunkSections, sections));
            if (json.length() > MAX_SECTION_MAP_CHARS) {
                log.debug("Section map for small-to-big retrieval skipped: {} chars exceeds cap {}",
                        json.length(), MAX_SECTION_MAP_CHARS);
                return null;
            }
            return json;
        } catch (Exception e) {
            log.warn("Failed to serialize section map: {}", e.getMessage());
            return null;
        }
    }

    private void putSectionMap(Map<String, Object> props, String sectionMapJson) {
        if (sectionMapJson == null || sectionMapJson.isBlank()) {
            props.remove(P_CL_SECTION_MAP);
            return;
        }
        props.put(P_CL_SECTION_MAP, sectionMapJson);
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

    /**
     * Deletes a downloaded temp file, and only a temp file.
     *
     * <p>Restricted to the system temp directory on purpose. A {@code ContentSourceClient} whose
     * backing store is the local filesystem can return the source file itself from
     * {@code downloadContent}, and deleting that would destroy the document just indexed. The SPI
     * contract says the download is the caller's to delete, so this stays defence in depth rather than
     * the primary guarantee: a source client must still return a copy.</p>
     */
    private void deleteTempFile(Resource resource) {
        if (!(resource instanceof FileSystemResource fsr)) {
            return;
        }
        try {
            Path path = fsr.getFile().toPath().toAbsolutePath().normalize();
            Path tempDir = Path.of(System.getProperty("java.io.tmpdir")).toAbsolutePath().normalize();
            if (!path.startsWith(tempDir)) {
                log.warn("Refusing to delete {} after extraction: it is outside {} and so is not a "
                        + "temp file. The source client must return a copy, not the source file.",
                        path, tempDir);
                return;
            }
            Files.deleteIfExists(path);
        } catch (Exception ignored) {
            // A temp file that cannot be deleted is a housekeeping problem, not an ingest failure.
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
    // Content-reuse counters
    // ──────────────────────────────────────────────────────────────────────

    /** Snapshot of how much reprocessing the content fingerprint has avoided. */
    public ContentReuseStats getContentReuseStats() {
        return contentReuseCounters.snapshot();
    }

    /**
     * How often content processing short-circuited on an unchanged fingerprint versus ran in full.
     *
     * @param shortCircuits documents whose chunking and embedding were skipped
     * @param reprocesses   documents that were chunked and embedded
     */
    public record ContentReuseStats(long shortCircuits, long reprocesses) {
    }

    /**
     * Plain atomics rather than Micrometer: {@code content-lake-core} does not depend on
     * {@code micrometer-core}, and adding it to five ingester apps plus the RAG service to publish
     * two numbers is not a trade worth making. Each short circuit also logs its running totals.
     */
    private static final class ContentReuseCounters {

        private final AtomicLong shortCircuits = new AtomicLong();
        private final AtomicLong reprocesses = new AtomicLong();

        ContentReuseStats snapshot() {
            return new ContentReuseStats(shortCircuits.get(), reprocesses.get());
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
