package org.hyland.contentlake.service;

import lombok.extern.slf4j.Slf4j;
import org.hyland.contentlake.client.HxprDocumentApi;
import org.hyland.contentlake.client.HxprService;
import org.hyland.contentlake.model.HxprDocument;

import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Keeps cursors in hxpr, as one state document per source.
 *
 * <p>The default store, because it needs nothing the deployment does not already have: the connector
 * host's volumes are mounted read-only, so a file-backed cursor has nowhere to live, while hxpr write
 * access is a precondition for ingesting at all.</p>
 *
 * <h3>Why a state document cannot be swept away</h3>
 * <p>A cursor stored in the same index as the documents a reconciliation sweep deletes is only safe
 * because the sweep cannot see it. Three omissions do that, and each is load-bearing with nothing in
 * the compiler to enforce it, so each has a test of its own:</p>
 * <ul>
 *   <li>no {@code cin_sourceId}, so {@code HxprService.forEachDocumentOfSource} does not visit it and
 *       the sweep never considers deleting the very document that tells it where it is;</li>
 *   <li>no {@code cin_paths}, so {@code IndexReconciliationService.underAnyPath} cannot match it even if
 *       some future scan did reach it;</li>
 *   <li>no {@code cin_id}, so it cannot be mistaken for a source node by a lookup that does not qualify
 *       its query with a source.</li>
 * </ul>
 * <p>It is also invisible to search: both retrieval legs read chunks from the embeddings index, and a
 * state document has no {@code _e_*} embedding children, so no query can return it as a hit.</p>
 *
 * <p>The document does carry the {@code CinRemote} mixin, which hxpr requires of anything writing a
 * {@code cin_*} field, and the cursor itself lives in {@code cin_ingestProperties}.</p>
 */
@Slf4j
public class HxprSyncCursorStore implements SyncCursorStore {

    /**
     * The state document's own property keys, kept here rather than in the shared ingest-property
     * constants: they describe a host state document, not anything a source ingested.
     */
    static final String PROP_SOURCE = "cursor_sourceId";
    static final String PROP_VALUE = "cursor_value";
    static final String PROP_UPDATED_AT = "cursor_updatedAt";
    static final String PROP_GENERATION = "cursor_generation";

    private final HxprService hxprService;
    private final HxprDocumentApi documentApi;
    private final String folderPath;

    public HxprSyncCursorStore(HxprService hxprService, HxprDocumentApi documentApi, String folderPath) {
        this.hxprService = hxprService;
        this.documentApi = documentApi;
        this.folderPath = normalizeFolder(folderPath);
    }

    @Override
    public Optional<SyncCursor> load(String qualifiedSourceId) {
        HxprDocument document = hxprService.findByPath(documentPath(qualifiedSourceId));
        if (document == null) {
            return Optional.empty();
        }
        Map<String, Object> props = document.getCinIngestProperties();
        if (props == null) {
            return Optional.empty();
        }

        // Two source ids that differ only in a character the document name cannot carry would resolve to
        // the same path. Resuming one source from another's cursor would skip a window silently, so the
        // stored id has to agree before the value is used.
        String storedSource = asString(props.get(PROP_SOURCE));
        if (storedSource != null && !storedSource.equals(qualifiedSourceId)) {
            log.warn("Cursor document at {} belongs to source {}, not {}; ignoring it and syncing in full",
                    documentPath(qualifiedSourceId), storedSource, qualifiedSourceId);
            return Optional.empty();
        }

        String value = asString(props.get(PROP_VALUE));
        if (value == null || value.isBlank()) {
            return Optional.empty();
        }
        return Optional.of(new SyncCursor(value,
                asTimestamp(props.get(PROP_UPDATED_AT)),
                asLong(props.get(PROP_GENERATION))));
    }

    @Override
    public void save(String qualifiedSourceId, SyncCursor cursor) {
        String path = documentPath(qualifiedSourceId);
        Map<String, Object> props = new LinkedHashMap<>();
        props.put(PROP_SOURCE, qualifiedSourceId);
        props.put(PROP_VALUE, cursor.value());
        props.put(PROP_UPDATED_AT, cursor.updatedAt() == null ? null : cursor.updatedAt().toString());
        props.put(PROP_GENERATION, cursor.generation());

        HxprDocument existing = hxprService.findByPath(path);
        if (existing != null && existing.getSysId() != null) {
            documentApi.updateById(existing.getSysId(), stateDocument(null, props));
            log.debug("Advanced cursor for {} to generation {}", qualifiedSourceId, cursor.generation());
            return;
        }

        hxprService.ensureFolder(folderPath);
        hxprService.createDocument(folderPath, stateDocument(documentName(qualifiedSourceId), props));
        log.debug("Stored the first cursor for {} at {}", qualifiedSourceId, path);
    }

    @Override
    public void clear(String qualifiedSourceId) {
        HxprDocument existing = hxprService.findByPath(documentPath(qualifiedSourceId));
        if (existing == null || existing.getSysId() == null) {
            return;
        }
        documentApi.deleteById(existing.getSysId());
        log.info("Cleared the cursor for {}; the next pass walks the source", qualifiedSourceId);
    }

    /** The path a source's state document lives at, so an operator can find it. */
    public String documentPath(String qualifiedSourceId) {
        return folderPath + "/" + documentName(qualifiedSourceId);
    }

    /**
     * The state document as hxpr receives it: a name and the cursor properties, and deliberately none of
     * the fields that would make it visible to a sweep, a source lookup or a search.
     *
     * <p>Shared with every other host state store through {@link HxprStateDocuments}, so the three omissions
     * that keep such a document out of a sweep's reach have one implementation rather than one per store.</p>
     */
    private HxprDocument stateDocument(String sysName, Map<String, Object> props) {
        return HxprStateDocuments.stateDocument(sysName, props);
    }

    /**
     * A document name derived from the qualified source id. The colon and anything else outside a plain
     * name is replaced, because the name becomes a path segment; {@link #PROP_SOURCE} carries the exact
     * id so the substitution cannot be mistaken for the source's own identifier.
     */
    static String documentName(String qualifiedSourceId) {
        return HxprStateDocuments.documentName("cursor-", qualifiedSourceId);
    }

    private static String normalizeFolder(String path) {
        return HxprStateDocuments.normalizeFolder(path, "cursor");
    }

    private static String asString(Object value) {
        return value == null ? null : String.valueOf(value);
    }

    private static long asLong(Object value) {
        if (value instanceof Number number) {
            return number.longValue();
        }
        try {
            return value == null ? 0L : Long.parseLong(String.valueOf(value).trim());
        } catch (NumberFormatException e) {
            return 0L;
        }
    }

    /**
     * A timestamp that will not parse costs nothing: it is diagnostic only, so a null keeps the cursor
     * usable rather than forcing a full walk over a display field.
     */
    private static OffsetDateTime asTimestamp(Object value) {
        String text = asString(value);
        if (text == null || text.isBlank()) {
            return null;
        }
        try {
            return OffsetDateTime.parse(text);
        } catch (DateTimeParseException e) {
            log.warn("Cursor timestamp '{}' is not an ISO-8601 instant; treating it as unknown", text);
            return null;
        }
    }
}
