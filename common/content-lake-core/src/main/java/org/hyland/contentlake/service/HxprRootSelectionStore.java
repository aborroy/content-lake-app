package org.hyland.contentlake.service;

import lombok.extern.slf4j.Slf4j;
import org.hyland.contentlake.client.HxprDocumentApi;
import org.hyland.contentlake.client.HxprService;
import org.hyland.contentlake.model.HxprDocument;

import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Root selections as a state document per source in the index.
 *
 * <p>The default, for the same reason the cursor store's hxpr backing is: both container volumes on the
 * ingester are read-only apart from one small state mount, so a store needing nothing the deployment does not
 * already have is the one that works everywhere.</p>
 *
 * <h3>Why this cannot be swept away</h3>
 * <p>It is built through {@link HxprStateDocuments}, which omits {@code cin_sourceId}, {@code cin_paths} and
 * {@code cin_id} so a reconciliation sweep cannot reach it. Those omissions are load-bearing and have tests of
 * their own; see that class for what each one prevents. This store shares them rather than restating them,
 * because a second copy is how one of them quietly stops being true.</p>
 *
 * <h3>A different folder and a different name prefix from the cursor store</h3>
 * <p>Both are keyed by the qualified source id, so a shared folder and prefix would put one source's cursor
 * and its selection at the same path, and whichever wrote last would win. The prefix alone would be enough;
 * the separate folder also means an operator listing selections is not reading cursors.</p>
 *
 * <p>Note {@code make clean} wipes the index, so it wipes selections with it. That is correct: a selection is
 * the scope of an index that no longer exists. The choice does survive a container restart, which is the case
 * that distinguishes this from the startup configuration it replaces.</p>
 */
@Slf4j
public class HxprRootSelectionStore implements RootSelectionStore {

    /** The state document's own property keys. They describe host state, not anything a source ingested. */
    static final String PROP_SOURCE = "selection_sourceId";
    static final String PROP_ROOTS = "selection_rootNodeIds";
    static final String PROP_UPDATED_AT = "selection_updatedAt";
    static final String PROP_UPDATED_BY = "selection_updatedBy";

    /**
     * Node ids are joined into one property rather than stored as a list.
     *
     * <p>A newline cannot appear in a node id in any source this host talks to, and a single string avoids
     * depending on how hxpr round-trips a multi-valued ingest property, which is not something this store
     * should be the first to discover.</p>
     */
    private static final String ROOT_SEPARATOR = "\n";

    private final HxprService hxprService;
    private final HxprDocumentApi documentApi;
    private final String folderPath;

    public HxprRootSelectionStore(HxprService hxprService, HxprDocumentApi documentApi, String folderPath) {
        this.hxprService = hxprService;
        this.documentApi = documentApi;
        this.folderPath = HxprStateDocuments.normalizeFolder(folderPath, "root selection");
    }

    @Override
    public Optional<RootSelection> load(String qualifiedSourceId) {
        HxprDocument document = hxprService.findByPath(documentPath(qualifiedSourceId));
        if (document == null) {
            return Optional.empty();
        }
        Map<String, Object> props = document.getCinIngestProperties();
        if (props == null) {
            return Optional.empty();
        }

        // The document name is a lossy transform of the source id, so two ids differing only in a character a
        // name cannot carry resolve to one path. Reading one source's scope as another's would walk the wrong
        // subtree, so the stored id has to agree before the value is used.
        String storedSource = asString(props.get(PROP_SOURCE));
        if (storedSource != null && !storedSource.equals(qualifiedSourceId)) {
            log.warn("Root selection document at {} belongs to source {}, not {}; ignoring it",
                    documentPath(qualifiedSourceId), storedSource, qualifiedSourceId);
            return Optional.empty();
        }

        // A present document with no roots is "chosen, and nothing", which is not the same as no document at
        // all. Returning empty here would fall through to configured roots and re-ingest the whole source.
        return Optional.of(new RootSelection(
                parseRoots(asString(props.get(PROP_ROOTS))),
                asTimestamp(props.get(PROP_UPDATED_AT)),
                asString(props.get(PROP_UPDATED_BY))));
    }

    @Override
    public void save(String qualifiedSourceId, RootSelection selection) {
        String path = documentPath(qualifiedSourceId);
        Map<String, Object> props = new LinkedHashMap<>();
        props.put(PROP_SOURCE, qualifiedSourceId);
        props.put(PROP_ROOTS, String.join(ROOT_SEPARATOR, selection.rootNodeIds()));
        props.put(PROP_UPDATED_AT, selection.updatedAt() == null ? null : selection.updatedAt().toString());
        props.put(PROP_UPDATED_BY, selection.updatedBy());

        HxprDocument existing = hxprService.findByPath(path);
        if (existing != null && existing.getSysId() != null) {
            documentApi.updateById(existing.getSysId(),
                    HxprStateDocuments.stateDocument(null, props));
            log.info("Root selection for {} is now {} root(s)", qualifiedSourceId,
                    selection.rootNodeIds().size());
            return;
        }

        hxprService.ensureFolder(folderPath);
        hxprService.createDocument(folderPath,
                HxprStateDocuments.stateDocument(documentName(qualifiedSourceId), props));
        log.info("Stored the first root selection for {} at {}: {} root(s)", qualifiedSourceId, path,
                selection.rootNodeIds().size());
    }

    @Override
    public void clear(String qualifiedSourceId) {
        HxprDocument existing = hxprService.findByPath(documentPath(qualifiedSourceId));
        if (existing == null || existing.getSysId() == null) {
            return;
        }
        documentApi.deleteById(existing.getSysId());
        log.info("Cleared the root selection for {}; the next pass falls back to configured roots",
                qualifiedSourceId);
    }

    /** The path a source's selection document lives at, so an operator can find it. */
    public String documentPath(String qualifiedSourceId) {
        return folderPath + "/" + documentName(qualifiedSourceId);
    }

    static String documentName(String qualifiedSourceId) {
        return HxprStateDocuments.documentName("roots-", qualifiedSourceId);
    }

    private static List<String> parseRoots(String joined) {
        if (joined == null || joined.isBlank()) {
            return List.of();
        }
        return Arrays.stream(joined.split(ROOT_SEPARATOR))
                .map(String::trim)
                .filter(root -> !root.isEmpty())
                .toList();
    }

    private static String asString(Object value) {
        return value == null ? null : String.valueOf(value);
    }

    /** An unparseable timestamp costs the audit detail, not the selection, so it is a warning not a failure. */
    private static OffsetDateTime asTimestamp(Object value) {
        String text = asString(value);
        if (text == null || text.isBlank()) {
            return null;
        }
        try {
            return OffsetDateTime.parse(text);
        } catch (DateTimeParseException e) {
            log.warn("Root selection timestamp '{}' is not a valid ISO-8601 instant; ignoring it", text);
            return null;
        }
    }
}
