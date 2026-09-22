package org.hyland.contentlake.service;

import org.hyland.contentlake.model.HxprDocument;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Builds the host's own state documents, the ones a reconciliation sweep must never delete.
 *
 * <h3>Why this is shared rather than copied</h3>
 * <p>A state document stored in the same index as the documents a sweep deletes is only safe because the
 * sweep cannot see it. Three omissions do that, none of them enforced by the compiler:</p>
 * <ul>
 *   <li>no {@code cin_sourceId}, so {@code HxprService.forEachDocumentOfSource} does not visit it and the
 *       sweep never considers deleting the very document that tells it where it is;</li>
 *   <li>no {@code cin_paths}, so {@code IndexReconciliationService.underAnyPath} cannot match it even if some
 *       future scan did reach it;</li>
 *   <li>no {@code cin_id}, so it cannot be mistaken for a source node by a lookup that does not qualify its
 *       query with a source.</li>
 * </ul>
 *
 * <p>It is also invisible to search: both retrieval legs read chunks from the embeddings index, and a state
 * document has no {@code _e_*} embedding children, so no query can return it as a hit.</p>
 *
 * <p>Every one of those is an omission, which is the kind of invariant a second implementation breaks by
 * accident rather than by disagreement. So there is one builder, and anything keeping host state goes through
 * it. Adding a field here that any of the three rules forbids makes every state document sweepable at once,
 * which is why the rules are restated above rather than referenced.</p>
 *
 * <p>The document does carry the {@code CinRemote} mixin, which hxpr requires of anything writing a
 * {@code cin_*} field, and the state itself lives in {@code cin_ingestProperties}.</p>
 */
final class HxprStateDocuments {

    private HxprStateDocuments() {
    }

    /**
     * A state document as hxpr receives it.
     *
     * @param sysName the document name on create, or {@code null} for an update, where naming it again would
     *                be a no-op at best and a rename at worst
     * @param props   the state, which becomes {@code cin_ingestProperties}
     */
    static HxprDocument stateDocument(String sysName, Map<String, Object> props) {
        HxprDocument document = new HxprDocument();
        if (sysName != null) {
            document.setSysPrimaryType("SysFile");
            document.setSysName(sysName);
        }
        document.setSysMixinTypes(List.of(HxprDocument.MIXIN_CIN_REMOTE));
        document.setCinIngestProperties(props);
        // cin_ingestPropertyNames must always mirror cin_ingestProperties.keySet(); building it here rather
        // than at each call site is the point of sharing this.
        document.setCinIngestPropertyNames(new ArrayList<>(props.keySet()));
        return document;
    }

    /**
     * A document name derived from a qualified source id, with a caller-supplied prefix so two kinds of state
     * for the same source cannot collide on one path.
     *
     * <p>The substitution is lossy, so a store using this must also record the exact id in a property and
     * check it on load. Two source ids differing only in a character a name cannot carry would otherwise
     * resolve to the same document, and reading one source's state as another's fails silently.</p>
     */
    static String documentName(String prefix, String qualifiedSourceId) {
        String safe = qualifiedSourceId == null ? "" : qualifiedSourceId.replaceAll("[^A-Za-z0-9._-]", "-");
        return prefix + (safe.isBlank() ? "unknown" : safe);
    }

    /** Leading slash, no trailing slash, and never blank, because the result becomes a path prefix. */
    static String normalizeFolder(String path, String what) {
        String trimmed = path == null ? "" : path.trim();
        while (trimmed.endsWith("/")) {
            trimmed = trimmed.substring(0, trimmed.length() - 1);
        }
        if (trimmed.isBlank()) {
            throw new IllegalArgumentException("A " + what + " folder path is required");
        }
        return trimmed.startsWith("/") ? trimmed : "/" + trimmed;
    }
}
