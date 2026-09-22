package org.hyland.contentlake.service;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.time.OffsetDateTime;
import java.util.List;

/**
 * Which roots an operator chose for a source, and who chose them.
 *
 * <p>An empty list is a meaningful value and not the same as no selection at all: it means somebody cleared
 * the choice, which a pass must treat as "nothing to walk" rather than as "walk everything". The difference
 * between the two is carried by {@link RootSelectionStore#load} returning empty versus returning this with an
 * empty list, and getting it wrong in the permissive direction would silently re-ingest a whole source.</p>
 *
 * @param rootNodeIds node ids to start a pass from, in the order they were chosen
 * @param updatedAt   when the selection was last written, for an operator answering "why did this change"
 * @param updatedBy   the authenticated principal that wrote it, for the same reason
 */
public record RootSelection(List<String> rootNodeIds, OffsetDateTime updatedAt, String updatedBy) {

    public RootSelection {
        rootNodeIds = rootNodeIds == null ? List.of() : List.copyOf(rootNodeIds);
    }

    /** A selection made now by the named principal. */
    public static RootSelection of(List<String> rootNodeIds, String updatedBy) {
        return new RootSelection(rootNodeIds, OffsetDateTime.now(), updatedBy);
    }

    /**
     * Whether the choice was "nothing".
     *
     * <p>{@code @JsonIgnore} is load-bearing, not tidiness. Jackson treats an {@code isX()} accessor as a
     * property, so without it this is written to the file store as {@code "empty": false} and then rejected on
     * read as an unrecognised field, because the record's canonical constructor has no such component. The
     * symptom is a store that writes successfully and cannot read anything back. Any further derived accessor
     * added here needs the same annotation.</p>
     */
    @JsonIgnore
    public boolean isEmpty() {
        return rootNodeIds.isEmpty();
    }
}
