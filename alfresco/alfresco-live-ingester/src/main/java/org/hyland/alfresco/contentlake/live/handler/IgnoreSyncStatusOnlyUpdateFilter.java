package org.hyland.alfresco.contentlake.live.handler;

import org.alfresco.event.sdk.handling.filter.EventFilter;
import org.alfresco.repo.event.v1.model.DataAttributes;
import org.alfresco.repo.event.v1.model.NodeResource;
import org.alfresco.repo.event.v1.model.RepoEvent;
import org.alfresco.repo.event.v1.model.Resource;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Filters out node update events where only {@code cl:syncStatus} or {@code cl:indexed} changed.
 *
 * <p>When the live ingester writes back sync status to Alfresco, it triggers a node update
 * event. Without this filter, the updated event would be processed again, causing an infinite
 * indexing loop. This filter compares the current and previous node state and returns
 * {@code false} (reject) if the only change is to Content Lake aspect properties.</p>
 */
final class IgnoreSyncStatusOnlyUpdateFilter implements EventFilter {

    private static final IgnoreSyncStatusOnlyUpdateFilter INSTANCE = new IgnoreSyncStatusOnlyUpdateFilter();

    private static final String CL_SYNC_STATUS = "cl:syncStatus";
    private static final String CL_SYNC_STATUS_VALUE = "cl:syncStatusValue";
    private static final String CL_SYNC_ERROR = "cl:syncError";
    private static final String CL_INDEXED_ASPECT = "cl:indexed";

    // Audit properties bumped by every node update; ignored when comparing
    // before/after snapshots so a clear/write of CL state is recognised as
    // CL-only even though Alfresco refreshes cm:modified at the same time.
    private static final String CM_MODIFIED = "cm:modified";
    private static final String CM_MODIFIER = "cm:modifier";

    static EventFilter get() {
        return INSTANCE;
    }

    private IgnoreSyncStatusOnlyUpdateFilter() {
    }

    @Override
    public boolean test(RepoEvent<DataAttributes<Resource>> event) {
        if (event == null || event.getData() == null) {
            return true;
        }

        Resource currentResource = event.getData().getResource();
        Resource previousResource = event.getData().getResourceBefore();

        if (!(currentResource instanceof NodeResource current)) {
            return true;
        }

        if (!(previousResource instanceof NodeResource previous)) {
            // No delta available: cannot decide CL-only — accept.
            return true;
        }

        // Alfresco event SDK semantics: resourceBefore carries ONLY the fields that
        // changed in this event. Null fields on `previous` mean "unchanged from current".
        // We must only treat fields as differing when the BEFORE side is non-null AND
        // differs, otherwise we'd flag every update as a non-CL change.

        if (changedAndDiffers(current.getName(), previous.getName())) {
            return true;
        }
        if (changedAndDiffers(current.getNodeType(), previous.getNodeType())) {
            return true;
        }
        if (changedAndDiffers(current.getContent(), previous.getContent())) {
            return true;
        }
        if (changedAndDiffers(current.getPrimaryAssocQName(), previous.getPrimaryAssocQName())) {
            return true;
        }
        if (changedAndDiffers(current.getPrimaryHierarchy(), previous.getPrimaryHierarchy())) {
            return true;
        }

        Map<String, java.io.Serializable> previousRawProps = previous.getProperties();
        if (previousRawProps == null) {
            // No property delta in the event; nothing more to compare.
            return false;
        }

        Map<String, java.io.Serializable> currentProps = filterContentLakeProperties(current.getProperties());
        Map<String, java.io.Serializable> previousProps = filterContentLakeProperties(previousRawProps);

        // resourceBefore.getProperties only carries the changed properties' previous values.
        // To check whether any non-CL property changed, intersect current with the keys
        // present in previous and compare values.
        for (Map.Entry<String, java.io.Serializable> entry : previousProps.entrySet()) {
            if (!Objects.equals(entry.getValue(), currentProps.get(entry.getKey()))) {
                return true;
            }
        }

        java.util.Set<String> previousAspects = previous.getAspectNames();
        if (previousAspects == null) {
            // No aspect delta in this event; CL-only.
            return false;
        }
        return !onlyContentLakeAspectsChanged(current.getAspectNames(), previousAspects);
    }

    private static boolean changedAndDiffers(Object cur, Object prev) {
        return prev != null && !Objects.equals(cur, prev);
    }

    private Map<String, java.io.Serializable> filterContentLakeProperties(Map<String, java.io.Serializable> properties) {
        if (properties == null || properties.isEmpty()) {
            return Map.of();
        }

        Map<String, java.io.Serializable> filtered = new HashMap<>(properties);
        filtered.remove(CL_SYNC_STATUS);
        filtered.remove(CL_SYNC_STATUS_VALUE);
        filtered.remove(CL_SYNC_ERROR);
        filtered.remove(CM_MODIFIED);
        filtered.remove(CM_MODIFIER);
        return filtered;
    }

    private boolean onlyContentLakeAspectsChanged(java.util.Set<String> currentAspects,
                                                   java.util.Set<String> previousAspects) {
        if (Objects.equals(currentAspects, previousAspects)) {
            // Aspect sets identical: nothing aspect-side changed. Combined with the
            // earlier property check, this means the only diffs are within stripped
            // CL_* and audit fields — the event is a CL-only update.
            return true;
        }

        if (currentAspects == null || previousAspects == null) {
            return false;
        }

        java.util.Set<String> currentFiltered = new java.util.HashSet<>(currentAspects);
        java.util.Set<String> previousFiltered = new java.util.HashSet<>(previousAspects);

        currentFiltered.remove(CL_INDEXED_ASPECT);
        previousFiltered.remove(CL_INDEXED_ASPECT);
        currentFiltered.remove(CL_SYNC_STATUS);
        previousFiltered.remove(CL_SYNC_STATUS);

        return currentFiltered.equals(previousFiltered);
    }
}
