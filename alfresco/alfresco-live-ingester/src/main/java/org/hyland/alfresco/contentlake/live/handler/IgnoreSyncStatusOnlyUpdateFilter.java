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

        if (previousResource == null) {
            return true;
        }

        if (!(previousResource instanceof NodeResource previous)) {
            return true;
        }

        if (!Objects.equals(current.getId(), previous.getId())) {
            return true;
        }

        if (!Objects.equals(current.getPrimaryHierarchy(), previous.getPrimaryHierarchy())
                || !Objects.equals(current.getName(), previous.getName())
                || !Objects.equals(current.getNodeType(), previous.getNodeType())
                || !Objects.equals(current.isFile(), previous.isFile())
                || !Objects.equals(current.isFolder(), previous.isFolder())
                || !Objects.equals(current.getContent(), previous.getContent())
                || !Objects.equals(current.getPrimaryAssocQName(), previous.getPrimaryAssocQName())) {
            return true;
        }

        Map<String, java.io.Serializable> currentProps = filterContentLakeProperties(current.getProperties());
        Map<String, java.io.Serializable> previousProps = filterContentLakeProperties(previous.getProperties());

        if (!Objects.equals(currentProps, previousProps)) {
            return true;
        }

        return !onlyContentLakeAspectsChanged(current.getAspectNames(), previous.getAspectNames());
    }

    private Map<String, java.io.Serializable> filterContentLakeProperties(Map<String, java.io.Serializable> properties) {
        if (properties == null || properties.isEmpty()) {
            return Map.of();
        }

        Map<String, java.io.Serializable> filtered = new HashMap<>(properties);
        filtered.remove(CL_SYNC_STATUS);
        filtered.remove(CL_SYNC_STATUS_VALUE);
        filtered.remove(CL_SYNC_ERROR);
        return filtered;
    }

    private boolean onlyContentLakeAspectsChanged(java.util.Set<String> currentAspects,
                                                   java.util.Set<String> previousAspects) {
        if (Objects.equals(currentAspects, previousAspects)) {
            return false;
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
