package org.hyland.alfresco.contentlake.live.handler;

import org.alfresco.repo.event.v1.model.DataAttributes;
import org.alfresco.repo.event.v1.model.NodeResource;
import org.alfresco.repo.event.v1.model.RepoEvent;
import org.alfresco.repo.event.v1.model.Resource;
import org.junit.jupiter.api.Test;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests model the Alfresco event SDK semantics: {@code resourceBefore} carries
 * ONLY the fields that changed in the event (a delta). Unchanged fields come
 * back as {@code null} on the previous resource even though they exist on the
 * current resource. The filter must treat null-on-previous as "unchanged".
 */
class IgnoreSyncStatusOnlyUpdateFilterTest {

    @Test
    void rejectsClearSyncStatusEvent() {
        // Tear-down clears cl:syncStatusValue and bumps cm:modified/cm:modifier.
        // resourceBefore carries only the changed fields' previous values.
        NodeResource current = currentNode(
                Map.of("cm:title", (Serializable) "doc.txt",
                        "cm:modified", "2026-05-27T10:00:00.000Z",
                        "cm:modifier", "admin"),
                Set.of("cm:auditable", "cl:syncStatus"));

        NodeResource previous = previousDelta(
                Map.of("cl:syncStatusValue", (Serializable) "INDEXED",
                        "cm:modified", "2026-05-27T09:00:00.000Z"),
                Set.of("cm:auditable", "cl:syncStatus"));

        assertThat(IgnoreSyncStatusOnlyUpdateFilter.get().test(event(current, previous))).isFalse();
    }

    @Test
    void rejectsWriteBackSyncStatusEvent() {
        // Write-back: cl:syncStatusValue PENDING -> INDEXED, cl:syncStatus aspect added.
        NodeResource current = currentNode(
                Map.of("cl:syncStatusValue", (Serializable) "INDEXED",
                        "cm:modified", "2026-05-27T10:00:00.000Z"),
                Set.of("cm:auditable", "cl:syncStatus"));

        NodeResource previous = previousDelta(
                Map.of("cm:modified", (Serializable) "2026-05-27T09:00:00.000Z"),
                Set.of("cm:auditable")); // before: no cl:syncStatus aspect

        assertThat(IgnoreSyncStatusOnlyUpdateFilter.get().test(event(current, previous))).isFalse();
    }

    @Test
    void acceptsRealUserPropertyChange() {
        // User-driven title rename: previous delta contains the old title.
        NodeResource current = currentNode(
                Map.of("cm:title", (Serializable) "renamed.txt",
                        "cm:modified", "2026-05-27T10:00:00.000Z"),
                Set.of("cm:auditable"));

        NodeResource previous = previousDelta(
                Map.of("cm:title", (Serializable) "doc.txt",
                        "cm:modified", "2026-05-27T09:00:00.000Z"),
                null);

        assertThat(IgnoreSyncStatusOnlyUpdateFilter.get().test(event(current, previous))).isTrue();
    }

    @Test
    void acceptsContentChange() {
        org.alfresco.repo.event.v1.model.ContentInfo previousContent =
                mock(org.alfresco.repo.event.v1.model.ContentInfo.class);

        NodeResource current = currentNode(null, null);
        when(current.getContent()).thenReturn(mock(org.alfresco.repo.event.v1.model.ContentInfo.class));

        NodeResource previous = previousDelta(null, null);
        when(previous.getContent()).thenReturn(previousContent);

        assertThat(IgnoreSyncStatusOnlyUpdateFilter.get().test(event(current, previous))).isTrue();
    }

    @Test
    void rejectsWhenPreviousDeltaIsEmpty() {
        // Defensive: empty delta means the SDK sent no changed-field info.
        NodeResource current = currentNode(
                Map.of("cm:title", (Serializable) "doc.txt"),
                Set.of("cm:auditable", "cl:syncStatus"));

        NodeResource previous = previousDelta(null, null);

        assertThat(IgnoreSyncStatusOnlyUpdateFilter.get().test(event(current, previous))).isFalse();
    }

    private static NodeResource currentNode(Map<String, Serializable> properties, Set<String> aspects) {
        NodeResource n = mock(NodeResource.class);
        when(n.getId()).thenReturn("node-1");
        when(n.getProperties()).thenReturn(properties == null ? null : new HashMap<>(properties));
        when(n.getAspectNames()).thenReturn(aspects);
        return n;
    }

    private static NodeResource previousDelta(Map<String, Serializable> changedPropertiesBefore,
                                               Set<String> aspectsBefore) {
        NodeResource n = mock(NodeResource.class);
        // Per ACS event SDK: resourceBefore.getId() is null when id didn't change.
        when(n.getId()).thenReturn(null);
        when(n.getProperties()).thenReturn(
                changedPropertiesBefore == null ? null : new HashMap<>(changedPropertiesBefore));
        when(n.getAspectNames()).thenReturn(aspectsBefore);
        return n;
    }

    @SuppressWarnings("unchecked")
    private static RepoEvent<DataAttributes<Resource>> event(Resource current, Resource previous) {
        RepoEvent<DataAttributes<Resource>> event = mock(RepoEvent.class);
        DataAttributes<Resource> data = mock(DataAttributes.class);
        when(event.getData()).thenReturn(data);
        when(data.getResource()).thenReturn(current);
        when(data.getResourceBefore()).thenReturn(previous);
        return event;
    }
}
