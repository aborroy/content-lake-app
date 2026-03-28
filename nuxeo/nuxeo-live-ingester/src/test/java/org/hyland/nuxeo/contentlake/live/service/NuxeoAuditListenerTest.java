package org.hyland.nuxeo.contentlake.live.service;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.hyland.nuxeo.contentlake.client.NuxeoClient;
import org.hyland.nuxeo.contentlake.live.client.NuxeoAuditClient;
import org.hyland.nuxeo.contentlake.live.config.NuxeoLiveProperties;
import org.hyland.nuxeo.contentlake.live.model.AuditCursor;
import org.hyland.nuxeo.contentlake.live.model.NuxeoAuditEntry;
import org.hyland.nuxeo.contentlake.live.model.NuxeoAuditPage;
import org.hyland.nuxeo.contentlake.service.NuxeoScopeResolver;
import org.hyland.contentlake.service.NodeSyncService;
import org.hyland.contentlake.spi.SourceNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class NuxeoAuditListenerTest {

    private static final Clock FIXED_CLOCK =
            Clock.fixed(Instant.parse("2026-03-26T17:00:00Z"), ZoneOffset.UTC);

    @Mock
    private NuxeoAuditClient auditClient;

    @Mock
    private AuditCursorStore cursorStore;

    @Mock
    private NodeSyncService nodeSyncService;

    @Mock
    private NuxeoClient nuxeoClient;

    @Mock
    private NuxeoScopeResolver scopeResolver;

    private NuxeoAuditListener listener;
    private SimpleMeterRegistry meterRegistry;

    @BeforeEach
    void setUp() {
        NuxeoLiveProperties props = new NuxeoLiveProperties();
        props.getAudit().setEnabled(true);
        props.getAudit().setPageSize(2);
        props.getAudit().setInitialLookback(Duration.ofMinutes(5));

        meterRegistry = new SimpleMeterRegistry();
        NuxeoAuditMetrics metrics = new NuxeoAuditMetrics(meterRegistry, FIXED_CLOCK);
        listener = new NuxeoAuditListener(
                auditClient,
                cursorStore,
                nodeSyncService,
                nuxeoClient,
                scopeResolver,
                props,
                metrics,
                FIXED_CLOCK
        );

        when(nuxeoClient.getSourceType()).thenReturn("nuxeo");
        when(nuxeoClient.getSourceId()).thenReturn("local");
    }

    @Test
    void listen_processesEntriesAndAdvancesCursorAfterCompletedCycle() {
        AuditCursor initialCursor = new AuditCursor(OffsetDateTime.parse("2026-03-26T16:48:41.235Z"), 46);
        OffsetDateTime windowEnd = OffsetDateTime.parse("2026-03-26T17:00:00Z");
        NuxeoAuditEntry created = entry(48, "documentCreated", "doc-48",
                "2026-03-26T16:48:45.902Z", "2026-03-26T16:48:45.939Z");
        NuxeoAuditEntry removed = entry(49, "documentRemoved", "doc-49",
                "2026-03-26T16:48:46.090Z", "2026-03-26T16:48:46.096Z");
        SourceNode sourceNode = sourceNode("doc-48");

        when(cursorStore.load("nuxeo:local")).thenReturn(Optional.of(initialCursor));
        when(auditClient.fetchPage(initialCursor, windowEnd, 2)).thenReturn(pageOf(false, created, removed));
        when(nuxeoClient.getNode("doc-48")).thenReturn(sourceNode);
        when(scopeResolver.isInScope(sourceNode)).thenReturn(true);

        listener.listen();

        verify(nodeSyncService).syncNode(sourceNode);
        verify(nodeSyncService).deleteNode("doc-49", OffsetDateTime.parse("2026-03-26T16:48:46.090Z"));
        verify(cursorStore).save("nuxeo:local", new AuditCursor(OffsetDateTime.parse("2026-03-26T16:48:46.096Z"), 49));
        verify(auditClient, never()).fetchPage(
                new AuditCursor(OffsetDateTime.parse("2026-03-26T16:48:46.096Z"), 49),
                windowEnd,
                2
        );
    }

    @Test
    void listen_fetchesNextPageWhenNuxeoReportsMorePages() {
        AuditCursor initialCursor = new AuditCursor(OffsetDateTime.parse("2026-03-26T16:48:41.235Z"), 46);
        OffsetDateTime windowEnd = OffsetDateTime.parse("2026-03-26T17:00:00Z");
        NuxeoAuditEntry created = entry(48, "documentCreated", "doc-48",
                "2026-03-26T16:48:45.902Z", "2026-03-26T16:48:45.939Z");
        NuxeoAuditEntry modified = entry(49, "documentModified", "doc-49",
                "2026-03-26T16:48:46.090Z", "2026-03-26T16:48:46.096Z");
        NuxeoAuditEntry removed = entry(50, "documentRemoved", "doc-50",
                "2026-03-26T16:48:46.500Z", "2026-03-26T16:48:46.550Z");
        AuditCursor nextCursor = new AuditCursor(OffsetDateTime.parse("2026-03-26T16:48:46.096Z"), 49);
        SourceNode createdNode = sourceNode("doc-48");
        SourceNode modifiedNode = sourceNode("doc-49");

        when(cursorStore.load("nuxeo:local")).thenReturn(Optional.of(initialCursor));
        when(auditClient.fetchPage(initialCursor, windowEnd, 2)).thenReturn(pageOf(true, created, modified));
        when(auditClient.fetchPage(nextCursor, windowEnd, 2)).thenReturn(pageOf(false, removed));
        when(nuxeoClient.getNode("doc-48")).thenReturn(createdNode);
        when(nuxeoClient.getNode("doc-49")).thenReturn(modifiedNode);
        when(scopeResolver.isInScope(createdNode)).thenReturn(true);
        when(scopeResolver.isInScope(modifiedNode)).thenReturn(true);

        listener.listen();

        verify(auditClient).fetchPage(initialCursor, windowEnd, 2);
        verify(auditClient).fetchPage(nextCursor, windowEnd, 2);
        verify(nodeSyncService).syncNode(createdNode);
        verify(nodeSyncService).syncNode(modifiedNode);
        verify(nodeSyncService).deleteNode("doc-50", OffsetDateTime.parse("2026-03-26T16:48:46.500Z"));
        verify(cursorStore).save("nuxeo:local", new AuditCursor(OffsetDateTime.parse("2026-03-26T16:48:46.550Z"), 50));
    }

    @Test
    void listen_recordsSkippedWhenOutOfScopeNodeNeverIndexed() {
        AuditCursor initialCursor = new AuditCursor(OffsetDateTime.parse("2026-03-26T16:48:41.235Z"), 46);
        OffsetDateTime windowEnd = OffsetDateTime.parse("2026-03-26T17:00:00Z");
        NuxeoAuditEntry modified = entry(50, "documentModified", "doc-50",
                "2026-03-26T16:48:47.000Z", "2026-03-26T16:48:47.050Z");
        SourceNode sourceNode = sourceNode("doc-50");

        when(cursorStore.load("nuxeo:local")).thenReturn(Optional.of(initialCursor));
        when(auditClient.fetchPage(initialCursor, windowEnd, 2)).thenReturn(pageOf(false, modified));
        when(nuxeoClient.getNode("doc-50")).thenReturn(sourceNode);
        when(scopeResolver.isInScope(sourceNode)).thenReturn(false);
        when(nodeSyncService.deleteNode("doc-50", OffsetDateTime.parse("2026-03-26T16:48:47.000Z"))).thenReturn(false);

        listener.listen();

        verify(nodeSyncService).deleteNode("doc-50", OffsetDateTime.parse("2026-03-26T16:48:47.000Z"));
        verify(cursorStore).save("nuxeo:local", new AuditCursor(OffsetDateTime.parse("2026-03-26T16:48:47.050Z"), 50));
        assertThat(outcomeCount("documentModified", "skipped")).isEqualTo(1.0);
        assertThat(outcomeCount("documentModified", "deleted")).isZero();
    }

    @Test
    void listen_recordsDeletedWhenIndexedNodeGoesOutOfScope() {
        AuditCursor initialCursor = new AuditCursor(OffsetDateTime.parse("2026-03-26T16:48:41.235Z"), 46);
        OffsetDateTime windowEnd = OffsetDateTime.parse("2026-03-26T17:00:00Z");
        NuxeoAuditEntry modified = entry(50, "documentModified", "doc-50",
                "2026-03-26T16:48:47.000Z", "2026-03-26T16:48:47.050Z");
        SourceNode sourceNode = sourceNode("doc-50");

        when(cursorStore.load("nuxeo:local")).thenReturn(Optional.of(initialCursor));
        when(auditClient.fetchPage(initialCursor, windowEnd, 2)).thenReturn(pageOf(false, modified));
        when(nuxeoClient.getNode("doc-50")).thenReturn(sourceNode);
        when(scopeResolver.isInScope(sourceNode)).thenReturn(false);
        when(nodeSyncService.deleteNode("doc-50", OffsetDateTime.parse("2026-03-26T16:48:47.000Z"))).thenReturn(true);

        listener.listen();

        verify(nodeSyncService).deleteNode("doc-50", OffsetDateTime.parse("2026-03-26T16:48:47.000Z"));
        assertThat(outcomeCount("documentModified", "deleted")).isEqualTo(1.0);
        assertThat(outcomeCount("documentModified", "skipped")).isZero();
    }

    @Test
    void listen_recordsSkippedWhenCreatedNodeNotFoundAndNotIndexed() {
        AuditCursor initialCursor = new AuditCursor(OffsetDateTime.parse("2026-03-26T16:48:41.235Z"), 46);
        OffsetDateTime windowEnd = OffsetDateTime.parse("2026-03-26T17:00:00Z");
        NuxeoAuditEntry created = entry(51, "documentCreated", "doc-51",
                "2026-03-26T16:48:48.000Z", "2026-03-26T16:48:48.050Z");

        when(cursorStore.load("nuxeo:local")).thenReturn(Optional.of(initialCursor));
        when(auditClient.fetchPage(initialCursor, windowEnd, 2)).thenReturn(pageOf(false, created));
        when(nuxeoClient.getNode("doc-51")).thenReturn(null);
        when(nodeSyncService.deleteNode("doc-51", OffsetDateTime.parse("2026-03-26T16:48:48.000Z"))).thenReturn(false);

        listener.listen();

        verify(nodeSyncService).deleteNode("doc-51", OffsetDateTime.parse("2026-03-26T16:48:48.000Z"));
        assertThat(outcomeCount("documentCreated", "skipped")).isEqualTo(1.0);
        assertThat(outcomeCount("documentCreated", "deleted")).isZero();
    }

    @Test
    void listen_recordsDeletedWhenCreatedNodeNotFoundButWasIndexed() {
        AuditCursor initialCursor = new AuditCursor(OffsetDateTime.parse("2026-03-26T16:48:41.235Z"), 46);
        OffsetDateTime windowEnd = OffsetDateTime.parse("2026-03-26T17:00:00Z");
        NuxeoAuditEntry created = entry(51, "documentCreated", "doc-51",
                "2026-03-26T16:48:48.000Z", "2026-03-26T16:48:48.050Z");

        when(cursorStore.load("nuxeo:local")).thenReturn(Optional.of(initialCursor));
        when(auditClient.fetchPage(initialCursor, windowEnd, 2)).thenReturn(pageOf(false, created));
        when(nuxeoClient.getNode("doc-51")).thenReturn(null);
        when(nodeSyncService.deleteNode("doc-51", OffsetDateTime.parse("2026-03-26T16:48:48.000Z"))).thenReturn(true);

        listener.listen();

        verify(nodeSyncService).deleteNode("doc-51", OffsetDateTime.parse("2026-03-26T16:48:48.000Z"));
        assertThat(outcomeCount("documentCreated", "deleted")).isEqualTo(1.0);
        assertThat(outcomeCount("documentCreated", "skipped")).isZero();
    }

    @Test
    void listen_advancesCursorAndContinuesWhenEntryProcessingFails() {
        AuditCursor initialCursor = new AuditCursor(OffsetDateTime.parse("2026-03-26T16:48:41.235Z"), 46);
        OffsetDateTime windowEnd = OffsetDateTime.parse("2026-03-26T17:00:00Z");
        NuxeoAuditEntry created = entry(51, "documentCreated", "doc-51",
                "2026-03-26T16:48:48.000Z", "2026-03-26T16:48:48.050Z");
        NuxeoAuditEntry removed = entry(52, "documentRemoved", "doc-52",
                "2026-03-26T16:48:49.000Z", "2026-03-26T16:48:49.050Z");
        SourceNode sourceNode = sourceNode("doc-51");

        when(cursorStore.load("nuxeo:local")).thenReturn(Optional.of(initialCursor));
        when(auditClient.fetchPage(initialCursor, windowEnd, 2)).thenReturn(pageOf(false, created, removed));
        when(nuxeoClient.getNode("doc-51")).thenReturn(sourceNode);
        when(scopeResolver.isInScope(sourceNode)).thenReturn(true);
        when(nodeSyncService.syncNode(sourceNode)).thenThrow(new RuntimeException("HXPR unavailable"));

        listener.listen();

        verify(nodeSyncService).deleteNode("doc-52", OffsetDateTime.parse("2026-03-26T16:48:49.000Z"));
        verify(cursorStore).save(eq("nuxeo:local"), eq(new AuditCursor(OffsetDateTime.parse("2026-03-26T16:48:49.050Z"), 52)));
    }

    @Test
    void listen_permissionUpdate_updatesAclWithoutFullResync() {
        AuditCursor initialCursor = new AuditCursor(OffsetDateTime.parse("2026-03-26T16:48:41.235Z"), 46);
        OffsetDateTime windowEnd = OffsetDateTime.parse("2026-03-26T17:00:00Z");
        NuxeoAuditEntry securityUpdated = entry(53, "documentSecurityUpdated", "doc-53",
                "2026-03-26T16:48:50.000Z", "2026-03-26T16:48:50.050Z");
        SourceNode sourceNode = sourceNode("doc-53");

        when(cursorStore.load("nuxeo:local")).thenReturn(Optional.of(initialCursor));
        when(auditClient.fetchPage(initialCursor, windowEnd, 2)).thenReturn(pageOf(false, securityUpdated));
        when(nuxeoClient.getNode("doc-53")).thenReturn(sourceNode);
        when(scopeResolver.isInScope(sourceNode)).thenReturn(true);

        listener.listen();

        verify(nodeSyncService).updatePermissions(sourceNode);
        verify(nodeSyncService, never()).syncNode(sourceNode);
        assertThat(outcomeCount("documentSecurityUpdated", "updated")).isEqualTo(1.0);
    }

    @Test
    void listen_doesNotPersistCursorWhenAuditFetchFails() {
        AuditCursor initialCursor = new AuditCursor(OffsetDateTime.parse("2026-03-26T16:48:41.235Z"), 46);
        OffsetDateTime windowEnd = OffsetDateTime.parse("2026-03-26T17:00:00Z");

        when(cursorStore.load("nuxeo:local")).thenReturn(Optional.of(initialCursor));
        when(auditClient.fetchPage(initialCursor, windowEnd, 2))
                .thenThrow(new IllegalStateException("Nuxeo audit unavailable"));

        listener.listen();

        verify(cursorStore, never()).save(eq("nuxeo:local"), any());
        verify(nodeSyncService, never()).deleteNode(any(), any());
        verify(nodeSyncService, never()).syncNode(any());
    }

    private static NuxeoAuditPage pageOf(boolean hasMore, NuxeoAuditEntry... entries) {
        NuxeoAuditPage page = new NuxeoAuditPage();
        page.setEntries(List.of(entries));
        page.setNextPageAvailable(hasMore);
        return page;
    }

    private static NuxeoAuditEntry entry(long id,
                                         String eventId,
                                         String docUuid,
                                         String eventDate,
                                         String logDate) {
        return new NuxeoAuditEntry(
                "logEntry",
                id,
                eventId,
                "default",
                docUuid,
                "/default-domain/workspaces/" + docUuid,
                "Note",
                OffsetDateTime.parse(eventDate),
                OffsetDateTime.parse(logDate)
        );
    }

    private double outcomeCount(String eventId, String outcome) {
        Counter counter = meterRegistry.find("contentlake.nuxeo.audit.events.total")
                .tags("repository", "nuxeo:local", "eventId", eventId, "outcome", outcome)
                .counter();
        return counter != null ? counter.count() : 0.0;
    }

    private static SourceNode sourceNode(String nodeId) {
        return new SourceNode(
                nodeId,
                "local",
                "nuxeo",
                nodeId,
                "/default-domain/workspaces",
                "text/plain",
                OffsetDateTime.parse("2026-03-26T16:48:00Z"),
                false,
                Set.of("GROUP_EVERYONE"),
                Set.of(),
                Map.of("nuxeo_documentType", "Note", "nuxeo_path", "/default-domain/workspaces/" + nodeId)
        );
    }
}
