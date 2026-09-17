package org.hyland.contentlake.service;

import org.hyland.contentlake.client.HxprDocumentApi;
import org.hyland.contentlake.client.HxprService;
import org.hyland.contentlake.model.ContentLakeIngestProperties;
import org.hyland.contentlake.model.HxprDocument;
import org.hyland.contentlake.service.chunking.SimpleChunkingService;
import org.hyland.contentlake.spi.ContentSourceClient;
import org.hyland.contentlake.spi.SourceNode;
import org.hyland.contentlake.spi.TextExtractor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * The generic {@code source_*} keys come from the {@link SourceNode} record, not from each adapter (#147).
 *
 * <p>They used to come only from an adapter's {@code sourceProperties} map. The three in-tree adapters
 * duplicate the record's own fields into that map by hand, so they never noticed; a connector loaded from
 * a jar populates the record correctly, sets only its own namespaced properties, and had its documents
 * stored with no {@code source_*} key at all. The measured cost was that {@code sourceType} on a search
 * request matched nothing for such a source, and that every result rendered untitled in both UIs.</p>
 */
@ExtendWith(MockitoExtension.class)
class NodeSyncServiceGenericSourcePropertiesTest {

    private static final String NODE_ID = "/data/connector/quarterly-review.txt";
    private static final String SOURCE_ID = "sample-directory";
    private static final String SOURCE_TYPE = "sample-directory";
    private static final String TARGET_PATH = "/connector-sync";

    @Mock
    private ContentSourceClient sourceClient;
    @Mock
    private HxprDocumentApi documentApi;
    @Mock
    private HxprService hxprService;
    @Mock
    private TextExtractor textExtractor;
    @Mock
    private EmbeddingService embeddingService;
    @Mock
    private SimpleChunkingService chunkingService;

    private NodeSyncService service;

    @BeforeEach
    void setUp() {
        service = new NodeSyncService(
                sourceClient, documentApi, hxprService, textExtractor, embeddingService,
                chunkingService, TARGET_PATH, null, false, true);
    }

    /**
     * A connector that sets none of the generic keys still gets all of them, which is the whole point:
     * nothing tells a plugin author that the duplication was ever required.
     */
    @Test
    void aConnectorThatSetsNoGenericKeysStillGetsThemAll() {
        Map<String, Object> stored = ingestPropertiesOf(connectorNode(Map.of(
                "sample_directory_absolutePath", NODE_ID,
                "sample_directory_sizeBytes", 2048)));

        assertThat(stored)
                .containsEntry(ContentLakeIngestProperties.SOURCE_NODE_ID, NODE_ID)
                .containsEntry(ContentLakeIngestProperties.SOURCE_TYPE, SOURCE_TYPE)
                .containsEntry(ContentLakeIngestProperties.SOURCE_NAME, "quarterly-review.txt")
                .containsEntry(ContentLakeIngestProperties.SOURCE_PATH, "/data/connector")
                .containsEntry(ContentLakeIngestProperties.SOURCE_MIME_TYPE, "text/plain");

        // The connector's own namespaced properties survive alongside them.
        assertThat(stored).containsEntry("sample_directory_sizeBytes", 2048);
    }

    /** {@code cin_ingestPropertyNames} must mirror the enriched map, not the map before seeding. */
    @Test
    void thePropertyNameListMirrorsTheEnrichedMap() {
        HxprDocument created = captureCreated(connectorNode(Map.of("sample_directory_sizeBytes", 2048)));

        assertThat(created.getCinIngestPropertyNames())
                .containsExactlyInAnyOrderElementsOf(created.getCinIngestProperties().keySet())
                .contains(ContentLakeIngestProperties.SOURCE_TYPE, ContentLakeIngestProperties.SOURCE_NAME);
    }

    /**
     * An adapter that sets one of these keys itself still wins, so the three in-tree sources are
     * unaffected and their hand-duplicated values are redundant rather than load-bearing.
     */
    @Test
    void anAdapterSuppliedValueOverridesTheRecord() {
        Map<String, Object> stored = ingestPropertiesOf(connectorNode(Map.of(
                ContentLakeIngestProperties.SOURCE_PATH, "/a/path/the/adapter/prefers",
                ContentLakeIngestProperties.SOURCE_NAME, "a-name-the-adapter-prefers.txt")));

        assertThat(stored)
                .containsEntry(ContentLakeIngestProperties.SOURCE_PATH, "/a/path/the/adapter/prefers")
                .containsEntry(ContentLakeIngestProperties.SOURCE_NAME, "a-name-the-adapter-prefers.txt")
                // Not overridden, so still the record's.
                .containsEntry(ContentLakeIngestProperties.SOURCE_TYPE, SOURCE_TYPE);
    }

    /**
     * The timestamp is normalised to UTC because the {@code modifiedAfter} / {@code modifiedBefore}
     * filters compare this value as text, where a non-{@code Z} offset does not order correctly.
     */
    @Test
    void theModifiedTimestampIsSeededAsUtc() {
        SourceNode node = new SourceNode(
                NODE_ID, SOURCE_ID, SOURCE_TYPE,
                "quarterly-review.txt", "/data/connector", "text/plain",
                OffsetDateTime.parse("2026-09-17T12:00:00+02:00"),
                false,
                Set.of("__Everyone__"), Set.of(),
                Map.of());

        assertThat(ingestPropertiesOf(node))
                .containsEntry(ContentLakeIngestProperties.SOURCE_MODIFIED_AT, "2026-09-17T10:00:00.000000000Z");
    }

    /**
     * The stored form must round-trip losslessly, or every sweep re-extracts the whole corpus.
     *
     * <p>{@code isStale} asks whether the incoming timestamp is *after* the stored one. A local
     * filesystem reports nanoseconds, so a format truncating to milliseconds makes an untouched node
     * compare as newer than its own stored copy every time, and content reuse never gets a chance to
     * fire. This is not hypothetical: the first version of the fix truncated, and the connector suite
     * re-embedded on its second pass because of it.
     */
    @Test
    void aNanosecondTimestampSurvivesStorageUnchanged() {
        OffsetDateTime fromAFilesystem = OffsetDateTime.parse("2026-09-17T09:23:25.023986867Z");
        SourceNode node = new SourceNode(
                NODE_ID, SOURCE_ID, SOURCE_TYPE,
                "quarterly-review.txt", "/data/connector", "text/plain",
                fromAFilesystem,
                false,
                Set.of("__Everyone__"), Set.of(),
                Map.of());

        Object stored = ingestPropertiesOf(node).get(ContentLakeIngestProperties.SOURCE_MODIFIED_AT);

        assertThat(stored).isEqualTo("2026-09-17T09:23:25.023986867Z");
        assertThat(OffsetDateTime.parse(stored.toString())).isEqualTo(fromAFilesystem);
        assertThat(fromAFilesystem.isAfter(OffsetDateTime.parse(stored.toString()))).isFalse();
    }

    /** Fixed width, so two timestamps order the same way as text and as instants. */
    @Test
    void theStoredFormIsFixedWidthSoItSortsAsText() {
        String earlier = storedTimestampOf(OffsetDateTime.parse("2026-09-17T10:00:00Z"));
        String later = storedTimestampOf(OffsetDateTime.parse("2026-09-17T10:00:00.5Z"));

        // The range predicates behind modifiedAfter / modifiedBefore compare this value as text.
        assertThat(earlier).hasSameSizeAs(later);
        assertThat(earlier.compareTo(later)).isNegative();
    }

    private String storedTimestampOf(OffsetDateTime modifiedAt) {
        SourceNode node = new SourceNode(
                NODE_ID, SOURCE_ID, SOURCE_TYPE,
                "quarterly-review.txt", "/data/connector", "text/plain",
                modifiedAt, false, Set.of("__Everyone__"), Set.of(), Map.of());
        return String.valueOf(ingestPropertiesOf(node).get(ContentLakeIngestProperties.SOURCE_MODIFIED_AT));
    }

    /** A null timestamp is absent rather than stored as a null, as every other property already is. */
    @Test
    void aNodeWithNoTimestampStoresNoTimestampKey() {
        SourceNode node = new SourceNode(
                NODE_ID, SOURCE_ID, SOURCE_TYPE,
                "quarterly-review.txt", "/data/connector", "text/plain",
                null,
                false,
                Set.of("__Everyone__"), Set.of(),
                Map.of());

        assertThat(ingestPropertiesOf(node))
                .doesNotContainKey(ContentLakeIngestProperties.SOURCE_MODIFIED_AT);
    }

    // ------------------------------------------------------------------
    // Fixtures
    // ------------------------------------------------------------------

    /** A node shaped like the sample connector's: the record populated, only namespaced properties set. */
    private static SourceNode connectorNode(Map<String, Object> sourceProperties) {
        return new SourceNode(
                NODE_ID, SOURCE_ID, SOURCE_TYPE,
                "quarterly-review.txt", "/data/connector", "text/plain",
                OffsetDateTime.parse("2026-09-17T10:00:00Z"),
                false,
                Set.of("__Everyone__"), Set.of(),
                sourceProperties);
    }

    private Map<String, Object> ingestPropertiesOf(SourceNode node) {
        return captureCreated(node).getCinIngestProperties();
    }

    /**
     * Syncs the node and returns the document handed to hxpr. Only the create path is exercised: the node
     * is new, so extraction never runs and no content stubs are needed.
     */
    private HxprDocument captureCreated(SourceNode node) {
        when(hxprService.findByNodeId(anyString(), anyString())).thenReturn(null);
        when(hxprService.findByPath(anyString())).thenReturn(null);
        when(hxprService.createDocument(anyString(), any(HxprDocument.class)))
                .thenAnswer(invocation -> {
                    HxprDocument submitted = invocation.getArgument(1);
                    submitted.setSysId("hxpr-1");
                    return submitted;
                });

        service.syncNode(node);

        ArgumentCaptor<HxprDocument> captor = ArgumentCaptor.forClass(HxprDocument.class);
        // atLeastOnce plus the last value, so a test may sync more than one node through this helper.
        verify(hxprService, atLeastOnce()).createDocument(any(), captor.capture());

        List<HxprDocument> submitted = captor.getAllValues();
        HxprDocument created = submitted.get(submitted.size() - 1);
        // Defensive copies, so an assertion cannot be satisfied by a later pipeline mutation.
        HxprDocument snapshot = new HxprDocument();
        snapshot.setCinIngestProperties(created.getCinIngestProperties());
        snapshot.setCinIngestPropertyNames(created.getCinIngestPropertyNames() == null
                ? null : new ArrayList<>(created.getCinIngestPropertyNames()));
        return snapshot;
    }
}
