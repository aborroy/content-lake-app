package org.hyland.contentlake.service;

import org.hyland.contentlake.client.HxprDocumentApi;
import org.hyland.contentlake.client.HxprService;
import org.hyland.contentlake.model.HxprDocument;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.time.OffsetDateTime;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * The hxpr-backed cursor store, and above all the shape of the document it writes.
 *
 * <p>The state document lives in the same index as the documents a reconciliation sweep deletes, so the
 * assertions about what it does <em>not</em> carry are the load-bearing ones: nothing in the type system
 * stops a later change from adding {@code cin_sourceId} and making a sync delete its own cursor.</p>
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class HxprSyncCursorStoreTest extends SyncCursorStoreContractTest {

    private static final String FOLDER = "/content-lake/_state/cursors";
    private static final String PATH = FOLDER + "/cursor-sample-instance-1";

    @Mock
    private HxprService hxprService;
    @Mock
    private HxprDocumentApi documentApi;

    private HxprSyncCursorStore store;

    /** Stands in for the index: what {@code findByPath} returns, keyed by path. */
    private final Map<String, HxprDocument> index = new LinkedHashMap<>();

    @BeforeEach
    void setUp() {
        store = new HxprSyncCursorStore(hxprService, documentApi, FOLDER);
        when(hxprService.findByPath(anyString())).thenAnswer(call -> index.get(call.getArgument(0)));
        when(hxprService.createDocument(anyString(), any())).thenAnswer(call -> {
            HxprDocument document = call.getArgument(1);
            document.setSysId("doc-" + document.getSysName());
            index.put(FOLDER + "/" + document.getSysName(), document);
            return document;
        });
        when(documentApi.updateById(anyString(), any())).thenAnswer(call -> {
            HxprDocument update = (HxprDocument) call.getArgument(1);
            index.values().stream()
                    .filter(stored -> call.getArgument(0).equals(stored.getSysId()))
                    .forEach(stored -> {
                        stored.setCinIngestProperties(update.getCinIngestProperties());
                        stored.setCinIngestPropertyNames(update.getCinIngestPropertyNames());
                    });
            return update;
        });
        doAnswer(call -> {
            index.values().removeIf(stored -> call.getArgument(0).equals(stored.getSysId()));
            return null;
        }).when(documentApi).deleteById(anyString());
    }

    @Override
    SyncCursorStore store() {
        return store;
    }

    @Nested
    class DocumentShape {

        @Test
        void namesTheDocumentAfterTheSourceWithoutTheColon() {
            assertThat(store.documentPath(SOURCE)).isEqualTo(PATH);
        }

        @Test
        void createsTheFolderBeforeTheFirstDocument() {
            store.save(SOURCE, SyncCursor.seeded("token-1"));

            verify(hxprService).ensureFolder(FOLDER);
            verify(hxprService).createDocument(eq(FOLDER), any());
        }

        /** Without {@code cin_sourceId} the sweep's source scan cannot visit the cursor at all. */
        @Test
        void carriesNoSourceId() {
            assertThat(created().getCinSourceId()).isNull();
        }

        /** Without {@code cin_paths} the sweep's in-scope predicate cannot match the cursor. */
        @Test
        void carriesNoPaths() {
            assertThat(created().getCinPaths()).isNull();
        }

        /** Without {@code cin_id} an unqualified node lookup cannot mistake the cursor for a source node. */
        @Test
        void carriesNoNodeId() {
            assertThat(created().getCinId()).isNull();
        }

        /** No embeddings means no chunk, and both retrieval legs read chunks, so no query can return it. */
        @Test
        void carriesNoEmbeddings() {
            assertThat(created().getSysembedEmbeddings()).isNull();
        }

        /** hxpr rejects any {@code cin_*} field on a document without this mixin. */
        @Test
        void declaresTheMixinThatOwnsTheFieldsItUses() {
            assertThat(created().getSysMixinTypes()).containsExactly(HxprDocument.MIXIN_CIN_REMOTE);
        }

        @Test
        void mirrorsThePropertyNames() {
            HxprDocument document = created();

            assertThat(document.getCinIngestPropertyNames())
                    .containsExactlyInAnyOrderElementsOf(document.getCinIngestProperties().keySet());
        }

        @Test
        void storesTheCursorValueTimestampAndGeneration() {
            store.save(SOURCE, new SyncCursor("token-9", OffsetDateTime.parse("2026-09-16T10:00:00Z"), 4L));

            assertThat(captureCreated().getCinIngestProperties())
                    .containsEntry(HxprSyncCursorStore.PROP_SOURCE, SOURCE)
                    .containsEntry(HxprSyncCursorStore.PROP_VALUE, "token-9")
                    .containsEntry(HxprSyncCursorStore.PROP_UPDATED_AT, "2026-09-16T10:00Z")
                    .containsEntry(HxprSyncCursorStore.PROP_GENERATION, 4L);
        }

        private HxprDocument created() {
            store.save(SOURCE, SyncCursor.seeded("token-1"));
            return captureCreated();
        }

        private HxprDocument captureCreated() {
            ArgumentCaptor<HxprDocument> captor = ArgumentCaptor.forClass(HxprDocument.class);
            verify(hxprService).createDocument(anyString(), captor.capture());
            return captor.getValue();
        }
    }

    @Nested
    class Updates {

        /** A second save updates the document in place rather than creating a sibling. */
        @Test
        void advancesTheExistingDocument() {
            store.save(SOURCE, SyncCursor.seeded("token-1"));
            store.save(SOURCE, SyncCursor.seeded("token-1").advancedTo("token-2"));

            verify(hxprService).createDocument(anyString(), any());
            verify(documentApi).updateById(anyString(), any());
            assertThat(store.load(SOURCE)).map(SyncCursor::value).contains("token-2");
        }

        @Test
        void clearingDeletesTheDocument() {
            store.save(SOURCE, SyncCursor.seeded("token-1"));
            store.clear(SOURCE);

            verify(documentApi).deleteById("doc-cursor-sample-instance-1");
        }

        @Test
        void clearingAnAbsentDocumentDeletesNothing() {
            store.clear(SOURCE);

            verify(documentApi, never()).deleteById(anyString());
        }
    }

    @Nested
    class Reading {

        @Test
        void readsAGenerationHxprReturnedAsAnInteger() {
            index.put(PATH, stateDocument(Map.of(
                    HxprSyncCursorStore.PROP_SOURCE, SOURCE,
                    HxprSyncCursorStore.PROP_VALUE, "token-1",
                    HxprSyncCursorStore.PROP_GENERATION, 3)));

            assertThat(store.load(SOURCE)).map(SyncCursor::generation).contains(3L);
        }

        /** The timestamp is diagnostic, so an unparseable one must not cost the caller its cursor. */
        @Test
        void keepsTheCursorWhenTheTimestampIsUnreadable() {
            index.put(PATH, stateDocument(Map.of(
                    HxprSyncCursorStore.PROP_VALUE, "token-1",
                    HxprSyncCursorStore.PROP_UPDATED_AT, "not a date")));

            assertThat(store.load(SOURCE)).hasValueSatisfying(cursor -> {
                assertThat(cursor.value()).isEqualTo("token-1");
                assertThat(cursor.updatedAt()).isNull();
            });
        }

        @Test
        void ignoresADocumentWithNoStoredValue() {
            index.put(PATH, stateDocument(Map.of(HxprSyncCursorStore.PROP_SOURCE, SOURCE)));

            assertThat(store.load(SOURCE)).isEmpty();
        }

        /**
         * Two source ids differing only in a character the document name cannot carry resolve to the same
         * path. Resuming one from the other's cursor would skip a window without saying so, so a
         * mismatch means a full walk.
         */
        @Test
        void refusesACursorBelongingToAnotherSource() {
            index.put(PATH, stateDocument(Map.of(
                    HxprSyncCursorStore.PROP_SOURCE, "sample-instance-1",
                    HxprSyncCursorStore.PROP_VALUE, "token-1")));

            assertThat(store.load(SOURCE)).isEmpty();
        }

        private HxprDocument stateDocument(Map<String, Object> props) {
            HxprDocument document = new HxprDocument();
            document.setSysId("doc-1");
            document.setSysName("cursor-sample-instance-1");
            document.setSysMixinTypes(List.of(HxprDocument.MIXIN_CIN_REMOTE));
            document.setCinIngestProperties(props);
            return document;
        }
    }
}
