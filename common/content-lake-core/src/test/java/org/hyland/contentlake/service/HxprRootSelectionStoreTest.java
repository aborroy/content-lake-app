package org.hyland.contentlake.service;

import org.hyland.contentlake.client.HxprDocumentApi;
import org.hyland.contentlake.client.HxprService;
import org.hyland.contentlake.model.HxprDocument;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * The hxpr-backed selection store, and above all the shape of the document it writes.
 *
 * <p>Same reasoning as the cursor store's test: the document lives in the index a reconciliation sweep deletes
 * from, so the assertions about what it does <em>not</em> carry are the load-bearing ones. Nothing in the type
 * system stops a later change from adding {@code cin_sourceId} and making a sync delete the document that
 * defines its own scope.</p>
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class HxprRootSelectionStoreTest extends RootSelectionStoreContractTest {

    private static final String FOLDER = "/content-lake/_state/roots";
    private static final String PATH = FOLDER + "/roots-sharepoint-instance-1";

    @Mock
    private HxprService hxprService;
    @Mock
    private HxprDocumentApi documentApi;

    private HxprRootSelectionStore store;

    /** Stands in for the index: what {@code findByPath} returns, keyed by path. */
    private final Map<String, HxprDocument> index = new LinkedHashMap<>();

    @BeforeEach
    void setUp() {
        store = new HxprRootSelectionStore(hxprService, documentApi, FOLDER);
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
    RootSelectionStore store() {
        return store;
    }

    private HxprDocument created() {
        store.save(SOURCE, RootSelection.of(List.of("b!drive:folder-a"), "admin"));
        return index.get(PATH);
    }

    @Nested
    class DocumentShape {

        @Test
        void namesTheDocumentAfterTheSourceWithoutTheColon() {
            assertThat(store.documentPath(SOURCE)).isEqualTo(PATH);
        }

        /**
         * The cursor store uses the same key and the same folder shape, so a shared prefix would put both at
         * one path and the later write would destroy the earlier.
         */
        @Test
        void usesAPrefixThatCannotCollideWithACursorForTheSameSource() {
            assertThat(HxprRootSelectionStore.documentName(SOURCE))
                    .isNotEqualTo(HxprSyncCursorStore.documentName(SOURCE));
        }

        @Test
        void createsTheFolderBeforeTheFirstDocument() {
            store.save(SOURCE, RootSelection.of(List.of("root-a"), "admin"));

            verify(hxprService).ensureFolder(FOLDER);
            verify(hxprService).createDocument(eq(FOLDER), any());
        }

        /** Without {@code cin_sourceId} the sweep's source scan cannot visit the selection at all. */
        @Test
        void carriesNoSourceId() {
            assertThat(created().getCinSourceId()).isNull();
        }

        /** Without {@code cin_paths} the sweep's in-scope predicate cannot match the selection. */
        @Test
        void carriesNoPaths() {
            assertThat(created().getCinPaths()).isNull();
        }

        /** Without {@code cin_id} an unqualified lookup cannot mistake it for a source node. */
        @Test
        void carriesNoNodeId() {
            assertThat(created().getCinId()).isNull();
        }

        @Test
        void declaresTheMixinThatOwnsTheFieldsItUses() {
            assertThat(created().getSysMixinTypes()).containsExactly(HxprDocument.MIXIN_CIN_REMOTE);
        }

        @Test
        void mirrorsThePropertyNamesOntoTheKeySet() {
            // cin_ingestPropertyNames must always mirror cin_ingestProperties.keySet().
            HxprDocument document = created();
            assertThat(document.getCinIngestPropertyNames())
                    .containsExactlyInAnyOrderElementsOf(document.getCinIngestProperties().keySet());
        }
    }

    @Nested
    class Reading {

        @Test
        void ignoresADocumentThatBelongsToAnotherSource() {
            // The document name is a lossy transform of the source id, so two ids can resolve to one path.
            // Reading the wrong source's scope would walk the wrong subtree.
            store.save(SOURCE, RootSelection.of(List.of("root-a"), "admin"));
            index.get(PATH).getCinIngestProperties()
                    .put(HxprRootSelectionStore.PROP_SOURCE, "sharepoint:a-different-instance");

            assertThat(store.load(SOURCE)).isEmpty();
        }

        @Test
        void readsAnEmptySelectionAsPresentRatherThanAbsent() {
            store.save(SOURCE, RootSelection.of(List.of(), "admin"));

            assertThat(store.load(SOURCE)).isPresent();
            assertThat(store.load(SOURCE)).get().extracting(RootSelection::isEmpty).isEqualTo(true);
        }

        @Test
        void survivesATimestampItCannotParse() {
            // Losing the audit detail is acceptable; losing the selection because of it is not.
            store.save(SOURCE, RootSelection.of(List.of("root-a"), "admin"));
            index.get(PATH).getCinIngestProperties()
                    .put(HxprRootSelectionStore.PROP_UPDATED_AT, "the day before yesterday");

            assertThat(store.load(SOURCE)).get().satisfies(selection -> {
                assertThat(selection.rootNodeIds()).containsExactly("root-a");
                assertThat(selection.updatedAt()).isNull();
            });
        }
    }
}
