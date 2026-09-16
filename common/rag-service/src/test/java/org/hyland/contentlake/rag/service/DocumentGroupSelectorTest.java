package org.hyland.contentlake.rag.service;

import org.hyland.contentlake.rag.model.SemanticSearchResponse.SearchHit;
import org.hyland.contentlake.rag.model.SemanticSearchResponse.SourceDocument;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A caller asking for a number of documents must get that many documents (#135).
 *
 * <p>The invariants here are the contract {@code topDocuments} rests on, and the one that separates this
 * from {@link DocumentDiversityLimiter} is the absence of a backfill: the sibling may only reorder a
 * result set, while this one may legitimately return fewer chunks than the budget allows.</p>
 */
class DocumentGroupSelectorTest {

    @Nested
    class DocumentBudget {

        /** The point of the feature: ten chunks of two documents becomes chunks of ten documents. */
        @Test
        void selectsExactlyTheRequestedNumberOfDocuments() {
            List<SearchHit> hits = new ArrayList<>();
            for (int i = 0; i < 10; i++) {
                hits.addAll(chunksOf("doc-" + i, 4, 0.90 - (i * 0.01)));
            }

            List<SearchHit> selected = DocumentGroupSelector.select(hits, 10, 1);

            assertThat(distinctDocumentsOf(selected)).hasSize(10);
            assertThat(selected).hasSize(10);
        }

        @Test
        void takesAtMostTheAllowedChunksFromEachDocument() {
            List<SearchHit> hits = new ArrayList<>();
            hits.addAll(chunksOf("long-report", 7, 0.90));
            hits.addAll(chunksOf("short-memo", 3, 0.60));

            List<SearchHit> selected = DocumentGroupSelector.select(hits, 2, 2);

            assertThat(documentsOf(selected))
                    .containsExactly("long-report", "long-report", "short-memo", "short-memo");
        }

        /**
         * A document past the budget must not be admitted even when a selected document has chunks to
         * spare. Its slot is gone, not merely its share.
         */
        @Test
        void aDocumentPastTheBudgetIsNotAdmittedToFillLeftoverRoom() {
            List<SearchHit> hits = new ArrayList<>();
            hits.addAll(chunksOf("first", 1, 0.90));
            hits.addAll(chunksOf("second", 1, 0.80));
            hits.addAll(chunksOf("third", 1, 0.70));

            List<SearchHit> selected = DocumentGroupSelector.select(hits, 2, 5);

            assertThat(documentsOf(selected)).containsExactly("first", "second");
        }

        /**
         * The walk must not stop at the document budget, or later chunks of already-selected documents
         * would be lost to whichever document happened to appear between them.
         */
        @Test
        void laterChunksOfSelectedDocumentsSurviveAnInterveningUnselectedDocument() {
            List<SearchHit> hits = new ArrayList<>();
            hits.add(chunk("kept", 0, 0.90));
            hits.add(chunk("dropped", 0, 0.80));
            hits.add(chunk("kept", 1, 0.70));

            List<SearchHit> selected = DocumentGroupSelector.select(hits, 1, 5);

            assertThat(documentsOf(selected)).containsExactly("kept", "kept");
            assertThat(selected).extracting(SearchHit::getChunkText)
                    .containsExactly("kept chunk 0", "kept chunk 1");
        }

        /** Non-positive budgets mean "no limit", matching the sibling's convention. */
        @Test
        void nonPositiveBudgetsImposeNoLimit() {
            List<SearchHit> hits = new ArrayList<>();
            hits.addAll(chunksOf("a", 3, 0.90));
            hits.addAll(chunksOf("b", 2, 0.50));

            assertThat(DocumentGroupSelector.select(hits, 0, 0)).hasSize(5);
            assertThat(DocumentGroupSelector.select(hits, -1, 2)).hasSize(4);
            assertThat(DocumentGroupSelector.select(hits, 1, 0)).hasSize(3);
        }

        /** Asking for more documents than the corpus holds returns what there is. */
        @Test
        void aBudgetLargerThanTheAvailableDocumentsReturnsAllOfThem() {
            List<SearchHit> hits = new ArrayList<>();
            hits.addAll(chunksOf("a", 2, 0.90));
            hits.addAll(chunksOf("b", 2, 0.50));

            List<SearchHit> selected = DocumentGroupSelector.select(hits, 50, 2);

            assertThat(distinctDocumentsOf(selected)).hasSize(2);
            assertThat(selected).hasSize(4);
        }
    }

    @Nested
    class NoBackfill {

        /**
         * The difference from {@link DocumentDiversityLimiter}, and the reason the response reports
         * {@code documentCount}: a short answer here is correct, not a shortfall.
         */
        @Test
        void aSelectionMayBeShorterThanTheChunkBudgetAllows() {
            List<SearchHit> hits = chunksOf("only-document", 6, 0.90);

            List<SearchHit> selected = DocumentGroupSelector.select(hits, 5, 2);

            assertThat(selected).hasSize(2);
            assertThat(distinctDocumentsOf(selected)).containsExactly("only-document");
        }

        /** Contrast asserted directly, so nobody "fixes" this by reusing the sibling. */
        @Test
        void theSiblingBackfillsWhereThisDoesNot() {
            List<SearchHit> hits = chunksOf("only-document", 6, 0.90);

            assertThat(DocumentDiversityLimiter.limit(hits, 5, 2)).hasSize(5);
            assertThat(DocumentGroupSelector.select(hits, 5, 2)).hasSize(2);
        }
    }

    @Nested
    class Grouping {

        /** A document sits where its best chunk ranked, and its chunks arrive together. */
        @Test
        void chunksOfOneDocumentAreContiguousAtItsBestChunksPosition() {
            List<SearchHit> hits = new ArrayList<>();
            hits.add(chunk("a", 0, 0.90));
            hits.add(chunk("b", 0, 0.85));
            hits.add(chunk("a", 1, 0.80));
            hits.add(chunk("c", 0, 0.75));
            hits.add(chunk("b", 1, 0.70));

            List<SearchHit> selected = DocumentGroupSelector.select(hits, 3, 2);

            assertThat(documentsOf(selected)).containsExactly("a", "a", "b", "b", "c");
        }

        @Test
        void chunksKeepTheirRelativeOrderWithinADocument() {
            List<SearchHit> hits = new ArrayList<>();
            hits.add(chunk("a", 0, 0.90));
            hits.add(chunk("b", 0, 0.85));
            hits.add(chunk("a", 1, 0.80));
            hits.add(chunk("a", 2, 0.60));

            List<SearchHit> selected = DocumentGroupSelector.select(hits, 2, 3);

            assertThat(selected).extracting(SearchHit::getChunkText)
                    .containsExactly("a chunk 0", "a chunk 1", "a chunk 2", "b chunk 0");
        }

        /**
         * Grouping means the score column is no longer descending. Asserted rather than left implicit,
         * because a consumer that assumed monotonic scores would be quietly wrong.
         */
        @Test
        void groupingBreaksScoreMonotonicityByDesign() {
            List<SearchHit> hits = new ArrayList<>();
            hits.add(chunk("a", 0, 0.90));
            hits.add(chunk("b", 0, 0.85));
            hits.add(chunk("a", 1, 0.10));

            List<SearchHit> selected = DocumentGroupSelector.select(hits, 2, 2);

            assertThat(selected).extracting(SearchHit::getScore).containsExactly(0.90, 0.10, 0.85);
        }

        /** Ranks are reassigned over the grouped order; scores are never rewritten. */
        @Test
        void ranksAreRenumberedAndScoresPreserved() {
            List<SearchHit> hits = new ArrayList<>();
            hits.addAll(chunksOf("a", 2, 0.90));
            hits.addAll(chunksOf("b", 1, 0.42));

            List<SearchHit> selected = DocumentGroupSelector.select(hits, 2, 2);

            assertThat(selected).extracting(SearchHit::getRank).containsExactly(1, 2, 3);
            assertThat(selected.get(2).getScore()).isEqualTo(0.42);
        }
    }

    @Nested
    class Keying {

        /**
         * A null key is dropped rather than pooled. Pooling would let unrelated hits spend one document
         * slot and one chunk allowance between them, costing a real document its place.
         */
        @Test
        void aHitWithNoKeyIsDroppedRatherThanPooled() {
            List<SearchHit> hits = new ArrayList<>();
            hits.add(chunk("a", 0, 0.90));
            hits.add(chunk("b", 0, 0.80));

            List<SearchHit> selected = DocumentGroupSelector.selectGroups(
                    hits, 2, 2, hit -> "a".equals(hit.getSourceDocument().getDocumentId()) ? null : "b");

            assertThat(documentsOf(selected)).containsExactly("b");
        }

        /**
         * Hits with no identity of any kind still count as separate documents, because the shared key
         * function gives each one its own. Selecting them as one document would be the pooling bug.
         */
        @Test
        void unidentifiableHitsCountAsSeparateDocuments() {
            List<SearchHit> hits = List.of(
                    SearchHit.builder().score(0.9).build(),
                    SearchHit.builder().score(0.8).build(),
                    SearchHit.builder().score(0.7).build());

            assertThat(DocumentGroupSelector.select(hits, 3, 1)).hasSize(3);
            assertThat(DocumentGroupSelector.select(hits, 2, 1)).hasSize(2);
        }

        /** The key must be the sibling's, or the two would count different things as one document. */
        @Test
        void theDocumentKeyIsSharedWithTheDiversityLimiter() {
            SearchHit byNodeId = SearchHit.builder()
                    .score(0.9)
                    .sourceDocument(SourceDocument.builder().nodeId("node-1").build())
                    .build();
            SearchHit sameNodeId = SearchHit.builder()
                    .score(0.8)
                    .sourceDocument(SourceDocument.builder().nodeId("node-1").build())
                    .build();

            assertThat(DocumentDiversityLimiter.documentKey(byNodeId))
                    .isEqualTo(DocumentDiversityLimiter.documentKey(sameNodeId));
            assertThat(DocumentGroupSelector.select(List.of(byNodeId, sameNodeId), 1, 2)).hasSize(2);
        }

        @Test
        void aNullEntryInThePoolIsSkipped() {
            List<SearchHit> hits = new ArrayList<>();
            hits.add(chunk("a", 0, 0.90));
            hits.add(null);
            hits.add(chunk("b", 0, 0.80));

            assertThat(DocumentGroupSelector.select(hits, 5, 5)).hasSize(2);
        }

        @Test
        void anEmptyOrNullPoolIsHandled() {
            assertThat(DocumentGroupSelector.select(null, 10, 3)).isEmpty();
            assertThat(DocumentGroupSelector.select(List.of(), 10, 3)).isEmpty();
        }
    }

    @Nested
    class DocumentCount {

        @Test
        void countsDistinctDocumentsRatherThanChunks() {
            List<SearchHit> hits = new ArrayList<>();
            hits.addAll(chunksOf("a", 3, 0.90));
            hits.addAll(chunksOf("b", 2, 0.50));

            assertThat(DocumentGroupSelector.documentCount(hits)).isEqualTo(2);
        }

        @Test
        void anEmptyOrNullResultSetCountsZero() {
            assertThat(DocumentGroupSelector.documentCount(null)).isZero();
            assertThat(DocumentGroupSelector.documentCount(List.of())).isZero();
        }

        @Test
        void hitsWithNoKeyDoNotCount() {
            List<SearchHit> hits = List.of(chunk("a", 0, 0.9), chunk("b", 0, 0.8));

            assertThat(DocumentGroupSelector.distinctGroups(hits, hit -> null)).isZero();
        }
    }

    // ------------------------------------------------------------------

    /** {@code count} chunks of one document, scored descending from {@code topScore}. */
    private static List<SearchHit> chunksOf(String documentId, int count, double topScore) {
        List<SearchHit> hits = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            hits.add(chunk(documentId, i, topScore - (i * 0.01)));
        }
        return hits;
    }

    private static SearchHit chunk(String documentId, int index, double score) {
        return SearchHit.builder()
                .score(score)
                .chunkText(documentId + " chunk " + index)
                .sourceDocument(SourceDocument.builder()
                        .documentId(documentId)
                        .name(documentId + ".txt")
                        .build())
                .build();
    }

    private static List<String> documentsOf(List<SearchHit> hits) {
        return hits.stream().map(hit -> hit.getSourceDocument().getDocumentId()).toList();
    }

    private static List<String> distinctDocumentsOf(List<SearchHit> hits) {
        return documentsOf(hits).stream().distinct().toList();
    }
}
