package org.hyland.contentlake.rag.service;

import org.hyland.contentlake.rag.model.SemanticSearchResponse.SearchHit;
import org.hyland.contentlake.rag.model.SemanticSearchResponse.SourceDocument;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * One document must not consume a whole result set.
 *
 * <p>Measured on the E2E fixture corpus, a {@code topK} of 10 came back as 10 chunks of only 2
 * documents: the winner took slots 1 to 7 with all of its chunks. A document that ranks first on its own
 * merits was therefore absent from a query quoting it nearly verbatim, which to a caller is
 * indistinguishable from it not being indexed, and it is why the same handful of short E2E fixtures
 * failed "NOT found after 180s" while being fully embedded.</p>
 */
class DocumentDiversityLimiterTest {

    /** The measured failure: one document's chunks fill the budget and crowd every other one out. */
    @Test
    void aDocumentCannotTakeMoreThanItsShareOfTheBudget() {
        List<SearchHit> hits = new ArrayList<>();
        hits.addAll(chunksOf("long-report", 7, 0.90));
        hits.addAll(chunksOf("short-memo", 3, 0.60));

        List<SearchHit> limited = DocumentDiversityLimiter.limit(hits, 5, 3);

        assertThat(documentsOf(limited)).containsExactly(
                "long-report", "long-report", "long-report", "short-memo", "short-memo");
    }

    /** Deferred, not dropped: the cap reorders a result set and never shortens one. */
    @Test
    void hitsOverTheCapAreBackfilledRatherThanDiscarded() {
        List<SearchHit> hits = chunksOf("only-document", 6, 0.90);

        List<SearchHit> limited = DocumentDiversityLimiter.limit(hits, 5, 2);

        assertThat(limited).hasSize(5);
        assertThat(documentsOf(limited)).containsOnly("only-document");
    }

    /**
     * The backfill has to come after the other documents' hits, or capping would achieve nothing: the
     * whole point is that a lower-scoring document is preferred over an eighth chunk of the winner.
     */
    @Test
    void theBackfillRunsBehindEveryCappedDocument() {
        List<SearchHit> hits = new ArrayList<>();
        hits.addAll(chunksOf("long-report", 4, 0.90));
        hits.addAll(chunksOf("short-memo", 1, 0.50));

        List<SearchHit> limited = DocumentDiversityLimiter.limit(hits, 4, 2);

        assertThat(documentsOf(limited))
                .containsExactly("long-report", "long-report", "short-memo", "long-report");
    }

    /** Off by default, so the ordering has to be untouched when the cap is disabled. */
    @Test
    void aDisabledCapOnlyTrims() {
        List<SearchHit> hits = new ArrayList<>();
        hits.addAll(chunksOf("long-report", 4, 0.90));
        hits.addAll(chunksOf("short-memo", 2, 0.50));

        List<SearchHit> limited = DocumentDiversityLimiter.limit(hits, 3, 0);

        assertThat(documentsOf(limited)).containsExactly("long-report", "long-report", "long-report");
    }

    /** Scores are the caller's threshold and display value, so the cap must not rewrite them. */
    @Test
    void everyHitKeepsItsOwnScoreAndGetsAFreshRank() {
        List<SearchHit> hits = new ArrayList<>();
        hits.addAll(chunksOf("long-report", 3, 0.90));
        hits.addAll(chunksOf("short-memo", 1, 0.42));

        List<SearchHit> limited = DocumentDiversityLimiter.limit(hits, 3, 2);

        assertThat(limited).extracting(SearchHit::getRank).containsExactly(1, 2, 3);
        assertThat(limited.get(2).getScore()).isEqualTo(0.42);
    }

    /**
     * Hits with no document identity of any kind must not be pooled into one bucket: capping them
     * against each other would drop real results from the capped pass on a corpus where the id is
     * simply absent.
     */
    @Test
    void unidentifiableHitsAreNotCappedAgainstEachOther() {
        List<SearchHit> hits = List.of(
                SearchHit.builder().score(0.9).build(),
                SearchHit.builder().score(0.8).build(),
                SearchHit.builder().score(0.7).build());

        List<SearchHit> limited = DocumentDiversityLimiter.limit(hits, 3, 1);

        assertThat(limited).hasSize(3);
        assertThat(limited).extracting(SearchHit::getScore).containsExactly(0.9, 0.8, 0.7);
    }

    @Test
    void anEmptyOrNullResultSetIsHandled() {
        assertThat(DocumentDiversityLimiter.limit(null, 10, 3)).isEmpty();
        assertThat(DocumentDiversityLimiter.limit(List.of(), 10, 3)).isEmpty();
    }

    /** A limit past the available hits returns everything, capped or not. */
    @Test
    void aLimitLargerThanTheResultSetReturnsAllOfIt() {
        List<SearchHit> hits = new ArrayList<>();
        hits.addAll(chunksOf("long-report", 3, 0.90));
        hits.addAll(chunksOf("short-memo", 2, 0.50));

        assertThat(DocumentDiversityLimiter.limit(hits, 50, 1)).hasSize(5);
    }

    // ------------------------------------------------------------------

    /** {@code count} chunks of one document, scored descending from {@code topScore}. */
    private static List<SearchHit> chunksOf(String documentId, int count, double topScore) {
        List<SearchHit> hits = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            hits.add(SearchHit.builder()
                    .score(topScore - (i * 0.01))
                    .chunkText(documentId + " chunk " + i)
                    .sourceDocument(SourceDocument.builder()
                            .documentId(documentId)
                            .name(documentId + ".txt")
                            .build())
                    .build());
        }
        return hits;
    }

    private static List<String> documentsOf(List<SearchHit> hits) {
        return hits.stream().map(hit -> hit.getSourceDocument().getDocumentId()).toList();
    }
}
