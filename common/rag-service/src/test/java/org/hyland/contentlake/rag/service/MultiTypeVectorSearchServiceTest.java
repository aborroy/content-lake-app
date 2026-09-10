package org.hyland.contentlake.rag.service;

import org.hyland.contentlake.client.EmbeddingTypeCatalog;
import org.hyland.contentlake.client.HxprService;
import org.hyland.contentlake.hxpr.api.model.Embedding;
import org.hyland.contentlake.hxpr.api.model.LocationModel;
import org.hyland.contentlake.hxpr.api.model.TextLocation;
import org.hyland.contentlake.hxpr.api.model.VectorSearchResult;
import org.hyland.contentlake.service.EmbeddingService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * The vector leg over several embedding types (#121).
 *
 * <p>Two things are being pinned here. That a single-type corpus keeps making exactly the call it
 * always made, wildcard and raw scores included, because that is the normal case and its ranking must
 * not move. And that when two types are present their scores are made comparable, so a retired model
 * cannot outrank a current one purely by reporting larger numbers.</p>
 */
@ExtendWith(MockitoExtension.class)
class MultiTypeVectorSearchServiceTest {

    private static final String PRIMARY_MODEL = "ai/mxbai-embed-large";
    private static final String PRIMARY_TYPE = "ai-mxbai-embed-large";
    private static final String LEGACY_MODEL = "ai/nomic-embed-text";
    private static final String LEGACY_TYPE = "ai-nomic-embed-text";

    private static final List<Double> PRIMARY_VECTOR = List.of(0.1d, 0.2d);
    private static final List<Double> LEGACY_VECTOR = List.of(0.9d, 0.8d);

    private static final String FILTER = "SELECT * FROM SysContent WHERE sys_racl = 'bob'";

    @Mock
    private HxprService hxprService;
    @Mock
    private EmbeddingTypeCatalog catalog;
    @Mock
    private EmbeddingService primaryEmbeddingService;
    @Mock
    private EmbeddingService legacyEmbeddingService;

    private MultiTypeVectorSearchService service(List<String> additionalModels) {
        when(primaryEmbeddingService.getModelName()).thenReturn(PRIMARY_MODEL);
        return new MultiTypeVectorSearchService(
                hxprService, catalog, primaryEmbeddingService, additionalModels);
    }

    private MultiTypeVectorSearchService serviceWithLegacyModel() {
        when(primaryEmbeddingService.forModel(LEGACY_MODEL)).thenReturn(legacyEmbeddingService);
        return service(List.of(LEGACY_MODEL));
    }

    private MultiTypeVectorSearchService.Request request() {
        return MultiTypeVectorSearchService.Request.of(
                PRIMARY_VECTOR, embedder -> LEGACY_VECTOR, null, FILTER, 10);
    }

    private static Embedding embedding(String docId, String chunkId, String type, double score) {
        Embedding embedding = new Embedding();
        embedding.setSysembedDocId(docId);
        embedding.setSysembedId(chunkId);
        embedding.setSysembedType(type);
        embedding.setSysembedScore(score);
        embedding.setSysembedText(chunkId + " text");
        return embedding;
    }

    /**
     * A row that also carries its chunk position, which is how the same chunk is recognised across
     * types. Real rows always have one; the chunk id does not survive a re-chunk, the position does.
     */
    private static Embedding embeddingAt(String docId, String chunkId, String type, double score,
                                         int paragraph) {
        Embedding embedding = embedding(docId, chunkId, type, score);
        TextLocation text = new TextLocation();
        text.setParagraph(paragraph);
        LocationModel location = new LocationModel();
        location.setText(text);
        embedding.setSysembedLocation(location);
        return embedding;
    }

    private static VectorSearchResult result(Embedding... embeddings) {
        VectorSearchResult result = new VectorSearchResult();
        result.setEmbeddings(new ArrayList<>(List.of(embeddings)));
        result.setTotalCount((long) embeddings.length);
        return result;
    }

    private static List<String> chunkIds(VectorSearchResult result) {
        return result.getEmbeddings().stream().map(Embedding::getSysembedId).toList();
    }

    // ------------------------------------------------------------------
    // Single type: unchanged behaviour
    // ------------------------------------------------------------------

    /**
     * The wildcard call, byte for byte. A single-model corpus must not see its ranking, its score
     * scale or its minScore calibration move because multi-type support exists.
     */
    @Test
    void aSingleTypeCorpusMakesTheWildcardCallWithRawScores() {
        when(catalog.activeTypes()).thenReturn(List.of(PRIMARY_TYPE));
        when(hxprService.vectorSearch(eq(PRIMARY_VECTOR), isNull(), eq(FILTER), eq(10)))
                .thenReturn(result(embedding("doc-1", "c1", PRIMARY_TYPE, 0.42d)));

        VectorSearchResult merged = service(List.of()).search(request());

        assertThat(merged.getEmbeddings()).hasSize(1);
        assertThat(merged.getEmbeddings().get(0).getSysembedScore())
                .as("raw score preserved: nothing to make comparable against")
                .isEqualTo(0.42d);
        verify(hxprService, never()).vectorSearch(any(), eq(PRIMARY_TYPE), any(), anyInt());
    }

    @Test
    void anEmptyCatalogueStillMakesTheWildcardCall() {
        when(catalog.activeTypes()).thenReturn(List.of());
        when(hxprService.vectorSearch(eq(PRIMARY_VECTOR), isNull(), eq(FILTER), eq(10)))
                .thenReturn(result(embedding("doc-1", "c1", PRIMARY_TYPE, 0.42d)));

        assertThat(service(List.of()).search(request()).getEmbeddings()).hasSize(1);
    }

    /** A caller that pinned a type is asking for that type only, which is how a migration is checked. */
    @Test
    void anExplicitRequestTypePinsASingleSearch() {
        when(hxprService.vectorSearch(eq(PRIMARY_VECTOR), eq(LEGACY_TYPE), eq(FILTER), eq(10)))
                .thenReturn(result(embedding("doc-1", "c1", LEGACY_TYPE, 0.42d)));

        MultiTypeVectorSearchService.Request pinned = MultiTypeVectorSearchService.Request.of(
                PRIMARY_VECTOR, embedder -> LEGACY_VECTOR, LEGACY_TYPE, FILTER, 10);

        assertThat(service(List.of()).search(pinned).getEmbeddings()).hasSize(1);
        verify(catalog, never()).activeTypes();
    }

    // ------------------------------------------------------------------
    // Two types
    // ------------------------------------------------------------------

    /** Each type is searched with its own model's vector, since a foreign vector means nothing. */
    @Test
    void eachTypeIsSearchedWithItsOwnQueryVector() {
        when(catalog.activeTypes()).thenReturn(List.of(PRIMARY_TYPE, LEGACY_TYPE));
        when(hxprService.vectorSearch(eq(PRIMARY_VECTOR), eq(PRIMARY_TYPE), eq(FILTER), eq(10)))
                .thenReturn(result(embedding("doc-1", "c1", PRIMARY_TYPE, 0.9d)));
        when(hxprService.vectorSearch(eq(LEGACY_VECTOR), eq(LEGACY_TYPE), eq(FILTER), eq(10)))
                .thenReturn(result(embedding("doc-2", "c2", LEGACY_TYPE, 0.9d)));

        VectorSearchResult merged = serviceWithLegacyModel().search(request());

        assertThat(chunkIds(merged)).containsExactlyInAnyOrder("c1", "c2");
        verify(hxprService).vectorSearch(eq(PRIMARY_VECTOR), eq(PRIMARY_TYPE), eq(FILTER), eq(10));
        verify(hxprService).vectorSearch(eq(LEGACY_VECTOR), eq(LEGACY_TYPE), eq(FILTER), eq(10));
    }

    /**
     * Scores cross type boundaries untouched, because every type's vectors sit in the same index field
     * and are therefore scored by the same similarity metric.
     *
     * <p>The number the caller sees has to be the number the ranking used, or the ranking and the
     * reported scores disagree, and {@code minScore} is applied to something the ordering never
     * considered.</p>
     */
    @Test
    void scoresCrossTypeBoundariesUnchanged() {
        when(catalog.activeTypes()).thenReturn(List.of(PRIMARY_TYPE, LEGACY_TYPE));
        when(hxprService.vectorSearch(eq(PRIMARY_VECTOR), eq(PRIMARY_TYPE), eq(FILTER), eq(10)))
                .thenReturn(result(
                        embedding("doc-1", "primary-best", PRIMARY_TYPE, 0.82d),
                        embedding("doc-2", "primary-weak", PRIMARY_TYPE, 0.41d)));
        when(hxprService.vectorSearch(eq(LEGACY_VECTOR), eq(LEGACY_TYPE), eq(FILTER), eq(10)))
                .thenReturn(result(
                        embedding("doc-3", "legacy-mid", LEGACY_TYPE, 0.60d),
                        embedding("doc-4", "legacy-weak", LEGACY_TYPE, 0.20d)));

        VectorSearchResult merged = serviceWithLegacyModel().search(request());

        assertThat(chunkIds(merged))
                .as("ordered by score across both types, not interleaved by type")
                .containsExactly("primary-best", "legacy-mid", "primary-weak", "legacy-weak");
        assertThat(merged.getEmbeddings().stream().map(Embedding::getSysembedScore))
                .as("reported exactly as hxpr scored them")
                .containsExactly(0.82d, 0.60d, 0.41d, 0.20d);
    }

    /**
     * The realistic mid-migration shape: the same chunk under two types, with a different chunk id
     * under each because the pipeline re-chunked, and the same position under both.
     *
     * <p>Keying identity on the chunk id instead of the position let both rows through, and a live
     * two-type corpus then returned a top ten that was five chunks repeated twice.</p>
     */
    @Test
    void theSameChunkUnderTwoTypesCollapsesEvenWhenItsChunkIdDiffers() {
        when(catalog.activeTypes()).thenReturn(List.of(PRIMARY_TYPE, LEGACY_TYPE));
        when(hxprService.vectorSearch(eq(PRIMARY_VECTOR), eq(PRIMARY_TYPE), eq(FILTER), eq(10)))
                .thenReturn(result(
                        embeddingAt("doc-1", "new-uuid-a", PRIMARY_TYPE, 0.61d, 0),
                        embeddingAt("doc-1", "new-uuid-b", PRIMARY_TYPE, 0.55d, 1)));
        when(hxprService.vectorSearch(eq(LEGACY_VECTOR), eq(LEGACY_TYPE), eq(FILTER), eq(10)))
                .thenReturn(result(
                        embeddingAt("doc-1", "old-uuid-a", LEGACY_TYPE, 0.60d, 0),
                        embeddingAt("doc-1", "old-uuid-b", LEGACY_TYPE, 0.58d, 1)));

        VectorSearchResult merged = serviceWithLegacyModel().search(request());

        assertThat(merged.getEmbeddings())
                .as("two chunks, not four")
                .hasSize(2);
        assertThat(merged.getEmbeddings().stream().map(Embedding::getSysembedScore))
                .as("each position kept at whichever type scored it higher")
                .containsExactly(0.61d, 0.58d);
    }

    /**
     * The defect this replaced: dividing each type's candidates by that type's own best mapped every
     * active type's top hit to 1.0, so a retired type's weak best was promoted to tie the current type's
     * strong best, the merged head became one hit per type regardless of relevance, and {@code minScore}
     * could no longer filter any type's best hit. Rank-based fusion across types would do the same, since
     * it also gives every type's first place an identical contribution.
     */
    @Test
    void aTypeWhoseBestHitIsWeakIsNotPromotedToTheHead() {
        when(catalog.activeTypes()).thenReturn(List.of(PRIMARY_TYPE, LEGACY_TYPE));
        when(hxprService.vectorSearch(eq(PRIMARY_VECTOR), eq(PRIMARY_TYPE), eq(FILTER), eq(10)))
                .thenReturn(result(
                        embedding("doc-1", "primary-best", PRIMARY_TYPE, 0.90d),
                        embedding("doc-2", "primary-second", PRIMARY_TYPE, 0.85d),
                        embedding("doc-3", "primary-third", PRIMARY_TYPE, 0.80d)));
        when(hxprService.vectorSearch(eq(LEGACY_VECTOR), eq(LEGACY_TYPE), eq(FILTER), eq(10)))
                .thenReturn(result(embedding("doc-9", "legacy-irrelevant", LEGACY_TYPE, 0.11d)));

        VectorSearchResult merged = serviceWithLegacyModel().search(request());

        assertThat(chunkIds(merged))
                .as("the weak type's best hit stays last instead of tying for first")
                .containsExactly("primary-best", "primary-second", "primary-third", "legacy-irrelevant");
        assertThat(merged.getEmbeddings().get(3).getSysembedScore())
                .as("still filterable by minScore, which a promoted 1.0 would not be")
                .isEqualTo(0.11d);
    }

    /**
     * The same chunk embedded by two models is one candidate, kept at its better score.
     *
     * <p>This is the normal state of a document mid-migration: it has been re-embedded under the new
     * type but the previous type's child has not been cleared yet. Without the collapse it would occupy
     * two of the caller's slots with identical text.</p>
     */
    @Test
    void theSameChunkFoundUnderTwoTypesCollapsesToItsBestScore() {
        when(catalog.activeTypes()).thenReturn(List.of(PRIMARY_TYPE, LEGACY_TYPE));
        when(hxprService.vectorSearch(eq(PRIMARY_VECTOR), eq(PRIMARY_TYPE), eq(FILTER), eq(10)))
                .thenReturn(result(
                        embedding("doc-1", "shared", PRIMARY_TYPE, 0.50d),
                        embedding("doc-9", "primary-top", PRIMARY_TYPE, 0.95d)));
        when(hxprService.vectorSearch(eq(LEGACY_VECTOR), eq(LEGACY_TYPE), eq(FILTER), eq(10)))
                .thenReturn(result(embedding("doc-1", "shared", LEGACY_TYPE, 0.72d)));

        VectorSearchResult merged = serviceWithLegacyModel().search(request());

        assertThat(chunkIds(merged)).containsExactly("primary-top", "shared");
        assertThat(merged.getEmbeddings().get(1).getSysembedScore())
                .as("kept from the type that scored it higher")
                .isEqualTo(0.72d);
    }

    /**
     * A type present in the corpus with no configured model is still queried, with the primary vector.
     * That is what the wildcard already does for it, so nothing regresses; naming the model is what
     * turns it into a real comparison.
     */
    @Test
    void anUnmappedTypeIsStillQueriedWithThePrimaryVector() {
        when(catalog.activeTypes()).thenReturn(List.of(PRIMARY_TYPE, "ai-some-retired-model"));
        when(hxprService.vectorSearch(eq(PRIMARY_VECTOR), eq(PRIMARY_TYPE), eq(FILTER), eq(10)))
                .thenReturn(result(embedding("doc-1", "c1", PRIMARY_TYPE, 0.9d)));
        when(hxprService.vectorSearch(eq(PRIMARY_VECTOR), eq("ai-some-retired-model"), eq(FILTER), eq(10)))
                .thenReturn(result(embedding("doc-2", "c2", "ai-some-retired-model", 0.7d)));

        VectorSearchResult merged = service(List.of()).search(request());

        assertThat(chunkIds(merged)).containsExactlyInAnyOrder("c1", "c2");
    }

    /**
     * An embedder is registered under the derived type as well as the raw model name, because a corpus
     * can carry either form.
     */
    @Test
    void aModelIsMatchedByItsRawNameAsWellAsItsDerivedType() {
        when(catalog.activeTypes()).thenReturn(List.of(PRIMARY_TYPE, LEGACY_MODEL));
        when(hxprService.vectorSearch(eq(PRIMARY_VECTOR), eq(PRIMARY_TYPE), eq(FILTER), eq(10)))
                .thenReturn(result(embedding("doc-1", "c1", PRIMARY_TYPE, 0.9d)));
        when(hxprService.vectorSearch(eq(LEGACY_VECTOR), eq(LEGACY_MODEL), eq(FILTER), eq(10)))
                .thenReturn(result(embedding("doc-2", "c2", LEGACY_MODEL, 0.9d)));

        VectorSearchResult merged = serviceWithLegacyModel().search(request());

        assertThat(chunkIds(merged)).containsExactlyInAnyOrder("c1", "c2");
    }

    /** A type whose search fails must not take the whole leg down with it. */
    @Test
    void aTypeThatFailsToEmbedIsSkippedRatherThanFailingTheSearch() {
        when(catalog.activeTypes()).thenReturn(List.of(PRIMARY_TYPE, LEGACY_TYPE));
        when(hxprService.vectorSearch(eq(PRIMARY_VECTOR), eq(PRIMARY_TYPE), eq(FILTER), eq(10)))
                .thenReturn(result(embedding("doc-1", "c1", PRIMARY_TYPE, 0.9d)));

        MultiTypeVectorSearchService.Request failing = MultiTypeVectorSearchService.Request.of(
                PRIMARY_VECTOR,
                embedder -> {
                    throw new IllegalStateException("model runner is down");
                },
                null, FILTER, 10);

        VectorSearchResult merged = serviceWithLegacyModel().search(failing);

        assertThat(chunkIds(merged)).containsExactly("c1");
        verify(hxprService, never()).vectorSearch(any(), eq(LEGACY_TYPE), any(), anyInt());
    }

    /**
     * A model the underlying {@code EmbeddingModel} cannot target is not registered at all, so its type
     * falls through to the unmapped path rather than being scored in the primary's space while looking
     * configured.
     */
    @Test
    void aModelThatCannotBeTargetedIsNotRegistered() {
        when(primaryEmbeddingService.forModel(LEGACY_MODEL)).thenReturn(null);
        when(catalog.activeTypes()).thenReturn(List.of(PRIMARY_TYPE, LEGACY_TYPE));
        when(hxprService.vectorSearch(eq(PRIMARY_VECTOR), eq(PRIMARY_TYPE), eq(FILTER), eq(10)))
                .thenReturn(result(embedding("doc-1", "c1", PRIMARY_TYPE, 0.9d)));
        when(hxprService.vectorSearch(eq(PRIMARY_VECTOR), eq(LEGACY_TYPE), eq(FILTER), eq(10)))
                .thenReturn(result(embedding("doc-2", "c2", LEGACY_TYPE, 0.9d)));

        VectorSearchResult merged = service(List.of(LEGACY_MODEL)).search(request());

        assertThat(chunkIds(merged)).containsExactlyInAnyOrder("c1", "c2");
    }

    /** Chunk-level term filtering travels with every per-type search, not just the first. */
    @Test
    void chunkFtsIsAppliedToEveryType() {
        when(catalog.activeTypes()).thenReturn(List.of(PRIMARY_TYPE, LEGACY_TYPE));
        when(hxprService.vectorSearch(any(), any(), eq(FILTER), eq("chg-105402"), eq(10)))
                .thenReturn(result(embedding("doc-1", "c1", PRIMARY_TYPE, 0.9d)));

        MultiTypeVectorSearchService.Request withFts = new MultiTypeVectorSearchService.Request(
                PRIMARY_VECTOR, embedder -> LEGACY_VECTOR, null, FILTER, "chg-105402", 10);

        serviceWithLegacyModel().search(withFts);

        verify(hxprService, times(2)).vectorSearch(any(), any(), eq(FILTER), eq("chg-105402"), eq(10));
    }

    @Test
    void noCandidatesFromAnyTypeReturnsNull() {
        when(catalog.activeTypes()).thenReturn(List.of(PRIMARY_TYPE, LEGACY_TYPE));
        when(hxprService.vectorSearch(any(), any(), eq(FILTER), eq(10))).thenReturn(null);

        assertThat(serviceWithLegacyModel().search(request())).isNull();
    }
}
