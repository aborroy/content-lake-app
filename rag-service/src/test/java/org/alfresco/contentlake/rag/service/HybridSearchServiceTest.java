package org.alfresco.contentlake.rag.service;

import org.alfresco.contentlake.client.HxprService;
import org.alfresco.contentlake.hxpr.api.model.Embedding;
import org.alfresco.contentlake.hxpr.api.model.VectorSearchResult;
import org.alfresco.contentlake.model.HxprDocument;
import org.alfresco.contentlake.model.HxprEmbedding;
import org.alfresco.contentlake.rag.config.HybridSearchProperties;
import org.alfresco.contentlake.rag.model.HybridSearchRequest;
import org.alfresco.contentlake.rag.model.HybridSearchResponse;
import org.alfresco.contentlake.rag.service.HybridSearchService.FusedResult;
import org.alfresco.contentlake.rag.service.HybridSearchService.ScoredChunk;
import org.alfresco.contentlake.security.SecurityContextService;
import org.alfresco.contentlake.service.EmbeddingService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class HybridSearchServiceTest {

    @Mock HxprService hxprService;
    @Mock EmbeddingService embeddingService;
    @Mock SecurityContextService securityContextService;
    @Mock HybridSearchProperties properties;

    @InjectMocks HybridSearchService service;

    @BeforeEach
    void setUp() {
        ReflectionTestUtils.setField(service, "repositoryId", "test-repo");
        ReflectionTestUtils.setField(service, "alfrescoUrl", "http://localhost:1");
        ReflectionTestUtils.setField(service, "serviceAccountUsername", "admin");
        ReflectionTestUtils.setField(service, "serviceAccountPassword", "admin");
    }

    // -----------------------------------------------------------------------
    // RRF fusion
    // -----------------------------------------------------------------------

    @Nested
    class RrfFusion {

        @Test
        void fuseRRF_bothLegsHaveResults_combinesScores() {
            var v1 = new ScoredChunk("doc1::e1", "doc1", "e1", "vector text 1", "model", 0.9, 1, null);
            var v2 = new ScoredChunk("doc2::e2", "doc2", "e2", "vector text 2", "model", 0.7, 2, null);

            var k1 = new ScoredChunk("doc2::e2", "doc2", "e2", "vector text 2", "model", 0.8, 1, null);
            var k2 = new ScoredChunk("doc3::e3", "doc3", "e3", "keyword text", "model", 0.6, 2, null);

            List<FusedResult> results = HybridSearchService.fuseRRF(List.of(v1, v2), List.of(k1, k2), 60);

            assertThat(results).hasSize(3);

            // doc2::e2 appears in both lists so should have highest RRF score
            FusedResult top = results.getFirst();
            assertThat(top.chunk.key()).isEqualTo("doc2::e2");
            // RRF score = 1/(60+2) + 1/(60+1) = 1/62 + 1/61
            double expectedScore = 1.0 / 62 + 1.0 / 61;
            assertThat(top.getScore()).isCloseTo(expectedScore, within(0.0001));
            assertThat(top.vectorScore).isEqualTo(0.7);
            assertThat(top.keywordScore).isEqualTo(0.8);
            assertThat(top.vectorRank).isEqualTo(2);
            assertThat(top.keywordRank).isEqualTo(1);
        }

        @Test
        void fuseRRF_emptyVectorLeg_returnsKeywordOnly() {
            var k1 = new ScoredChunk("doc1::e1", "doc1", "e1", "keyword text", "model", 0.9, 1, null);

            List<FusedResult> results = HybridSearchService.fuseRRF(List.of(), List.of(k1), 60);

            assertThat(results).hasSize(1);
            assertThat(results.getFirst().vectorScore).isNull();
            assertThat(results.getFirst().keywordScore).isEqualTo(0.9);
        }

        @Test
        void fuseRRF_emptyKeywordLeg_returnsVectorOnly() {
            var v1 = new ScoredChunk("doc1::e1", "doc1", "e1", "vector text", "model", 0.9, 1, null);

            List<FusedResult> results = HybridSearchService.fuseRRF(List.of(v1), List.of(), 60);

            assertThat(results).hasSize(1);
            assertThat(results.getFirst().vectorScore).isEqualTo(0.9);
            assertThat(results.getFirst().keywordScore).isNull();
        }

        @Test
        void fuseRRF_bothEmpty_returnsEmpty() {
            List<FusedResult> results = HybridSearchService.fuseRRF(List.of(), List.of(), 60);
            assertThat(results).isEmpty();
        }

        @Test
        void fuseRRF_higherKSmooths_rankDifferences() {
            var v1 = new ScoredChunk("a", "d1", "e1", "t1", "m", 0.9, 1, null);
            var v2 = new ScoredChunk("b", "d2", "e2", "t2", "m", 0.1, 10, null);

            // With k=1 (low smoothing), rank 1 gets 1/2 = 0.5, rank 10 gets 1/11 = 0.09
            List<FusedResult> lowK = HybridSearchService.fuseRRF(List.of(v1, v2), List.of(), 1);
            double diffLow = lowK.get(0).getScore() - lowK.get(1).getScore();

            // With k=100 (high smoothing), rank 1 gets 1/101 = 0.0099, rank 10 gets 1/110 = 0.0091
            List<FusedResult> highK = HybridSearchService.fuseRRF(List.of(v1, v2), List.of(), 100);
            double diffHigh = highK.get(0).getScore() - highK.get(1).getScore();

            assertThat(diffHigh).isLessThan(diffLow);
        }
    }

    // -----------------------------------------------------------------------
    // Weighted fusion
    // -----------------------------------------------------------------------

    @Nested
    class WeightedFusion {

        @Test
        void fuseWeighted_appliesWeightsCorrectly() {
            var v1 = new ScoredChunk("doc1::e1", "doc1", "e1", "text", "model", 0.8, 1, null);
            var k1 = new ScoredChunk("doc1::e1", "doc1", "e1", "text", "model", 1.0, 1, null);

            List<FusedResult> results = HybridSearchService.fuseWeighted(
                    List.of(v1), List.of(k1), 0.7, 0.3);

            assertThat(results).hasSize(1);
            // vectorScore normalised: 0.8/0.8 = 1.0, keywordScore normalised: 1.0/1.0 = 1.0
            // fused = 0.7 * 1.0 + 0.3 * 1.0 = 1.0
            assertThat(results.getFirst().getScore()).isCloseTo(1.0, within(0.0001));
        }

        @Test
        void fuseWeighted_normalisesScores() {
            var v1 = new ScoredChunk("a", "d1", "e1", "t1", "m", 1.0, 1, null);
            var v2 = new ScoredChunk("b", "d2", "e2", "t2", "m", 0.5, 2, null);

            List<FusedResult> results = HybridSearchService.fuseWeighted(
                    List.of(v1, v2), List.of(), 0.7, 0.3);

            // v1 normalised = 1.0/1.0 = 1.0, score = 0.7 * 1.0 = 0.7
            assertThat(results.get(0).getScore()).isCloseTo(0.7, within(0.0001));
            // v2 normalised = 0.5/1.0 = 0.5, score = 0.7 * 0.5 = 0.35
            assertThat(results.get(1).getScore()).isCloseTo(0.35, within(0.0001));
        }

        @Test
        void fuseWeighted_vectorOnlyResult_noKeywordContribution() {
            var v1 = new ScoredChunk("a", "d1", "e1", "t1", "m", 0.9, 1, null);

            List<FusedResult> results = HybridSearchService.fuseWeighted(
                    List.of(v1), List.of(), 0.6, 0.4);

            assertThat(results).hasSize(1);
            // normalised = 0.9/0.9 = 1.0, fused = 0.6 * 1.0 = 0.6
            assertThat(results.getFirst().getScore()).isCloseTo(0.6, within(0.0001));
            assertThat(results.getFirst().keywordScore).isNull();
        }

        @Test
        void fuseWeighted_keywordOnlyResult_noVectorContribution() {
            var k1 = new ScoredChunk("a", "d1", "e1", "t1", "m", 0.8, 1, null);

            List<FusedResult> results = HybridSearchService.fuseWeighted(
                    List.of(), List.of(k1), 0.6, 0.4);

            assertThat(results).hasSize(1);
            // normalised = 0.8/0.8 = 1.0, fused = 0.4 * 1.0 = 0.4
            assertThat(results.getFirst().getScore()).isCloseTo(0.4, within(0.0001));
            assertThat(results.getFirst().vectorScore).isNull();
        }

        @Test
        void fuseWeighted_minMaxNormalisation_changesDistribution() {
            var v1 = new ScoredChunk("a", "d1", "e1", "t1", "m", 0.9, 1, null);
            var v2 = new ScoredChunk("b", "d2", "e2", "t2", "m", 0.3, 2, null);

            List<FusedResult> results = HybridSearchService.fuseWeighted(
                    List.of(v1, v2), List.of(), 1.0, 0.0, "minmax");

            // min-max: (0.9 - 0.3) / (0.9 - 0.3) = 1.0
            assertThat(results.get(0).getScore()).isCloseTo(1.0, within(0.0001));
            // min-max: (0.3 - 0.3) / (0.9 - 0.3) = 0.0
            assertThat(results.get(1).getScore()).isCloseTo(0.0, within(0.0001));
        }
    }

    // -----------------------------------------------------------------------
    // Keyword scoring
    // -----------------------------------------------------------------------

    @Nested
    class KeywordScoring {

        @Test
        void computeKeywordScore_allTermsMatch_returnsOne() {
            double score = HybridSearchService.computeKeywordScore(
                    "This document discusses Alfresco content management", new String[]{"alfresco", "content"});
            assertThat(score).isEqualTo(1.0);
        }

        @Test
        void computeKeywordScore_partialMatch_returnsFraction() {
            double score = HybridSearchService.computeKeywordScore(
                    "Alfresco is a CMS", new String[]{"alfresco", "content", "lake"});
            assertThat(score).isCloseTo(1.0 / 3, within(0.0001));
        }

        @Test
        void computeKeywordScore_noMatch_returnsZero() {
            double score = HybridSearchService.computeKeywordScore(
                    "unrelated text", new String[]{"alfresco", "content"});
            assertThat(score).isEqualTo(0.0);
        }

        @Test
        void computeKeywordScore_emptyTerms_returnsZero() {
            double score = HybridSearchService.computeKeywordScore("some text", new String[]{});
            assertThat(score).isEqualTo(0.0);
        }

        @Test
        void computeKeywordScore_caseInsensitive() {
            double score = HybridSearchService.computeKeywordScore(
                    "ALFRESCO Content Management", new String[]{"alfresco", "content"});
            assertThat(score).isEqualTo(1.0);
        }
    }

    // -----------------------------------------------------------------------
    // Fulltext query building
    // -----------------------------------------------------------------------

    @Nested
    class FulltextQueryBuilding {

        @Test
        void buildFulltextQuery_withPermissionFilter_combinesCorrectly() {
            String permFilter = "SELECT * FROM SysContent WHERE (sys_racl = '__Everyone__')";
            String hxql = service.buildFulltextQuery("search terms", permFilter);

            assertThat(hxql).startsWith("SELECT * FROM SysContent WHERE sys_fulltextBinary = 'search terms'");
            assertThat(hxql).contains("AND (sys_racl = '__Everyone__')");
        }

        @Test
        void buildFulltextQuery_nullPermissionFilter_fulltextOnly() {
            String hxql = service.buildFulltextQuery("test query", null);
            assertThat(hxql).isEqualTo("SELECT * FROM SysContent WHERE sys_fulltextBinary = 'test query'");
        }

        @Test
        void buildFulltextQuery_escapesQuotes() {
            String hxql = service.buildFulltextQuery("it's a test", null);
            assertThat(hxql).contains("it''s a test");
        }
    }

    // -----------------------------------------------------------------------
    // Metadata filter building
    // -----------------------------------------------------------------------

    @Nested
    class MetadataFilterBuilding {

        @Test
        void combineFilters_bothPresent_combinesWithAnd() {
            String combined = HybridSearchService.combineFilters("cin_sourceId = 'repo'", "cin_id = 'node'");
            assertThat(combined).isEqualTo("(cin_sourceId = 'repo') AND (cin_id = 'node')");
        }

        @Test
        void combineFilters_onePresent_returnsSingleFilter() {
            assertThat(HybridSearchService.combineFilters("  cin_sourceId = 'repo'  ", null))
                    .isEqualTo("cin_sourceId = 'repo'");
            assertThat(HybridSearchService.combineFilters(null, "  cin_id = 'node'  "))
                    .isEqualTo("cin_id = 'node'");
        }

        @Test
        void buildMetadataFilter_allFields_buildsExpectedClauses() {
            HybridSearchRequest.MetadataFilter metadata = HybridSearchRequest.MetadataFilter.builder()
                    .mimeType("application/pdf")
                    .pathPrefix("/Company Home/Sites/Finance")
                    .modifiedAfter("2026-01-01T00:00:00Z")
                    .modifiedBefore("2026-12-31T23:59:59Z")
                    .properties(Map.of("cm:title", "Budget 2026"))
                    .build();

            String filter = service.buildMetadataFilter(metadata);

            assertThat(filter).contains("cin_ingestProperties.alfresco_mimeType = 'application/pdf'");
            assertThat(filter).contains("(cin_ingestProperties.alfresco_path >= '/Company Home/Sites/Finance' AND cin_ingestProperties.alfresco_path < '/Company Home/Sites/Finance\uFFFF')");
            assertThat(filter).contains("cin_ingestProperties.alfresco_modifiedAt >= '2026-01-01T00:00:00Z'");
            assertThat(filter).contains("cin_ingestProperties.alfresco_modifiedAt <= '2026-12-31T23:59:59Z'");
            assertThat(filter).contains("cin_ingestProperties.cm:title = 'Budget 2026'");
        }

        @Test
        void buildMetadataFilter_invalidCustomPropertyKey_isIgnored() {
            HybridSearchRequest.MetadataFilter metadata = HybridSearchRequest.MetadataFilter.builder()
                    .properties(Map.of("bad key", "value"))
                    .build();

            String filter = service.buildMetadataFilter(metadata);
            assertThat(filter).isNull();
        }
    }

    // -----------------------------------------------------------------------
    // Permission filter
    // -----------------------------------------------------------------------

    @Nested
    class PermissionFilter {

        @Test
        void buildPermissionFilter_includesEveryoneAndUser() {
            HybridSearchService svc = spy(service);
            doReturn(List.of("alice", "GROUP_EVERYONE")).when(svc).getUserAuthorities("alice");

            String filter = svc.buildPermissionFilter("alice", null);

            assertThat(filter).contains("sys_racl = '__Everyone__'");
            assertThat(filter).contains("sys_racl = 'alice_#_test-repo'");
        }

        @Test
        void buildPermissionFilter_withGroups() {
            HybridSearchService svc = spy(service);
            doReturn(List.of("bob", "GROUP_EVERYONE", "GROUP_ENGINEERING"))
                    .when(svc).getUserAuthorities("bob");

            String filter = svc.buildPermissionFilter("bob", null);

            assertThat(filter).contains("g:GROUP_ENGINEERING_#_test-repo");
        }

        @Test
        void buildPermissionFilter_withAdditionalFilter() {
            HybridSearchService svc = spy(service);
            doReturn(List.of("user")).when(svc).getUserAuthorities("user");

            String filter = svc.buildPermissionFilter("user", "cin_sourceId = 'my-repo'");

            assertThat(filter).contains(" AND ");
            assertThat(filter).contains("cin_sourceId = 'my-repo'");
        }
    }

    // -----------------------------------------------------------------------
    // End-to-end search
    // -----------------------------------------------------------------------

    @Nested
    class EndToEndSearch {

        @Test
        void search_emptyEmbedding_returnsKeywordOnlyResults() {
            when(properties.getStrategy()).thenReturn("rrf");
            when(properties.getCandidateCount()).thenReturn(20);
            when(properties.getMaxResults()).thenReturn(5);
            when(properties.getRrfK()).thenReturn(60);
            when(properties.getDefaultMinScore()).thenReturn(0.0);

            when(securityContextService.getCurrentUsername()).thenReturn("user");
            when(embeddingService.embedQuery(any())).thenReturn(List.of());
            when(embeddingService.getModelName()).thenReturn("test-model");


            HybridSearchService svc = spy(service);
            doReturn(List.of("user")).when(svc).getUserAuthorities(any());
            doReturn(List.of()).when(svc).executeKeywordSearch(any(), any(), anyInt());

            HybridSearchRequest request = HybridSearchRequest.builder().query("test").build();
            HybridSearchResponse response = svc.search(request);

            assertThat(response.getResultCount()).isZero();
            assertThat(response.getStrategy()).isEqualTo("rrf");
            assertThat(response.getQuery()).isEqualTo("test");
        }

        @Test
        void search_vectorResults_noKeywordResults_returnsVectorOnly() {
            when(properties.getStrategy()).thenReturn("rrf");
            when(properties.getCandidateCount()).thenReturn(20);
            when(properties.getMaxResults()).thenReturn(5);
            when(properties.getRrfK()).thenReturn(60);
            when(properties.getDefaultMinScore()).thenReturn(0.0);

            when(securityContextService.getCurrentUsername()).thenReturn("user");
            when(embeddingService.embedQuery(any())).thenReturn(List.of(0.1, 0.2));
            when(embeddingService.getModelName()).thenReturn("test-model");


            Embedding emb = mock(Embedding.class);
            when(emb.getSysembedDocId()).thenReturn("doc-id-1");
            when(emb.getSysembedId()).thenReturn("emb-1");
            when(emb.getSysembedText()).thenReturn("relevant chunk");
            when(emb.getSysembedType()).thenReturn("mxbai");
            when(emb.getSysembedScore()).thenReturn(0.85);
            when(emb.getSysembedLocation()).thenReturn(null);

            VectorSearchResult vectorResult = mock(VectorSearchResult.class);
            when(vectorResult.getEmbeddings()).thenReturn(List.of(emb));

            when(hxprService.vectorSearch(any(), any(), any(), anyInt())).thenReturn(vectorResult);

            HybridSearchService svc = spy(service);
            doReturn(List.of("user")).when(svc).getUserAuthorities(any());
            doReturn(List.of()).when(svc).executeKeywordSearch(any(), any(), anyInt());

            HybridSearchRequest request = HybridSearchRequest.builder().query("test").build();
            HybridSearchResponse response = svc.search(request);

            assertThat(response.getResultCount()).isEqualTo(1);
            assertThat(response.getVectorCandidates()).isEqualTo(1);
            assertThat(response.getKeywordCandidates()).isZero();
            assertThat(response.getResults().getFirst().getChunkText()).isEqualTo("relevant chunk");
            assertThat(response.getResults().getFirst().getVectorScore()).isEqualTo(0.85);
            assertThat(response.getResults().getFirst().getKeywordScore()).isNull();
        }

        @Test
        void search_weightedStrategy_usesWeightedFusion() {
            when(properties.getStrategy()).thenReturn("weighted");
            when(properties.getCandidateCount()).thenReturn(20);
            when(properties.getMaxResults()).thenReturn(5);
            when(properties.getNormalization()).thenReturn("minmax");
            when(properties.getVectorWeight()).thenReturn(0.7);
            when(properties.getTextWeight()).thenReturn(0.3);
            when(properties.getDefaultMinScore()).thenReturn(0.0);

            when(securityContextService.getCurrentUsername()).thenReturn("user");
            when(embeddingService.embedQuery(any())).thenReturn(List.of(0.1, 0.2));
            when(embeddingService.getModelName()).thenReturn("test-model");


            Embedding emb = mock(Embedding.class);
            when(emb.getSysembedDocId()).thenReturn("doc-1");
            when(emb.getSysembedId()).thenReturn("e1");
            when(emb.getSysembedText()).thenReturn("semantic match");
            when(emb.getSysembedType()).thenReturn("mxbai");
            when(emb.getSysembedScore()).thenReturn(0.9);
            when(emb.getSysembedLocation()).thenReturn(null);

            VectorSearchResult vectorResult = mock(VectorSearchResult.class);
            when(vectorResult.getEmbeddings()).thenReturn(List.of(emb));
            when(hxprService.vectorSearch(any(), any(), any(), anyInt())).thenReturn(vectorResult);

            HybridSearchService svc = spy(service);
            doReturn(List.of("user")).when(svc).getUserAuthorities(any());
            doReturn(List.of()).when(svc).executeKeywordSearch(any(), any(), anyInt());

            HybridSearchRequest request = HybridSearchRequest.builder().query("test").build();
            HybridSearchResponse response = svc.search(request);

            assertThat(response.getStrategy()).isEqualTo("weighted");
            assertThat(response.getNormalization()).isEqualTo("minmax");
            assertThat(response.getResultCount()).isEqualTo(1);
        }

        @Test
        void search_requestOverridesStrategy() {
            when(properties.getCandidateCount()).thenReturn(20);
            when(properties.getMaxResults()).thenReturn(5);
            when(properties.getVectorWeight()).thenReturn(0.7);
            when(properties.getTextWeight()).thenReturn(0.3);
            when(properties.getDefaultMinScore()).thenReturn(0.0);

            when(securityContextService.getCurrentUsername()).thenReturn("user");
            when(embeddingService.embedQuery(any())).thenReturn(List.of());
            when(embeddingService.getModelName()).thenReturn("test-model");


            HybridSearchService svc = spy(service);
            doReturn(List.of("user")).when(svc).getUserAuthorities(any());
            doReturn(List.of()).when(svc).executeKeywordSearch(any(), any(), anyInt());

            HybridSearchRequest request = HybridSearchRequest.builder()
                    .query("test")
                    .strategy("weighted")
                    .build();
            HybridSearchResponse response = svc.search(request);

            assertThat(response.getStrategy()).isEqualTo("weighted");
        }

        @Test
        void search_invalidStrategy_fallsBackToRrf() {
            when(properties.getCandidateCount()).thenReturn(20);
            when(properties.getMaxResults()).thenReturn(5);
            when(properties.getRrfK()).thenReturn(60);
            when(properties.getDefaultMinScore()).thenReturn(0.0);

            when(securityContextService.getCurrentUsername()).thenReturn("user");
            when(embeddingService.embedQuery(any())).thenReturn(List.of());
            when(embeddingService.getModelName()).thenReturn("test-model");


            HybridSearchService svc = spy(service);
            doReturn(List.of("user")).when(svc).getUserAuthorities(any());
            doReturn(List.of()).when(svc).executeKeywordSearch(any(), any(), anyInt());

            HybridSearchRequest request = HybridSearchRequest.builder()
                    .query("test")
                    .strategy("custom")
                    .build();
            HybridSearchResponse response = svc.search(request);

            assertThat(response.getStrategy()).isEqualTo("rrf");
        }

        @Test
        void search_minScoreFiltering_excludesLowResults() {
            when(properties.getStrategy()).thenReturn("rrf");
            when(properties.getCandidateCount()).thenReturn(20);
            when(properties.getMaxResults()).thenReturn(10);
            when(properties.getRrfK()).thenReturn(60);

            when(securityContextService.getCurrentUsername()).thenReturn("user");
            when(embeddingService.embedQuery(any())).thenReturn(List.of(0.1));
            when(embeddingService.getModelName()).thenReturn("test-model");


            Embedding emb1 = mock(Embedding.class);
            when(emb1.getSysembedDocId()).thenReturn("d1");
            when(emb1.getSysembedId()).thenReturn("e1");
            when(emb1.getSysembedText()).thenReturn("text 1");
            when(emb1.getSysembedType()).thenReturn("m");
            when(emb1.getSysembedScore()).thenReturn(0.9);
            when(emb1.getSysembedLocation()).thenReturn(null);

            Embedding emb2 = mock(Embedding.class);
            when(emb2.getSysembedDocId()).thenReturn("d2");
            when(emb2.getSysembedId()).thenReturn("e2");
            when(emb2.getSysembedText()).thenReturn("text 2");
            when(emb2.getSysembedType()).thenReturn("m");
            when(emb2.getSysembedScore()).thenReturn(0.3);
            when(emb2.getSysembedLocation()).thenReturn(null);

            VectorSearchResult vr = mock(VectorSearchResult.class);
            when(vr.getEmbeddings()).thenReturn(List.of(emb1, emb2));
            when(hxprService.vectorSearch(any(), any(), any(), anyInt())).thenReturn(vr);

            HybridSearchService svc = spy(service);
            doReturn(List.of("user")).when(svc).getUserAuthorities(any());
            doReturn(List.of()).when(svc).executeKeywordSearch(any(), any(), anyInt());

            // Set minScore high enough to filter the second result
            // RRF score for rank 1 = 1/61 ≈ 0.0164, rank 2 = 1/62 ≈ 0.0161
            HybridSearchRequest request = HybridSearchRequest.builder()
                    .query("test")
                    .minScore(1.0 / 61.5) // between rank 1 and rank 2 RRF scores
                    .build();
            HybridSearchResponse response = svc.search(request);

            assertThat(response.getResultCount()).isEqualTo(1);
        }
    }

    // -----------------------------------------------------------------------
    // Keyword search extraction
    // -----------------------------------------------------------------------

    @Nested
    class KeywordSearchExtraction {

        @Test
        void executeKeywordSearch_extractsAndScoresChunks() {
            HxprDocument doc = new HxprDocument();
            doc.setSysId("doc-1");

            HxprEmbedding emb1 = new HxprEmbedding();
            emb1.setText("This chunk contains alfresco content management");
            emb1.setType("mxbai");

            HxprEmbedding emb2 = new HxprEmbedding();
            emb2.setText("This chunk is about something else entirely");
            emb2.setType("mxbai");

            doc.setSysembedEmbeddings(List.of(emb1, emb2));

            HxprDocument.QueryResult result = new HxprDocument.QueryResult();
            result.setDocuments(List.of(doc));

            when(hxprService.query(any(), anyInt(), anyInt())).thenReturn(result);

            List<ScoredChunk> chunks = service.executeKeywordSearch(
                    "alfresco content", "SELECT * FROM SysContent WHERE (sys_racl = '__Everyone__')", 20);

            // First chunk matches both terms -> score 1.0
            assertThat(chunks).hasSize(1);
            assertThat(chunks.get(0).score()).isEqualTo(1.0);
            assertThat(chunks.getFirst().text()).isEqualTo("This chunk contains alfresco content management");
        }

        @Test
        void executeKeywordSearch_emptyDocuments_returnsEmpty() {
            HxprDocument.QueryResult result = new HxprDocument.QueryResult();
            result.setDocuments(List.of());

            when(hxprService.query(any(), anyInt(), anyInt())).thenReturn(result);

            List<ScoredChunk> chunks = service.executeKeywordSearch("test", "filter", 20);
            assertThat(chunks).isEmpty();
        }

        @Test
        void executeKeywordSearch_queryFailure_returnsEmpty() {
            when(hxprService.query(any(), anyInt(), anyInt())).thenThrow(new RuntimeException("connection error"));

            List<ScoredChunk> chunks = service.executeKeywordSearch("test", "filter", 20);
            assertThat(chunks).isEmpty();
        }

        @Test
        void executeKeywordSearch_documentsWithoutEmbeddings_skipped() {
            HxprDocument doc = new HxprDocument();
            doc.setSysId("doc-1");
            doc.setSysembedEmbeddings(null);

            HxprDocument.QueryResult result = new HxprDocument.QueryResult();
            result.setDocuments(List.of(doc));

            when(hxprService.query(any(), anyInt(), anyInt())).thenReturn(result);

            List<ScoredChunk> chunks = service.executeKeywordSearch("test", "filter", 20);
            assertThat(chunks).isEmpty();
        }
    }
}
