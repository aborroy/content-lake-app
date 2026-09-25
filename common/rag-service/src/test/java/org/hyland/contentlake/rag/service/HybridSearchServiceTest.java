package org.hyland.contentlake.rag.service;

import org.hyland.contentlake.client.HxprService;
import org.hyland.contentlake.model.HxprTermsAggregationResult;
import org.hyland.contentlake.hxpr.api.model.Embedding;
import org.hyland.contentlake.hxpr.api.model.VectorSearchResult;
import org.hyland.contentlake.model.HxprDocument;
import org.hyland.contentlake.rag.config.HybridSearchProperties;
import org.hyland.contentlake.rag.config.RagProperties;
import org.hyland.contentlake.rag.model.HybridSearchRequest;
import org.hyland.contentlake.rag.model.HybridSearchResponse;
import org.hyland.contentlake.rag.service.HybridSearchService.FusedResult;
import org.hyland.contentlake.rag.service.HybridSearchService.ScoredChunk;
import org.hyland.contentlake.security.CallerIdentities;
import org.hyland.contentlake.security.SecurityContextService;
import org.hyland.contentlake.service.EmbeddingService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class HybridSearchServiceTest {

    @Mock HxprService hxprService;
    @Mock EmbeddingService embeddingService;
    @Mock SecurityContextService securityContextService;
    @Mock HybridSearchProperties properties;
    @Mock SourceMetadataResolver sourceMetadataResolver;
    @Mock SectionMapResolver sectionMapResolver;
    @Mock QueryExpansionService queryExpansionService;
    @Mock RagProperties ragProperties;
    @Mock org.hyland.contentlake.client.NamedQueryService namedQueryService;
    @Mock org.hyland.contentlake.client.VocabularyService vocabularyService;

    @InjectMocks HybridSearchService service;

    @BeforeEach
    void setUp() {
        // Vocabulary resolution defaults to a pass-through; only the vocabulary-specific test overrides it.
        org.mockito.Mockito.lenient()
                .when(vocabularyService.resolve(org.mockito.ArgumentMatchers.anyString()))
                .thenAnswer(inv -> inv.getArgument(0));
        // Retrieval shaping defaults to the real properties with every flag off, so a search behaves as
        // it did before #122. The verbatim tests replace this with the flag on.
        org.mockito.Mockito.lenient()
                .when(ragProperties.getRetrieval())
                .thenReturn(new RagProperties.RetrievalProperties());
        ReflectionTestUtils.setField(service, "alfrescoSourceId", "test-repo");
        ReflectionTestUtils.setField(service, "permissionSourceIds", "");
        ReflectionTestUtils.setField(service, "nuxeoSourceId", "");
    }

    // -----------------------------------------------------------------------
    // RRF fusion
    // -----------------------------------------------------------------------

    @Nested
    class RrfFusion {

        @Test
        void fuseRRF_bothLegsHaveResults_combinesScores() {
            var v1 = new ScoredChunk("doc1::e1", "doc1", "e1", "vector text 1", "model", 0.9, 1, null, null, null);
            var v2 = new ScoredChunk("doc2::e2", "doc2", "e2", "vector text 2", "model", 0.7, 2, null, null, null);

            var k1 = new ScoredChunk("doc2::e2", "doc2", "e2", "vector text 2", "model", 0.8, 1, null, null, null);
            var k2 = new ScoredChunk("doc3::e3", "doc3", "e3", "keyword text", "model", 0.6, 2, null, null, null);

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
            var k1 = new ScoredChunk("doc1::e1", "doc1", "e1", "keyword text", "model", 0.9, 1, null, null, null);

            List<FusedResult> results = HybridSearchService.fuseRRF(List.of(), List.of(k1), 60);

            assertThat(results).hasSize(1);
            assertThat(results.getFirst().vectorScore).isNull();
            assertThat(results.getFirst().keywordScore).isEqualTo(0.9);
        }

        @Test
        void fuseRRF_emptyKeywordLeg_returnsVectorOnly() {
            var v1 = new ScoredChunk("doc1::e1", "doc1", "e1", "vector text", "model", 0.9, 1, null, null, null);

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
            var v1 = new ScoredChunk("a", "d1", "e1", "t1", "m", 0.9, 1, null, null, null);
            var v2 = new ScoredChunk("b", "d2", "e2", "t2", "m", 0.1, 10, null, null, null);

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
            var v1 = new ScoredChunk("doc1::e1", "doc1", "e1", "text", "model", 0.8, 1, null, null, null);
            var k1 = new ScoredChunk("doc1::e1", "doc1", "e1", "text", "model", 1.0, 1, null, null, null);

            List<FusedResult> results = HybridSearchService.fuseWeighted(
                    List.of(v1), List.of(k1), 0.7, 0.3);

            assertThat(results).hasSize(1);
            // vectorScore normalised: 0.8/0.8 = 1.0, keywordScore normalised: 1.0/1.0 = 1.0
            // fused = 0.7 * 1.0 + 0.3 * 1.0 = 1.0
            assertThat(results.getFirst().getScore()).isCloseTo(1.0, within(0.0001));
        }

        @Test
        void fuseWeighted_normalisesScores() {
            var v1 = new ScoredChunk("a", "d1", "e1", "t1", "m", 1.0, 1, null, null, null);
            var v2 = new ScoredChunk("b", "d2", "e2", "t2", "m", 0.5, 2, null, null, null);

            List<FusedResult> results = HybridSearchService.fuseWeighted(
                    List.of(v1, v2), List.of(), 0.7, 0.3);

            // v1 normalised = 1.0/1.0 = 1.0, score = 0.7 * 1.0 = 0.7
            assertThat(results.get(0).getScore()).isCloseTo(0.7, within(0.0001));
            // v2 normalised = 0.5/1.0 = 0.5, score = 0.7 * 0.5 = 0.35
            assertThat(results.get(1).getScore()).isCloseTo(0.35, within(0.0001));
        }

        @Test
        void fuseWeighted_vectorOnlyResult_noKeywordContribution() {
            var v1 = new ScoredChunk("a", "d1", "e1", "t1", "m", 0.9, 1, null, null, null);

            List<FusedResult> results = HybridSearchService.fuseWeighted(
                    List.of(v1), List.of(), 0.6, 0.4);

            assertThat(results).hasSize(1);
            // normalised = 0.9/0.9 = 1.0, fused = 0.6 * 1.0 = 0.6
            assertThat(results.getFirst().getScore()).isCloseTo(0.6, within(0.0001));
            assertThat(results.getFirst().keywordScore).isNull();
        }

        @Test
        void fuseWeighted_keywordOnlyResult_noVectorContribution() {
            var k1 = new ScoredChunk("a", "d1", "e1", "t1", "m", 0.8, 1, null, null, null);

            List<FusedResult> results = HybridSearchService.fuseWeighted(
                    List.of(), List.of(k1), 0.6, 0.4);

            assertThat(results).hasSize(1);
            // normalised = 0.8/0.8 = 1.0, fused = 0.4 * 1.0 = 0.4
            assertThat(results.getFirst().getScore()).isCloseTo(0.4, within(0.0001));
            assertThat(results.getFirst().vectorScore).isNull();
        }

        @Test
        void fuseWeighted_minMaxNormalisation_changesDistribution() {
            var v1 = new ScoredChunk("a", "d1", "e1", "t1", "m", 0.9, 1, null, null, null);
            var v2 = new ScoredChunk("b", "d2", "e2", "t2", "m", 0.3, 2, null, null, null);

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
        void computeBm25TfScore_allTermsMatch_returnsPositiveScore() {
            double score = HybridSearchService.computeBm25TfScore(
                    "This document discusses Alfresco content management", new String[]{"alfresco", "content"});
            assertThat(score).isGreaterThan(0.0);
        }

        @Test
        void computeBm25TfScore_partialMatch_scoresLowerThanFullMatch() {
            double fullMatch = HybridSearchService.computeBm25TfScore(
                    "Alfresco content lake", new String[]{"alfresco", "content", "lake"});
            double partialMatch = HybridSearchService.computeBm25TfScore(
                    "Alfresco is a CMS", new String[]{"alfresco", "content", "lake"});
            assertThat(partialMatch).isLessThan(fullMatch);
            assertThat(partialMatch).isGreaterThan(0.0);
        }

        @Test
        void computeBm25TfScore_noMatch_returnsZero() {
            double score = HybridSearchService.computeBm25TfScore(
                    "unrelated text", new String[]{"alfresco", "content"});
            assertThat(score).isEqualTo(0.0);
        }

        @Test
        void computeBm25TfScore_emptyTerms_returnsZero() {
            double score = HybridSearchService.computeBm25TfScore("some text", new String[]{});
            assertThat(score).isEqualTo(0.0);
        }

        @Test
        void computeBm25TfScore_caseInsensitive() {
            double score = HybridSearchService.computeBm25TfScore(
                    "ALFRESCO Content Management", new String[]{"alfresco", "content"});
            assertThat(score).isGreaterThan(0.0);
        }

        @Test
        void computeBm25TfScore_higherTermFrequencyScoresHigher() {
            // "revenue" appears once vs three times — higher TF should score higher (sub-linearly)
            double once = HybridSearchService.computeBm25TfScore(
                    "revenue was reported last quarter", new String[]{"revenue"});
            double thrice = HybridSearchService.computeBm25TfScore(
                    "revenue revenue revenue reported last quarter", new String[]{"revenue"});
            assertThat(thrice).isGreaterThan(once);
        }

        @Test
        void computeBm25TfScore_tfSaturation_sublinear() {
            // Score increase from 1→3 occurrences should be less than 3× (BM25 saturation)
            double once = HybridSearchService.computeBm25TfScore(
                    "revenue reported", new String[]{"revenue"});
            double thrice = HybridSearchService.computeBm25TfScore(
                    "revenue revenue revenue reported", new String[]{"revenue"});
            assertThat(thrice).isLessThan(once * 3);
        }

        @Test
        void computeBm25TfScore_nullOrBlankChunk_returnsZero() {
            assertThat(HybridSearchService.computeBm25TfScore(null, new String[]{"term"})).isEqualTo(0.0);
            assertThat(HybridSearchService.computeBm25TfScore("", new String[]{"term"})).isEqualTo(0.0);
            assertThat(HybridSearchService.computeBm25TfScore("   ", new String[]{"term"})).isEqualTo(0.0);
        }
    }

    // -----------------------------------------------------------------------
    // Fulltext query building
    // -----------------------------------------------------------------------

    /**
     * The keyword leg's HXQL. Every expectation here was verified against a live hxpr index.
     *
     * <p>The previous form queried {@code cin_ingestProperties.contentLake_extractedText}, a key no
     * ingester ever wrote, so the leg contributed nothing to fusion on any query and
     * {@code keyword_leg_hit_rate} measured 0.0000. The sync now mirrors extracted text into that
     * property and this leg queries {@code sys_fulltext}, hxpr's analysed index over it, because the
     * property's own index truncates at 256 characters.</p>
     */
    @Nested
    class FulltextQueryBuilding {

        private static final String FIELD = "sys_fulltext";

        @Test
        void buildFulltextQuery_withPermissionFilter_combinesCorrectly() {
            String permFilter = "SELECT * FROM SysContent WHERE (sys_racl = '__Everyone__')";
            String hxql = service.buildFulltextQuery("revenue growth", permFilter);

            assertThat(hxql).startsWith("SELECT * FROM SysContent WHERE (" + FIELD + " = 'revenue'");
            assertThat(hxql).contains(FIELD + " = 'growth'");
            assertThat(hxql).contains("AND (sys_racl = '__Everyone__')");
        }

        @Test
        void buildFulltextQuery_nullPermissionFilter_fulltextOnly() {
            String hxql = service.buildFulltextQuery("revenue", null);
            assertThat(hxql).isEqualTo(
                    "SELECT * FROM SysContent WHERE " + FIELD + " = 'revenue'");
        }

        @Test
        void buildFulltextQuery_usesLikeNotEquality() {
            // The previous form queried cin_ingestProperties.contentLake_extractedText, which no
            // ingester wrote, so the leg measured a 0.0000 hit rate. Querying that property with
            // LIKE is also wrong: its index truncates at 256 chars. sys_fulltext has no such limit.
            String hxql = service.buildFulltextQuery("volatility", null);
            assertThat(hxql).contains(FIELD + " = 'volatility'");
        }

        @Test
        void buildFulltextQuery_oneClausePerTerm_orred() {
            String hxql = service.buildFulltextQuery("alpha beta gamma", null);
            assertThat(hxql).isEqualTo("SELECT * FROM SysContent WHERE ("
                    + FIELD + " = 'alpha' OR "
                    + FIELD + " = 'beta' OR "
                    + FIELD + " = 'gamma')");
        }

        @Test
        void buildFulltextQuery_lowercasesTerms() {
            // sys_fulltext matches case-insensitively, so this is normalisation for a stable query
            // string and for term de-duplication rather than a correctness requirement.
            String hxql = service.buildFulltextQuery("Severity RESPONSE", null);
            assertThat(hxql).contains(FIELD + " = 'severity'");
            assertThat(hxql).contains(FIELD + " = 'response'");
        }

        @Test
        void buildFulltextQuery_keepsSentinelIdentifiersIntact() {
            // The entire point of the keyword leg: an exact identifier must survive as one term,
            // hyphen included.
            String hxql = service.buildFulltextQuery("change CHG-105402", null);
            assertThat(hxql).contains(FIELD + " = 'chg-105402'");
        }

        @Test
        void buildFulltextQuery_dropsQuotesAndBackslashes() {
            // The parser cannot escape either inside a string literal.
            String hxql = service.buildFulltextQuery("company's backslash\\path", null);
            assertThat(hxql).doesNotContain("\\");
            assertThat(hxql).doesNotContain("'s ");
            assertThat(hxql).contains(FIELD + " = 'companys'");
            assertThat(hxql).contains(FIELD + " = 'backslashpath'");
        }

        @Test
        void buildFulltextQuery_stripsLikeWildcardsFromTerms() {
            // An unescaped % in a term would widen the pattern to match every document.
            String hxql = service.buildFulltextQuery("100% cover_age", null);
            assertThat(hxql).contains(FIELD + " = '100'");
            assertThat(hxql).contains(FIELD + " = 'coverage'");
            assertThat(hxql).doesNotContain("''");
        }

        @Test
        void buildFulltextQuery_dropsStopWordsAndShortTerms() {
            String hxql = service.buildFulltextQuery("what is the revenue", null);
            assertThat(hxql).contains(FIELD + " = 'revenue'");
            assertThat(hxql).doesNotContain(FIELD + " = 'the'");
            assertThat(hxql).doesNotContain(FIELD + " = 'what'");
            assertThat(hxql).doesNotContain(FIELD + " = 'is'");
        }

        @Test
        void buildFulltextQuery_deduplicatesRepeatedTerms() {
            String hxql = service.buildFulltextQuery("revenue revenue revenue", null);
            assertThat(hxql).isEqualTo(
                    "SELECT * FROM SysContent WHERE " + FIELD + " = 'revenue'");
        }

        @Test
        void buildFulltextQuery_noUsableTerm_fallsBackToPermissionFilter() {
            // A clause-less WHERE would hand the keyword leg the entire corpus.
            String permFilter = "SELECT * FROM SysContent WHERE (sys_racl = '__Everyone__')";
            assertThat(service.buildFulltextQuery("is a the", permFilter)).isEqualTo(permFilter);
            assertThat(service.buildFulltextQuery("", null)).isEqualTo("SELECT * FROM SysContent");
            assertThat(service.buildFulltextQuery(null, null)).isEqualTo("SELECT * FROM SysContent");
        }

        @Test
        void buildFulltextClause_nullWhenNothingUsable() {
            assertThat(service.buildFulltextClause("the is a")).isNull();
            assertThat(service.buildFulltextClause("   ")).isNull();
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

            assertThat(filter).contains("cin_ingestProperties.source_mimeType = 'application/pdf'");
            assertThat(filter).contains("(cin_ingestProperties.source_path >= '/Company Home/Sites/Finance' AND cin_ingestProperties.source_path < '/Company Home/Sites/Finance\uFFFF')");
            assertThat(filter).contains("cin_ingestProperties.source_modifiedAt >= '2026-01-01T00:00:00Z'");
            assertThat(filter).contains("cin_ingestProperties.source_modifiedAt <= '2026-12-31T23:59:59Z'");
            assertThat(filter).contains("cin_ingestProperties.cm:title = 'Budget 2026'");
        }

        @Test
        void buildMetadataFilter_escapesNxqlLiterals() {
            HybridSearchRequest.MetadataFilter metadata = HybridSearchRequest.MetadataFilter.builder()
                    .pathPrefix("C:\\Docs\\Owner's Manual")
                    .properties(Map.of("cm:title", "Owner's Manual"))
                    .build();

            String filter = service.buildMetadataFilter(metadata);

            assertThat(filter).contains("C:\\\\Docs\\\\Owner\\'s Manual");
            assertThat(filter).contains("cin_ingestProperties.cm:title = 'Owner\\'s Manual'");
        }

        @Test
        void buildMetadataFilter_invalidCustomPropertyKey_isIgnored() {
            HybridSearchRequest.MetadataFilter metadata = HybridSearchRequest.MetadataFilter.builder()
                    .properties(Map.of("bad key", "value"))
                    .build();

            String filter = service.buildMetadataFilter(metadata);
            assertThat(filter).isNull();
        }

        @Test
        void buildMetadataFilter_normalizesCustomPropertyValueThroughVocabulary() {
            // The same concept ("HR Documents") stored under different labels across repos must
            // filter on the canonical vocabulary key (#39).
            when(vocabularyService.resolve("HR Documents")).thenReturn("hr");
            HybridSearchRequest.MetadataFilter metadata = HybridSearchRequest.MetadataFilter.builder()
                    .properties(Map.of("cm:category", "HR Documents"))
                    .build();

            String filter = service.buildMetadataFilter(metadata);

            assertThat(filter).contains("cin_ingestProperties.cm:category = 'hr'");
        }
    }

    // -----------------------------------------------------------------------
    // Permission filter: this service only delegates now
    // -----------------------------------------------------------------------

    /**
     * Stubs the source discovery the permission filter runs: one terms aggregation over
     * {@code cin_sourceId}, whose bucket keys are the stored {@code <sourceType>:<sourceId>} values.
     */
    private void stubIndexedSources(String... qualifiedSourceIds) {
        HxprTermsAggregationResult aggregation = new HxprTermsAggregationResult();
        aggregation.setAggregationsBuckets(java.util.Arrays.stream(qualifiedSourceIds).map(key -> {
            HxprTermsAggregationResult.Bucket bucket = new HxprTermsAggregationResult.Bucket();
            bucket.setKey(key);
            bucket.setDocCount(1L);
            return bucket;
        }).toList());
        when(hxprService.termsAggregation(isNull(), eq("cin_sourceId"), isNull(), anyInt()))
                .thenReturn(aggregation);
    }

    @Test
    void buildCurrentUserPermissionFilter_passesTheCallersIdentitiesAndThisServicesSettings() {
        PermissionFilterBuilder builder = mock(PermissionFilterBuilder.class);
        ReflectionTestUtils.setField(service, "permissionFilterBuilder", builder);
        // A plain token carries no per-source identity, so the caller's name comes from here.
        when(securityContextService.getCurrentUsername()).thenReturn("alice");
        SecurityContextHolder.getContext().setAuthentication(
                new UsernamePasswordAuthenticationToken("alice", null,
                        List.of(new SimpleGrantedAuthority("ROLE_USER"))));
        when(builder.query(any(), any(), any(), any())).thenReturn("SELECT * FROM SysContent WHERE 1=1");

        try {
            String filter = service.buildCurrentUserPermissionFilter("alfresco", "cin_sourceId = 'x'");

            assertThat(filter).isEqualTo("SELECT * FROM SysContent WHERE 1=1");

            ArgumentCaptor<CallerIdentities> identities = ArgumentCaptor.forClass(CallerIdentities.class);
            ArgumentCaptor<PermissionFilterBuilder.Settings> settings =
                    ArgumentCaptor.forClass(PermissionFilterBuilder.Settings.class);
            verify(builder).query(identities.capture(), settings.capture(),
                    eq("alfresco"), eq("cin_sourceId = 'x'"));

            // The identity reaches the builder untyped, so it answers for every source as it always has.
            assertThat(identities.getValue().usernameFor("alfresco")).isEqualTo("alice");
            assertThat(identities.getValue().usernameFor("sharepoint")).isEqualTo("alice");
            // And the configured ids come from this service's own fields on every call.
            assertThat(settings.getValue().sources().sourceIdOf("alfresco")).isEqualTo("test-repo");
        } finally {
            SecurityContextHolder.clearContext();
        }
    }

    // -----------------------------------------------------------------------
    // logPermissionSourceIdConfiguration (startup validation)
    // -----------------------------------------------------------------------

    @Test
    void logPermissionSourceIdConfiguration_unset_skipsIndexProbe() {
        ReflectionTestUtils.setField(service, "permissionSourceIds", "");

        service.logPermissionSourceIdConfiguration();

        verify(hxprService, never()).query(anyString(), anyInt(), anyInt());
        verify(hxprService, never()).termsAggregation(any(), any(), any(), anyInt());
    }

    @Test
    void logPermissionSourceIdConfiguration_pinnedAndCovers_doesNotMisreport() {
        ReflectionTestUtils.setField(service, "permissionSourceIds", "covered-repo");

        stubIndexedSources("alfresco:covered-repo");

        service.logPermissionSourceIdConfiguration();

        verify(hxprService).termsAggregation(isNull(), eq("cin_sourceId"), isNull(), anyInt());
    }

    @Test
    void logPermissionSourceIdConfiguration_pinnedMissesIndexedAlfrescoSource_probesIndex() {
        // Mirrors the incident: pinned "default,local" misses the real Alfresco repo UUID.
        ReflectionTestUtils.setField(service, "permissionSourceIds", "default,local");

        stubIndexedSources("alfresco:de0b9044-4790-4006-8b90-44479030061f");

        service.logPermissionSourceIdConfiguration();

        verify(hxprService).termsAggregation(isNull(), eq("cin_sourceId"), isNull(), anyInt());
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
            doReturn(List.of()).when(svc).executeKeywordSearch(any(), any(), anyInt(), any(), any());

            HybridSearchRequest request = HybridSearchRequest.builder().query("test").build();
            HybridSearchResponse response = svc.search(request);

            assertThat(response.getResultCount()).isZero();
            assertThat(response.getStrategy()).isEqualTo("rrf");
            assertThat(response.getQuery()).isEqualTo("test");
        }

        @Test
        void search_chunkFtsEnabled_pushesTermsIntoVectorCallAndSkipsKeywordLeg() {
            when(properties.getStrategy()).thenReturn("rrf");
            when(properties.getCandidateCount()).thenReturn(20);
            when(properties.getMaxResults()).thenReturn(5);
            when(properties.getRrfK()).thenReturn(60);
            when(properties.getDefaultMinScore()).thenReturn(0.0);
            when(properties.isChunkFtsEnabled()).thenReturn(true);

            when(securityContextService.getCurrentUsername()).thenReturn("user");
            when(embeddingService.embedQuery(any())).thenReturn(List.of(0.1d, 0.2d));
            when(embeddingService.getModelName()).thenReturn("test-model");

            VectorSearchResult vectorResult = mock(VectorSearchResult.class);
            when(vectorResult.getEmbeddings()).thenReturn(List.of());
            // Chunk-FTS mode calls the 5-arg vectorSearch (with the chunkFTS term string).
            when(hxprService.vectorSearch(any(), any(), any(), any(), anyInt())).thenReturn(vectorResult);

            HybridSearchService svc = spy(service);

            svc.search(HybridSearchRequest.builder().query("alfresco").build());

            // Keyword terms travel as chunkFTS on the vector call...
            verify(hxprService).vectorSearch(any(), any(), any(), eq("alfresco"), anyInt());
            // ...and the separate BM25 keyword leg is not run.
            verify(svc, never()).executeKeywordSearch(any(), any(), anyInt(), any(), any());
        }

        @Test
        void search_withSourceType_addsGenericSourceFilterAndNarrowsAuthorities() {
            when(properties.getStrategy()).thenReturn("rrf");
            when(properties.getCandidateCount()).thenReturn(20);
            when(properties.getMaxResults()).thenReturn(5);
            when(properties.getRrfK()).thenReturn(60);
            when(properties.getDefaultMinScore()).thenReturn(0.0);

            when(securityContextService.getCurrentUsername()).thenReturn("user");
            when(embeddingService.embedQuery(any())).thenReturn(List.of(0.1d, 0.2d));
            when(embeddingService.getModelName()).thenReturn("test-model");

            VectorSearchResult vectorResult = mock(VectorSearchResult.class);
            when(vectorResult.getEmbeddings()).thenReturn(List.of());
            when(hxprService.vectorSearch(any(), any(), any(), anyInt())).thenReturn(vectorResult);

            HybridSearchService svc = spy(service);
            ReflectionTestUtils.setField(svc, "nuxeoSourceId", "nuxeo-demo");
            doReturn(List.of()).when(svc).executeKeywordSearch(any(), any(), anyInt(), any(), any());

            svc.search(HybridSearchRequest.builder()
                    .query("test")
                    .sourceType("nuxeo")
                    .build());

            verify(hxprService).vectorSearch(any(), any(), argThat(filter ->
                    filter.contains("cin_ingestProperties.source_type = 'nuxeo'")
                            && filter.contains("sys_racl = 'u:user_#_nuxeo-demo'")
                            && !filter.contains("u:user_#_test-repo")
            ), anyInt());
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
            doReturn(List.of()).when(svc).executeKeywordSearch(any(), any(), anyInt(), any(), any());

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
            doReturn(List.of()).when(svc).executeKeywordSearch(any(), any(), anyInt(), any(), any());

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
            doReturn(List.of()).when(svc).executeKeywordSearch(any(), any(), anyInt(), any(), any());

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
            doReturn(List.of()).when(svc).executeKeywordSearch(any(), any(), anyInt(), any(), any());

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
            doReturn(List.of()).when(svc).executeKeywordSearch(any(), any(), anyInt(), any(), any());

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
    // Document budget (#135)
    // -----------------------------------------------------------------------

    /**
     * {@code topDocuments} on the hybrid endpoint.
     *
     * <p>Document ids here are deliberately not uuid-shaped, so {@code fetchDocumentMetadata} skips them
     * and each hit carries the bare document id. Selection reads the id off the fused chunk, not off the
     * enriched metadata, which is what makes that possible.</p>
     */
    @Nested
    class DocumentBudget {

        /** One candidate chunk. Every getter read by the vector leg is stubbed, so none goes unused. */
        private Embedding chunkOf(String docId, String embeddingId) {
            Embedding emb = mock(Embedding.class);
            when(emb.getSysembedDocId()).thenReturn(docId);
            when(emb.getSysembedId()).thenReturn(embeddingId);
            when(emb.getSysembedText()).thenReturn(docId + "/" + embeddingId);
            return emb;
        }

        /**
         * A service whose vector leg returns {@code embeddings} and whose keyword leg returns nothing, so
         * the fused order is the candidate order. Built up front: nesting a when(...) inside another
         * when(...) leaves Mockito mid-stubbing.
         */
        private HybridSearchService stubbedService(List<Embedding> embeddings) {
            when(properties.getStrategy()).thenReturn("rrf");
            when(properties.getRrfK()).thenReturn(60);
            when(securityContextService.getCurrentUsername()).thenReturn("user");
            when(embeddingService.embedQuery(any())).thenReturn(List.of(0.1));
            when(embeddingService.getModelName()).thenReturn("test-model");

            VectorSearchResult result = mock(VectorSearchResult.class);
            when(result.getEmbeddings()).thenReturn(embeddings);
            when(hxprService.vectorSearch(any(), any(), any(), anyInt())).thenReturn(result);

            HybridSearchService svc = spy(service);
            doReturn(List.of()).when(svc).executeKeywordSearch(any(), any(), anyInt(), any(), any());
            return svc;
        }

        private List<String> documentIdsOf(HybridSearchResponse response) {
            return response.getResults().stream()
                    .map(hit -> hit.getSourceDocument().getDocumentId())
                    .toList();
        }

        @Test
        void search_withTopDocuments_returnsChunksOfThatManyDistinctDocuments() {
            HybridSearchService svc = stubbedService(List.of(
                    chunkOf("doc-a", "a1"),
                    chunkOf("doc-a", "a2"),
                    chunkOf("doc-b", "b1"),
                    chunkOf("doc-c", "c1")));

            HybridSearchResponse response = svc.search(HybridSearchRequest.builder()
                    .query("test")
                    .topDocuments(2)
                    .chunksPerDocument(1)
                    .build());

            assertThat(documentIdsOf(response)).containsExactly("doc-a", "doc-b");
            assertThat(response.getResultCount()).isEqualTo(2);
            assertThat(response.getDocumentCount()).isEqualTo(2);
            assertThat(response.getAppliedTopDocuments()).isEqualTo(2);
            assertThat(response.getAppliedChunksPerDocument()).isEqualTo(1);
        }

        /**
         * The one behaviour that separates a document budget from the per-document cap: a document outside
         * the budget is not admitted to fill the chunk budget, so the answer is short rather than wrong.
         */
        @Test
        void search_withTopDocuments_doesNotBackfillFromDocumentsOutsideTheBudget() {
            HybridSearchService svc = stubbedService(List.of(
                    chunkOf("doc-a", "a1"),
                    chunkOf("doc-a", "a2"),
                    chunkOf("doc-a", "a3"),
                    chunkOf("doc-b", "b1")));

            HybridSearchResponse response = svc.search(HybridSearchRequest.builder()
                    .query("test")
                    .topDocuments(1)
                    .chunksPerDocument(2)
                    .build());

            assertThat(documentIdsOf(response)).containsExactly("doc-a", "doc-a");
            assertThat(response.getDocumentCount()).isEqualTo(1);
        }

        /** With no per-request override the budget falls back to the configured cap, not to unlimited. */
        @Test
        void search_withNoPerDocumentOverride_usesTheConfiguredCap() {
            HybridSearchService svc = stubbedService(List.of(
                    chunkOf("doc-a", "a1"),
                    chunkOf("doc-a", "a2"),
                    chunkOf("doc-a", "a3"),
                    chunkOf("doc-b", "b1")));

            HybridSearchResponse response = svc.search(HybridSearchRequest.builder()
                    .query("test")
                    .topDocuments(1)
                    .build());

            assertThat(response.getResultCount()).isEqualTo(2);
            assertThat(response.getAppliedChunksPerDocument()).isEqualTo(2);
        }

        /** A document budget is unsatisfiable out of a pool the size of the answer, so the pool grows. */
        @Test
        void search_withTopDocuments_asksTheLegsPastTheConfiguredCandidateCount() {
            when(properties.getCandidateCount()).thenReturn(20);
            HybridSearchService svc = stubbedService(List.of());

            svc.search(HybridSearchRequest.builder()
                    .query("test")
                    .topDocuments(50)
                    .chunksPerDocument(10)
                    .build());

            // 50 x 10 caps at the 200-chunk ceiling, doubled so the selection has documents to choose between.
            verify(hxprService).vectorSearch(any(), any(), any(), eq(400));
        }

        @Test
        void search_withoutTopDocuments_asksTheLegsForTheConfiguredCandidateCount() {
            when(properties.getCandidateCount()).thenReturn(20);
            when(properties.getMaxResults()).thenReturn(5);
            HybridSearchService svc = stubbedService(List.of());

            svc.search(HybridSearchRequest.builder().query("test").build());

            verify(hxprService).vectorSearch(any(), any(), any(), eq(20));
        }

        /** Raising the pool must never lower it: a small budget keeps whatever the deployment configured. */
        @Test
        void search_withASmallDocumentBudget_doesNotShrinkTheCandidatePool() {
            when(properties.getCandidateCount()).thenReturn(100);
            HybridSearchService svc = stubbedService(List.of());

            svc.search(HybridSearchRequest.builder()
                    .query("test")
                    .topDocuments(2)
                    .chunksPerDocument(1)
                    .build());

            verify(hxprService).vectorSearch(any(), any(), any(), eq(100));
        }

        @Test
        void search_withoutTopDocuments_stillReportsDocumentCountAndNoAppliedBudget() {
            when(properties.getCandidateCount()).thenReturn(20);
            when(properties.getMaxResults()).thenReturn(10);
            HybridSearchService svc = stubbedService(List.of(
                    chunkOf("doc-a", "a1"),
                    chunkOf("doc-a", "a2"),
                    chunkOf("doc-b", "b1")));

            HybridSearchResponse response = svc.search(HybridSearchRequest.builder().query("test").build());

            assertThat(response.getResultCount()).isEqualTo(3);
            assertThat(response.getDocumentCount()).isEqualTo(2);
            assertThat(response.getAppliedTopDocuments()).isNull();
            assertThat(response.getAppliedChunksPerDocument()).isNull();
        }

        /** Without this the same query with and without a budget returns whichever ran first. */
        @Test
        void theDocumentBudgetIsPartOfTheCacheKey() {
            String chunks = cacheKey(HybridSearchRequest.builder().query("test").build());
            String documents = cacheKey(HybridSearchRequest.builder().query("test").topDocuments(10).build());
            String perDocument = cacheKey(HybridSearchRequest.builder()
                    .query("test").topDocuments(10).chunksPerDocument(3).build());

            assertThat(chunks).isNotEqualTo(documents);
            assertThat(documents).isNotEqualTo(perDocument);
        }

        private String cacheKey(HybridSearchRequest request) {
            return ReflectionTestUtils.invokeMethod(service, "buildCacheKey", request);
        }
    }

    // -----------------------------------------------------------------------
    // Keyword search extraction
    // -----------------------------------------------------------------------

    /**
     * The keyword leg reads chunks through the embeddings endpoint, not through a plain HXQL query.
     *
     * <p>A plain query returns documents with no chunks attached: chunk text lives in an
     * {@code embeddings.parquet} blob that only {@code /api/query/embeddings} decodes. The previous
     * implementation queried documents, found their embeddings null, skipped every one and returned
     * an empty list for every query, which is half of why {@code keyword_leg_hit_rate} measured
     * 0.0000. Passing the keyword HXQL as that endpoint's filter is what makes the leg able to
     * produce a chunk at all.</p>
     */
    @Nested
    class KeywordSearchExtraction {

        /**
         * Built with doReturn so a nested stub never sits inside an open when(...), and lenient
         * because a chunk that no query term matches is dropped before its ids are ever read.
         */
        private Embedding embedding(String docId, String chunkId, String text) {
            Embedding emb = mock(Embedding.class, withSettings().lenient());
            doReturn(docId).when(emb).getSysembedDocId();
            doReturn(chunkId).when(emb).getSysembedId();
            doReturn(text).when(emb).getSysembedText();
            return emb;
        }

        private void stubChunks(List<Embedding> embeddings) {
            VectorSearchResult result = mock(VectorSearchResult.class);
            doReturn(embeddings).when(result).getEmbeddings();
            doReturn(result).when(hxprService).vectorSearch(any(), any(), any(), anyInt());
        }

        @Test
        void executeKeywordSearch_scoresChunksByTermFrequency() {
            stubChunks(List.of(
                    embedding("doc-1", "c1", "This chunk contains alfresco content management"),
                    embedding("doc-1", "c2", "This chunk is about something else entirely")));

            List<ScoredChunk> chunks = service.executeKeywordSearch(
                    "alfresco content",
                    "SELECT * FROM SysContent WHERE (sys_racl = '__Everyone__')",
                    20,
                    List.of(0.1d, 0.2d));

            // Only the first chunk carries a query term; the second scores 0 and is dropped.
            assertThat(chunks).hasSize(1);
            assertThat(chunks.getFirst().text()).isEqualTo("This chunk contains alfresco content management");
            assertThat(chunks.getFirst().score()).isGreaterThan(0.0);
            assertThat(chunks.getFirst().rank()).isEqualTo(1);
        }

        @Test
        void executeKeywordSearch_passesTheKeywordClauseAsTheEmbeddingFilter() {
            stubChunks(List.of());

            service.executeKeywordSearch(
                    "alfresco content",
                    "SELECT * FROM SysContent WHERE (sys_racl = '__Everyone__')",
                    20,
                    List.of(0.1d, 0.2d));

            verify(hxprService).vectorSearch(any(), any(), argThat(filter ->
                    filter.contains("sys_fulltext = 'alfresco'")
                            && filter.contains("sys_fulltext = 'content'")
                            && filter.contains("sys_racl = '__Everyone__'")
            ), anyInt());
        }

        @Test
        void executeKeywordSearch_propagatesTheEmbeddingTypeToTheVectorCall() {
            // The keyword leg reads chunks through the embeddings endpoint; it must target the
            // same embeddingType as the vector leg, not the "*" wildcard, or kNN candidate
            // selection would mix incompatible embedding models (#42).
            stubChunks(List.of());

            service.executeKeywordSearch(
                    "alfresco", "SELECT * FROM SysContent", 20, List.of(0.1d, 0.2d), "text-embed-v2");

            verify(hxprService).vectorSearch(any(), eq("text-embed-v2"), any(), anyInt());
        }

        @Test
        void executeKeywordSearch_ranksByTermScoreNotByVectorOrder() {
            // The endpoint returns chunks in vector-similarity order, which carries no keyword
            // signal. The chunk mentioning the term twice must win regardless of arriving second.
            stubChunks(List.of(
                    embedding("doc-1", "c1", "alfresco appears once here among many other words"),
                    embedding("doc-2", "c2", "alfresco alfresco")));

            List<ScoredChunk> chunks = service.executeKeywordSearch(
                    "alfresco", "SELECT * FROM SysContent", 20, List.of(0.1d, 0.2d));

            assertThat(chunks).hasSize(2);
            assertThat(chunks.getFirst().docId()).isEqualTo("doc-2");
            assertThat(chunks.getFirst().score()).isGreaterThan(chunks.get(1).score());
            assertThat(chunks.get(1).rank()).isEqualTo(2);
        }

        @Test
        void executeKeywordSearch_reusesTheSuppliedQueryVector() {
            // Embedding the same query twice per search would double the inference cost of every
            // hybrid request, since the vector is only needed to read chunk text.
            stubChunks(List.of());

            service.executeKeywordSearch("alfresco", "SELECT * FROM SysContent", 20, List.of(0.1d, 0.2d));

            verify(embeddingService, never()).embedQuery(any());
        }

        @Test
        void executeKeywordSearch_embedsTheQueryWhenNoVectorIsSupplied() {
            doReturn(List.of(0.1d, 0.2d)).when(embeddingService).embedQuery("alfresco");
            stubChunks(List.of());

            service.executeKeywordSearch("alfresco", "SELECT * FROM SysContent", 20);

            verify(embeddingService).embedQuery("alfresco");
        }

        @Test
        void executeKeywordSearch_noUsableTerm_skipsTheLegEntirely() {
            // Falling back to the permission filter alone would hand the whole corpus to this leg
            // and let fusion promote arbitrary documents.
            List<ScoredChunk> chunks = service.executeKeywordSearch(
                    "is a the", "SELECT * FROM SysContent", 20, List.of(0.1d, 0.2d));

            assertThat(chunks).isEmpty();
            verify(hxprService, never()).vectorSearch(any(), any(), any(), anyInt());
        }

        @Test
        void executeKeywordSearch_emptyResult_returnsEmpty() {
            stubChunks(List.of());

            assertThat(service.executeKeywordSearch(
                    "alfresco", "filter", 20, List.of(0.1d, 0.2d))).isEmpty();
        }

        @Test
        void executeKeywordSearch_failureReturnsEmptyRatherThanFailingTheSearch() {
            // A dead keyword leg must degrade to vector-only, not fail the whole request.
            doThrow(new RuntimeException("connection error"))
                    .when(hxprService).vectorSearch(any(), any(), any(), anyInt());

            assertThat(service.executeKeywordSearch(
                    "alfresco", "filter", 20, List.of(0.1d, 0.2d))).isEmpty();
        }

        @Test
        void executeKeywordSearch_noEmbeddingAvailable_returnsEmpty() {
            doReturn(List.of()).when(embeddingService).embedQuery("alfresco");

            assertThat(service.executeKeywordSearch("alfresco", "filter", 20)).isEmpty();
            verify(hxprService, never()).vectorSearch(any(), any(), any(), anyInt());
        }
    }

    // -----------------------------------------------------------------------
    // Verbatim matching for identifier-like queries (#122)
    // -----------------------------------------------------------------------

    /**
     * A short alphanumeric identifier embeds poorly, and the keyword leg alone does not rescue it.
     * Query expansion cannot either: the token is rare, not ambiguous. The verbatim pass restricts the
     * search to chunks holding the token literally, and must never cost a result: a query that looks
     * token-like but matches nothing verbatim has to rank exactly as it would with the flag off.
     */
    @Nested
    class VerbatimIdentifier {

        private RagProperties.RetrievalProperties.VerbatimIdentifierProperties config;

        @BeforeEach
        void enableVerbatimPass() {
            RagProperties.RetrievalProperties retrieval = new RagProperties.RetrievalProperties();
            config = retrieval.getVerbatimIdentifier();
            config.setEnabled(true);
            lenient().when(ragProperties.getRetrieval()).thenReturn(retrieval);
            // The verbatim pass is bounded by the shared variant ceiling, so the real defaults are
            // needed; the ceiling test overrides this with a tighter one.
            lenient().when(ragProperties.getQueryExpansion())
                    .thenReturn(new RagProperties.QueryExpansionProperties());
        }

        private List<String> identifiersIn(String query) {
            return HybridSearchService.identifierTerms(query, config);
        }

        // --- the classifier ---

        @Test
        void aBareIdentifierFires() {
            assertThat(identifiersIn("CL3004")).containsExactly("cl3004");
        }

        /**
         * The corpus's own identifiers carry internal hyphens, so a rule rejecting all punctuation would
         * reject the very tokens this feature exists for.
         */
        @Test
        void anIdentifierWithInternalPunctuationFires() {
            assertThat(identifiersIn("CHG-105402")).containsExactly("chg-105402");
            assertThat(identifiersIn("AST-3121904")).containsExactly("ast-3121904");
            assertThat(identifiersIn("v1.4-beta2")).containsExactly("v1.4-beta2");
        }

        /**
         * The classifier accepts exactly what the chunk filter can express (#129). {@code _} is a
         * single-character wildcard in HXQL {@code LIKE}, so {@code sanitizeLikeTerm} strips it: a token
         * carrying one reaches hxpr as a different token and matches nothing, which would make the pass
         * a silent no-op rather than an honest miss.
         */
        @Test
        void anUnderscoreBearingTokenDoesNotFire() {
            assertThat(identifiersIn("nomic_embed.v1")).isEmpty();
            assertThat(identifiersIn("CHG_105402")).isEmpty();
        }

        /** The reason the classifier rejects it: the term the filter would carry is not the token. */
        @Test
        void theChunkFilterCannotExpressAnUnderscore() {
            assertThat(HybridSearchService.buildChunkFts("nomic_embed.v1")).isEqualTo("nomicembed.v1");
        }

        @Test
        void anIdentifierInsideAQuestionFires() {
            assertThat(identifiersIn("where is CHG-105402 recorded?"))
                    .containsExactly("chg-105402");
        }

        @Test
        void aProseQueryDoesNotFire() {
            assertThat(identifiersIn("Which change was reversed after it went wrong?")).isEmpty();
        }

        @Test
        void aTokenBelowTheMinimumLengthDoesNotFire() {
            assertThat(identifiersIn("A1")).isEmpty();
        }

        /** Letters alone are an acronym and digits alone are a number; neither is an identifier. */
        @Test
        void allLettersOrAllDigitsDoNotFire() {
            assertThat(identifiersIn("OAUTH")).isEmpty();
            assertThat(identifiersIn("105402")).isEmpty();
        }

        @Test
        void internalSentencePunctuationDisqualifiesAToken() {
            assertThat(identifiersIn("CHG:105402")).isEmpty();
            assertThat(identifiersIn("what/is1")).isEmpty();
        }

        @Test
        void trailingPunctuationIsTrimmedRatherThanDisqualifying() {
            assertThat(identifiersIn("CL3004?")).containsExactly("cl3004");
            assertThat(identifiersIn("(CL3004)")).containsExactly("cl3004");
        }

        @Test
        void theTermCapIsHonoured() {
            config.setMaxTerms(2);

            assertThat(identifiersIn("CL3004 CL1006 CHG-105402 AST-3121904")).hasSize(2);
        }

        @Test
        void duplicateIdentifiersAreCollapsed() {
            assertThat(identifiersIn("CL3004 and CL3004 again")).containsExactly("cl3004");
        }

        // --- the pass ---

        private HybridSearchService stubbedSearch() {
            when(properties.getStrategy()).thenReturn("rrf");
            when(properties.getCandidateCount()).thenReturn(20);
            when(properties.getMaxResults()).thenReturn(5);
            when(properties.getRrfK()).thenReturn(60);
            when(properties.getDefaultMinScore()).thenReturn(0.0);
            when(securityContextService.getCurrentUsername()).thenReturn("user");
            when(embeddingService.embedQuery(any())).thenReturn(List.of(0.1d, 0.2d));
            lenient().when(embeddingService.getModelName()).thenReturn("test-model");

            HybridSearchService svc = spy(service);
            doReturn(List.of()).when(svc).executeKeywordSearch(any(), any(), anyInt(), any(), any());
            return svc;
        }

        private VectorSearchResult emptyResult() {
            VectorSearchResult result = mock(VectorSearchResult.class, withSettings().lenient());
            doReturn(List.of()).when(result).getEmbeddings();
            return result;
        }

        /**
         * doReturn rather than when(...).thenReturn(...): the result is itself a mock, and building one
         * inside an open when(...) leaves an unfinished stubbing that fails the next interaction.
         */
        private void stubUnrestricted(VectorSearchResult result) {
            doReturn(result).when(hxprService).vectorSearch(any(), any(), any(), anyInt());
        }

        private void stubRestricted(VectorSearchResult result) {
            doReturn(result).when(hxprService).vectorSearch(any(), any(), any(), any(), anyInt());
        }

        /**
         * The restriction is expressed through {@code VectorQuery.chunkFTS}, which is the only
         * server-side lever that filters at the chunk level. HXQL {@code LIKE} fails on keyword fields
         * and a {@code sys_fulltext} predicate selects documents, so neither can say "chunks containing
         * this token".
         */
        @Test
        void anIdentifierQueryAddsAChunkRestrictedPass() {
            HybridSearchService svc = stubbedSearch();
            stubUnrestricted(emptyResult());
            stubRestricted(emptyResult());

            HybridSearchResponse response =
                    svc.search(HybridSearchRequest.builder().query("where is CHG-105402").build());

            // The original variant runs unrestricted...
            verify(hxprService).vectorSearch(any(), any(), any(), anyInt());
            // ...and the verbatim pass restricts to chunks holding the identifier.
            verify(hxprService).vectorSearch(any(), any(), any(), eq("chg-105402"), anyInt());
            assertThat(response.getQueryVariants()).isEqualTo(2);
        }

        /** No second embedding call: the verbatim pass reuses the query vector already computed. */
        @Test
        void theVerbatimPassCostsNoExtraEmbeddingCall() {
            HybridSearchService svc = stubbedSearch();
            stubUnrestricted(emptyResult());
            stubRestricted(emptyResult());

            svc.search(HybridSearchRequest.builder().query("CHG-105402").build());

            verify(embeddingService, times(1)).embedQuery(any());
        }

        @Test
        void aProseQueryAddsNoPass() {
            HybridSearchService svc = stubbedSearch();
            stubUnrestricted(emptyResult());

            HybridSearchResponse response = svc.search(
                    HybridSearchRequest.builder().query("Which change was reversed?").build());

            verify(hxprService, never()).vectorSearch(any(), any(), any(), any(), anyInt());
            assertThat(response.getQueryVariants()).isNull();
        }

        /**
         * End to end for #129: a query naming an underscore-bearing token runs the original variant
         * only. Before, it added a restricted pass whose filter had the underscore stripped, so the pass
         * matched nothing and the feature quietly did not apply to that class of identifier.
         */
        @Test
        void anUnderscoreBearingTokenAddsNoPass() {
            HybridSearchService svc = stubbedSearch();
            stubUnrestricted(emptyResult());

            HybridSearchResponse response = svc.search(
                    HybridSearchRequest.builder().query("where is nomic_embed.v1 configured").build());

            verify(hxprService, never()).vectorSearch(any(), any(), any(), any(), anyInt());
            assertThat(response.getQueryVariants()).isNull();
        }

        @Test
        void withTheFlagOffAnIdentifierQueryAddsNoPass() {
            config.setEnabled(false);
            HybridSearchService svc = stubbedSearch();
            stubUnrestricted(emptyResult());

            HybridSearchResponse response =
                    svc.search(HybridSearchRequest.builder().query("CHG-105402").build());

            verify(hxprService, never()).vectorSearch(any(), any(), any(), any(), anyInt());
            assertThat(response.getQueryVariants()).isNull();
        }

        /**
         * The acceptance criterion that matters most: a verbatim pass that finds nothing must leave the
         * ranking exactly as it was. It comes out of the variant structure rather than a special case,
         * since a variant contributing no results is not a contributor to cross-variant fusion.
         */
        @Test
        void aVerbatimPassThatMatchesNothingLeavesTheRankingUnchanged() {
            Embedding hit = mock(Embedding.class, withSettings().lenient());
            doReturn("doc-1").when(hit).getSysembedDocId();
            doReturn("c1").when(hit).getSysembedId();
            doReturn("a chunk with no identifier in it").when(hit).getSysembedText();
            doReturn(0.9d).when(hit).getSysembedScore();

            VectorSearchResult unrestricted = mock(VectorSearchResult.class, withSettings().lenient());
            doReturn(List.of(hit)).when(unrestricted).getEmbeddings();

            HybridSearchService svc = stubbedSearch();
            stubUnrestricted(unrestricted);
            stubRestricted(emptyResult());

            HybridSearchResponse withPass =
                    svc.search(HybridSearchRequest.builder().query("CHG-105402").build());

            assertThat(withPass.getResultCount()).isEqualTo(1);
            assertThat(withPass.getResults().get(0).getChunkText())
                    .isEqualTo("a chunk with no identifier in it");
        }

        @Test
        void aVerbatimHitIsReturnedAlongsideTheUnrestrictedOnes() {
            Embedding verbatimHit = mock(Embedding.class, withSettings().lenient());
            doReturn("doc-2").when(verbatimHit).getSysembedDocId();
            doReturn("c2").when(verbatimHit).getSysembedId();
            doReturn("Change CHG-105402 was reverted").when(verbatimHit).getSysembedText();
            doReturn(0.5d).when(verbatimHit).getSysembedScore();

            VectorSearchResult restricted = mock(VectorSearchResult.class, withSettings().lenient());
            doReturn(List.of(verbatimHit)).when(restricted).getEmbeddings();

            HybridSearchService svc = stubbedSearch();
            stubUnrestricted(emptyResult());
            stubRestricted(restricted);

            HybridSearchResponse response =
                    svc.search(HybridSearchRequest.builder().query("CHG-105402").build());

            assertThat(response.getResults()).hasSize(1);
            assertThat(response.getResults().get(0).getChunkText())
                    .isEqualTo("Change CHG-105402 was reverted");
        }

        /** The shared variant ceiling applies: the verbatim pass does not get to exceed it. */
        @Test
        void theVariantCeilingIsRespected() {
            RagProperties.QueryExpansionProperties ceiling = new RagProperties.QueryExpansionProperties();
            ceiling.setMaxVariants(1);
            when(ragProperties.getQueryExpansion()).thenReturn(ceiling);
            HybridSearchService svc = stubbedSearch();
            stubUnrestricted(emptyResult());

            svc.search(HybridSearchRequest.builder().query("CHG-105402").build());

            verify(hxprService, never()).vectorSearch(any(), any(), any(), any(), anyInt());
        }
    }
}
