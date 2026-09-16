package org.hyland.contentlake.rag.service;

import org.hyland.contentlake.client.HxprService;
import org.hyland.contentlake.hxpr.api.model.Embedding;
import org.hyland.contentlake.hxpr.api.model.VectorSearchResult;
import org.hyland.contentlake.model.HxprDocument;
import org.hyland.contentlake.model.HxprTermsAggregationResult;
import org.hyland.contentlake.rag.config.RagProperties;
import org.hyland.contentlake.rag.model.SemanticSearchRequest;
import org.hyland.contentlake.rag.model.SemanticSearchResponse;
import org.hyland.contentlake.rag.security.SourceGroupResolverRegistry;
import org.hyland.contentlake.security.GroupResolutionFailurePolicy;
import org.hyland.contentlake.security.SecurityContextService;
import org.hyland.contentlake.security.SourceGroupResolver;
import org.hyland.contentlake.service.EmbeddingService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class SemanticSearchServiceTest {

    @Mock HxprService hxprService;
    @Mock EmbeddingService embeddingService;
    @Mock SecurityContextService securityContextService;
    @Mock SourceMetadataResolver sourceMetadataResolver;
    @Mock SectionMapResolver sectionMapResolver;
    @Mock QueryExpansionService queryExpansionService;
    @Mock RagProperties ragProperties;
    @Mock org.hyland.contentlake.client.NamedQueryService namedQueryService;

    @InjectMocks SemanticSearchService service;

    @BeforeEach
    void setUp() {
        ReflectionTestUtils.setField(service, "alfrescoSourceId", "test-repo");
        ReflectionTestUtils.setField(service, "permissionSourceIds", "");
        ReflectionTestUtils.setField(service, "nuxeoSourceId", "");
        ReflectionTestUtils.setField(service, "defaultMinScore", 0.5d);
    }

    /** A resolver for the {@code alfresco} type whose single answer is whatever the supplier does. */
    private static SourceGroupResolver alfrescoResolver(Supplier<List<String>> answer) {
        return new SourceGroupResolver() {
            @Override
            public String sourceType() {
                return "alfresco";
            }

            @Override
            public List<String> resolveGroups(String username) {
                return answer.get();
            }
        };
    }

    private void withResolvers(SourceGroupResolver... resolvers) {
        withRegistry(GroupResolutionFailurePolicy.FAIL_CLOSED, resolvers);
    }

    /** Wires a registry with no cache, so each test's resolver answer is the one that is read. */
    private void withRegistry(GroupResolutionFailurePolicy policy, SourceGroupResolver... resolvers) {
        ReflectionTestUtils.setField(service, "groupResolverRegistry",
                new SourceGroupResolverRegistry(List.of(resolvers), policy, 0L, 0L, null));
    }

    /**
     * Stubs the source discovery the permission filter runs: one terms aggregation over
     * {@code cin_sourceId}, whose bucket keys are the stored {@code <sourceType>:<sourceId>} values.
     */
    private void stubIndexedSources(String... qualifiedSourceIds) {
        HxprTermsAggregationResult aggregation = new HxprTermsAggregationResult();
        aggregation.setAggregationsBuckets(Arrays.stream(qualifiedSourceIds).map(key -> {
            HxprTermsAggregationResult.Bucket bucket = new HxprTermsAggregationResult.Bucket();
            bucket.setKey(key);
            bucket.setDocCount(1L);
            return bucket;
        }).toList());
        when(hxprService.termsAggregation(isNull(), eq("cin_sourceId"), isNull(), anyInt()))
                .thenReturn(aggregation);
    }

    // -----------------------------------------------------------------------
    // Permission filter
    // -----------------------------------------------------------------------

    @Test
    void buildPermissionFilter_adminUser_includesEveryoneAndUsername() {
        SemanticSearchService svc = spy(service);
        doReturn(List.of("admin", "GROUP_EVERYONE")).when(svc).getUserAuthorities("admin", "test-repo");

        String filter = svc.buildPermissionFilter("admin", null);

        // __Everyone__ is always included
        assertThat(filter).contains("sys_racl = '__Everyone__'");
        assertThat(filter).contains("sys_racl = 'u:admin_#_test-repo'");
        // GROUP_EVERYONE itself is skipped (not added as a clause)
        assertThat(filter).doesNotContain("GROUP_EVERYONE");
    }

    @Test
    void buildPermissionFilter_userWithGroups_includesGroupRaclFormat() {
        SemanticSearchService svc = spy(service);
        doReturn(List.of("alice", "GROUP_EVERYONE", "GROUP_DEVELOPERS"))
                .when(svc).getUserAuthorities("alice", "test-repo");

        String filter = svc.buildPermissionFilter("alice", null);

        // Groups are prefixed with "g:" in sys_racl
        assertThat(filter).contains("sys_racl = 'g:GROUP_DEVELOPERS_#_test-repo'");
        // Username also included
        assertThat(filter).contains("sys_racl = 'u:alice_#_test-repo'");
    }

    @Test
    void buildPermissionFilter_withAdditionalFilter_combinesWithAnd() {
        SemanticSearchService svc = spy(service);
        doReturn(List.of("alice")).when(svc).getUserAuthorities("alice", "my-repo");

        String filter = svc.buildPermissionFilter("alice", "cin_sourceId = 'my-repo'");

        assertThat(filter).contains(" AND ");
        assertThat(filter).contains("cin_sourceId = 'my-repo'");
    }

    @Test
    void buildPermissionFilter_withSourceFilter_usesFilteredSourceId() {
        SemanticSearchService svc = spy(service);
        doReturn(List.of("alice")).when(svc).getUserAuthorities("alice", "nuxeo-demo");

        String filter = svc.buildPermissionFilter("alice", "cin_sourceId = 'nuxeo:nuxeo-demo'");

        assertThat(filter).contains("sys_racl = 'u:alice_#_nuxeo-demo'");
        assertThat(filter).doesNotContain("u:alice_#_test-repo");
    }

    @Test
    void buildPermissionFilter_withConfiguredExtraSourceIds_includesAllNamespaces() {
        SemanticSearchService svc = spy(service);
        ReflectionTestUtils.setField(svc, "permissionSourceIds", "test-repo,nuxeo-demo");
        doReturn(List.of("alice", "GROUP_DEVELOPERS")).when(svc).getUserAuthorities("alice", "test-repo");
        doReturn(List.of("alice", "GROUP_ENGINEERING")).when(svc).getUserAuthorities("alice", "nuxeo-demo");

        String filter = svc.buildPermissionFilter("alice", null);

        assertThat(filter).contains("sys_racl = 'g:GROUP_DEVELOPERS_#_test-repo'");
        assertThat(filter).contains("sys_racl = 'u:alice_#_nuxeo-demo'");
        assertThat(filter).contains("sys_racl = 'g:GROUP_ENGINEERING_#_nuxeo-demo'");
        assertThat(filter).doesNotContain("g:GROUP_ENGINEERING_#_test-repo'");
    }

    @Test
    void buildPermissionFilter_withSourceType_usesOnlyMatchingSourceId() {
        SemanticSearchService svc = spy(service);
        ReflectionTestUtils.setField(svc, "nuxeoSourceId", "nuxeo-demo");
        doReturn(List.of("alice")).when(svc).getUserAuthorities("alice", "nuxeo-demo");

        String filter = svc.buildPermissionFilter("alice", "nuxeo", null);

        assertThat(filter).contains("sys_racl = 'u:alice_#_nuxeo-demo'");
        assertThat(filter).doesNotContain("u:alice_#_test-repo");
    }

    @Test
    void buildPermissionFilter_alfrescoAdminDoesNotRestrictToAdminAuthorities() {
        SemanticSearchService svc = spy(service);
        ReflectionTestUtils.setField(svc, "adminBypassEnabled", true);
        doReturn(List.of("admin", "GROUP_EVERYONE", "GROUP_ALFRESCO_ADMINISTRATORS"))
                .when(svc).getUserAuthorities("admin", "test-repo");

        String filter = svc.buildPermissionFilter("admin", "alfresco", null);

        assertThat(filter).contains("cin_sourceId = 'alfresco:test-repo'");
        assertThat(filter).doesNotContain("sys_racl = 'u:admin_#_test-repo'");
        assertThat(filter).doesNotContain("g:GROUP_ALFRESCO_ADMINISTRATORS_#_test-repo");
    }

    @Test
    void buildPermissionFilter_adminBypassOffByDefault_alfrescoAdminIsAclFilteredLikeAnyoneElse() {
        SemanticSearchService svc = spy(service);
        doReturn(List.of("admin", "GROUP_EVERYONE", "GROUP_ALFRESCO_ADMINISTRATORS"))
                .when(svc).getUserAuthorities("admin", "test-repo");

        String filter = svc.buildPermissionFilter("admin", "alfresco", null);

        // No unconditional source clause: the administrator reads through sys_racl like everyone else.
        assertThat(filter).doesNotContain("cin_sourceId = 'alfresco:test-repo'");
        assertThat(filter).contains("sys_racl = 'u:admin_#_test-repo'");
        assertThat(filter).contains("sys_racl = '__Everyone__'");
        // The group is namespaced like any other, so it grants only what documents actually name.
        assertThat(filter).contains("sys_racl = 'g:GROUP_ALFRESCO_ADMINISTRATORS_#_test-repo'");
    }

    @Test
    void buildPermissionFilter_discoversAlfrescoSourceIdFromTheIndex() {
        SemanticSearchService svc = spy(service);
        ReflectionTestUtils.setField(svc, "alfrescoSourceId", "");
        ReflectionTestUtils.setField(svc, "adminBypassEnabled", true);

        stubIndexedSources("alfresco:discovered-repo");
        doReturn(List.of("admin", "GROUP_EVERYONE", "GROUP_ALFRESCO_ADMINISTRATORS"))
                .when(svc).getUserAuthorities("admin", "discovered-repo");

        String filter = svc.buildPermissionFilter("admin", "alfresco", null);

        assertThat(filter).contains("cin_sourceId = 'alfresco:discovered-repo'");
        assertThat(filter).doesNotContain("cin_sourceId = 'alfresco:test-repo'");
    }

    // -----------------------------------------------------------------------
    // A source rag-service was not compiled against (#133)
    // -----------------------------------------------------------------------

    @Test
    void buildPermissionFilter_thirdSourceInTheIndex_getsAClauseWithoutAPin() {
        SemanticSearchService svc = spy(service);
        ReflectionTestUtils.setField(svc, "alfrescoSourceId", "");
        stubIndexedSources("sample-directory:sample-directory");
        doReturn(List.of("alice", "GROUP_EVERYONE")).when(svc).getUserAuthorities("alice", "sample-directory");

        String filter = svc.buildPermissionFilter("alice", null);

        // Before #133 nothing named this source, so the filter was unresolvedSourceClause() and every
        // query returned nothing at all.
        assertThat(filter).doesNotContain("__unresolved_permission_source__");
        assertThat(filter).contains("sys_racl = '__Everyone__'");
        assertThat(filter).contains("sys_racl = 'u:alice_#_sample-directory'");
    }

    @Test
    void buildPermissionFilter_thirdSourceWithAdminBypass_qualifiesTheSourceIdWithItsOwnType() {
        SemanticSearchService svc = spy(service);
        ReflectionTestUtils.setField(svc, "alfrescoSourceId", "");
        ReflectionTestUtils.setField(svc, "adminBypassEnabled", true);
        stubIndexedSources("cmis:docbase-1");
        // The bypass group is Alfresco's and grants nothing here, so this asserts the qualified form
        // rather than the bypass: a bare id could never match a stored '<type>:<id>'.
        doReturn(List.of("admin", "GROUP_EVERYONE", "GROUP_ALFRESCO_ADMINISTRATORS"))
                .when(svc).getUserAuthorities("admin", "docbase-1");

        String filter = svc.buildPermissionFilter("admin", null);

        assertThat(filter).doesNotContain("cin_sourceId = 'docbase-1'");
        assertThat(filter).contains("sys_racl = 'u:admin_#_docbase-1'");
        assertThat(filter).contains("sys_racl = 'g:GROUP_ALFRESCO_ADMINISTRATORS_#_docbase-1'");
    }

    @Test
    void getUserAuthorities_thirdSource_resolvesDefaultsOnlyAndDoesNotCallADirectory() {
        // No resolver claims that source's type, so the caller gets themselves and Everyone: its public
        // documents are retrievable and its group-granted ones stay hidden.
        withResolvers(alfrescoResolver(() -> List.of("GROUP_UNREACHABLE")));

        assertThat(service.getUserAuthorities("alice", "sample-directory"))
                .containsExactly("alice", "GROUP_EVERYONE");
    }

    @Test
    void getUserAuthorities_resolverKnowsTheUser_addsTheirGroups() {
        withResolvers(alfrescoResolver(() -> List.of("GROUP_DEVELOPERS")));

        assertThat(service.getUserAuthorities("alice", "test-repo"))
                .containsExactly("alice", "GROUP_EVERYONE", "GROUP_DEVELOPERS");
    }

    @Test
    void getUserAuthorities_resolverHasNoSuchIdentity_keepsTheSourceWithDefaults() {
        // null is "not in this directory", which is not a directory failure and must not cost the source.
        withResolvers(alfrescoResolver(() -> null));

        assertThat(service.getUserAuthorities("alice", "test-repo"))
                .containsExactly("alice", "GROUP_EVERYONE");
    }

    // -----------------------------------------------------------------------
    // Group resolution failure policy
    // -----------------------------------------------------------------------

    @Test
    void getUserAuthorities_lookupFails_failClosed_resolvesNoAuthorities() {
        withRegistry(GroupResolutionFailurePolicy.FAIL_CLOSED, alfrescoResolver(() -> {
            throw new IllegalStateException("directory down");
        }));

        assertThat(service.getUserAuthorities("alice", "test-repo")).isEmpty();
    }

    @Test
    void getUserAuthorities_lookupFails_degrade_keepsUsernameAndEveryone() {
        withRegistry(GroupResolutionFailurePolicy.DEGRADE, alfrescoResolver(() -> {
            throw new IllegalStateException("directory down");
        }));

        assertThat(service.getUserAuthorities("alice", "test-repo"))
                .containsExactly("alice", "GROUP_EVERYONE");
    }

    @Test
    void getUserAuthorities_noRegistryWired_resolvesDefaultsForEverySource() {
        // A service constructed without the registry, as several tests here do, must still answer.
        assertThat(service.getUserAuthorities("alice", "test-repo"))
                .containsExactly("alice", "GROUP_EVERYONE");
    }

    @Test
    void buildPermissionFilter_unresolvedAuthorities_excludesTheSourceAndMatchesNothing() {
        SemanticSearchService svc = spy(service);
        doReturn(List.of()).when(svc).getUserAuthorities("alice", "test-repo");

        String filter = svc.buildPermissionFilter("alice", null);

        // No clause at all would match every document, so the sentinel has to take its place.
        assertThat(filter).contains("cin_sourceId = '__unresolved_permission_source__'");
        assertThat(filter).doesNotContain("sys_racl");
    }

    @Test
    void buildPermissionFilter_oneSourceUnresolved_keepsTheOtherAndDropsOnlyThatSource() {
        SemanticSearchService svc = spy(service);
        ReflectionTestUtils.setField(svc, "permissionSourceIds", "test-repo,nuxeo-demo");
        ReflectionTestUtils.setField(svc, "nuxeoSourceId", "nuxeo-demo");
        doReturn(List.of()).when(svc).getUserAuthorities("alice", "test-repo");
        doReturn(List.of("alice", "GROUP_MEMBERS")).when(svc).getUserAuthorities("alice", "nuxeo-demo");

        String filter = svc.buildPermissionFilter("alice", null);

        assertThat(filter).contains("sys_racl = 'u:alice_#_nuxeo-demo'");
        assertThat(filter).contains("sys_racl = 'g:GROUP_MEMBERS_#_nuxeo-demo'");
        assertThat(filter).doesNotContain("test-repo");
    }

    @Test
    void buildPermissionFilter_dualAuth_unresolvedSourceIsNotGivenDefaultAuthorities() {
        SemanticSearchService svc = spy(service);
        ReflectionTestUtils.setField(svc, "permissionSourceIds", "test-repo,nuxeo-demo");
        ReflectionTestUtils.setField(svc, "nuxeoSourceId", "nuxeo-demo");
        doReturn(List.of()).when(svc).getUserAuthorities("alice", "test-repo");
        doReturn(List.of("bob")).when(svc).getUserAuthorities("bob", "nuxeo-demo");

        String filter = svc.buildPermissionFilter("alice", "bob", null, null);

        assertThat(filter).contains("sys_racl = 'u:bob_#_nuxeo-demo'");
        assertThat(filter).doesNotContain("u:alice_#_test-repo");
    }

    // -----------------------------------------------------------------------
    // logPermissionSourceIdConfiguration (startup validation)
    // -----------------------------------------------------------------------

    @Test
    void logPermissionSourceIdConfiguration_unset_skipsIndexProbe() {
        ReflectionTestUtils.setField(service, "permissionSourceIds", "");

        service.logPermissionSourceIdConfiguration();

        // Discovery path: no validation probe against the index at startup, by either route.
        verify(hxprService, never()).query(anyString(), anyInt(), anyInt());
        verify(hxprService, never()).termsAggregation(any(), any(), any(), anyInt());
    }

    @Test
    void logPermissionSourceIdConfiguration_pinnedAndCovers_doesNotMisreport() {
        ReflectionTestUtils.setField(service, "permissionSourceIds", "covered-repo");

        stubIndexedSources("alfresco:covered-repo");

        // Should validate against the index without throwing; configured id covers the indexed one.
        service.logPermissionSourceIdConfiguration();

        verify(hxprService).termsAggregation(isNull(), eq("cin_sourceId"), isNull(), anyInt());
    }

    @Test
    void logPermissionSourceIdConfiguration_pinnedMissesIndexedAlfrescoSource_probesIndex() {
        // Mirrors the incident: pinned "default,local" misses the real Alfresco repo UUID.
        ReflectionTestUtils.setField(service, "permissionSourceIds", "default,local");

        stubIndexedSources("alfresco:de0b9044-4790-4006-8b90-44479030061f");

        // Does not throw; the uncovered indexed source id triggers the WARN diagnostic.
        service.logPermissionSourceIdConfiguration();

        verify(hxprService).termsAggregation(isNull(), eq("cin_sourceId"), isNull(), anyInt());
    }

    @Test
    void buildPermissionFilter_mixedSources_keepsAlfrescoAdminBypassScopedToAlfresco() {
        SemanticSearchService svc = spy(service);
        ReflectionTestUtils.setField(svc, "permissionSourceIds", "test-repo,nuxeo-demo");
        ReflectionTestUtils.setField(svc, "nuxeoSourceId", "nuxeo-demo");
        ReflectionTestUtils.setField(svc, "adminBypassEnabled", true);
        doReturn(List.of("admin", "GROUP_EVERYONE", "GROUP_ALFRESCO_ADMINISTRATORS"))
                .when(svc).getUserAuthorities("admin", "test-repo");
        doReturn(List.of("admin", "GROUP_MEMBERS"))
                .when(svc).getUserAuthorities("admin", "nuxeo-demo");

        String filter = svc.buildPermissionFilter("admin", null, null);

        assertThat(filter).contains("cin_sourceId = 'alfresco:test-repo'");
        assertThat(filter).contains("sys_racl = 'g:GROUP_MEMBERS_#_nuxeo-demo'");
        assertThat(filter).doesNotContain("sys_racl = 'u:admin_#_test-repo'");
        assertThat(filter).doesNotContain("g:GROUP_ALFRESCO_ADMINISTRATORS_#_test-repo");
    }

    @Test
    void buildPermissionFilter_adminBypassOn_doesNotLeakIntoANuxeoSource() {
        // The bypass is an Alfresco repository concept. Enabling it must not turn the same group name
        // into full access on a source that has no notion of it.
        SemanticSearchService svc = spy(service);
        ReflectionTestUtils.setField(svc, "permissionSourceIds", "nuxeo-demo");
        ReflectionTestUtils.setField(svc, "nuxeoSourceId", "nuxeo-demo");
        ReflectionTestUtils.setField(svc, "adminBypassEnabled", true);
        doReturn(List.of("admin", "GROUP_ALFRESCO_ADMINISTRATORS"))
                .when(svc).getUserAuthorities("admin", "nuxeo-demo");

        String filter = svc.buildPermissionFilter("admin", null, null);

        assertThat(filter).doesNotContain("cin_sourceId = 'nuxeo:nuxeo-demo'");
        assertThat(filter).contains("sys_racl = 'u:admin_#_nuxeo-demo'");
    }

    // -----------------------------------------------------------------------
    // looksLikeUuid helper
    // -----------------------------------------------------------------------

    @Test
    void looksLikeUuid_validUuid_returnsTrue() {
        assertThat(SemanticSearchService.looksLikeUuid("550e8400-e29b-41d4-a716-446655440000")).isTrue();
        assertThat(SemanticSearchService.looksLikeUuid("00000000-0000-0000-0000-000000000000")).isTrue();
    }

    @Test
    void looksLikeUuid_shortString_returnsFalse() {
        assertThat(SemanticSearchService.looksLikeUuid("abc123")).isFalse();
        assertThat(SemanticSearchService.looksLikeUuid("not-a-uuid")).isFalse();
        assertThat(SemanticSearchService.looksLikeUuid("")).isFalse();
    }

    @Test
    void looksLikeUuid_null_returnsFalse() {
        assertThat(SemanticSearchService.looksLikeUuid(null)).isFalse();
    }

    // -----------------------------------------------------------------------
    // search() behaviour
    // -----------------------------------------------------------------------

    @Test
    void search_emptyEmbedding_returnsEmptyResponse() {
        when(embeddingService.embedQuery(any())).thenReturn(List.of());
        when(embeddingService.getModelName()).thenReturn("test-model");

        SemanticSearchRequest request = SemanticSearchRequest.builder().query("test").build();
        SemanticSearchResponse response = service.search(request);

        assertThat(response.getResultCount()).isZero();
        assertThat(response.getTotalCount()).isZero();
        assertThat(response.getResults()).isEmpty();
        assertThat(response.getQuery()).isEqualTo("test");
    }

    @Test
    void search_noResults_returnsEmptyResponse() {
        SemanticSearchService svc = spy(service);
        doReturn(List.of("user")).when(svc).getUserAuthorities(anyString(), anyString());

        when(securityContextService.getCurrentUsername()).thenReturn("user");
        when(embeddingService.embedQuery(any())).thenReturn(List.of(0.1d, 0.2d));
        when(embeddingService.getModelName()).thenReturn("test-model");
        when(hxprService.vectorSearch(any(), any(), any(), anyInt())).thenReturn(null);

        SemanticSearchRequest request = SemanticSearchRequest.builder().query("test").build();
        SemanticSearchResponse response = svc.search(request);

        assertThat(response.getResults()).isEmpty();
        assertThat(response.getResultCount()).isZero();
    }

    @Test
    void search_minScoreFiltering_excludesLowScoringResults() {
        SemanticSearchService svc = spy(service);
        doReturn(List.of("user")).when(svc).getUserAuthorities(anyString(), anyString());

        when(securityContextService.getCurrentUsername()).thenReturn("user");
        when(embeddingService.embedQuery(any())).thenReturn(List.of(0.1d, 0.2d));
        when(embeddingService.getModelName()).thenReturn("test-model");

        Embedding highScore = mock(Embedding.class);
        when(highScore.getSysembedScore()).thenReturn(0.8d);
        when(highScore.getSysembedText()).thenReturn("relevant chunk");
        when(highScore.getSysembedDocId()).thenReturn(null);
        when(highScore.getSysembedId()).thenReturn("emb-1");
        when(highScore.getSysembedType()).thenReturn("mxbai");
        when(highScore.getSysembedLocation()).thenReturn(null);

        // Its docId is deliberately not stubbed: a candidate below the threshold is dropped before
        // enrichment, so nothing asks which document it belonged to (#135).
        Embedding lowScore = mock(Embedding.class);
        when(lowScore.getSysembedScore()).thenReturn(0.2d);

        VectorSearchResult vectorResult = mock(VectorSearchResult.class);
        when(vectorResult.getEmbeddings()).thenReturn(List.of(highScore, lowScore));
        when(vectorResult.getTotalCount()).thenReturn(2L);

        when(hxprService.vectorSearch(any(), any(), any(), anyInt())).thenReturn(vectorResult);

        SemanticSearchRequest request = SemanticSearchRequest.builder()
                .query("test")
                .minScore(0.5d)
                .build();
        SemanticSearchResponse response = svc.search(request);

        assertThat(response.getResultCount()).isEqualTo(1);
        assertThat(response.getResults()).hasSize(1);
        assertThat(response.getResults().get(0).getScore()).isEqualTo(0.8d);
        assertThat(response.getResults().get(0).getChunkText()).isEqualTo("relevant chunk");
    }

    /**
     * Enrichment runs over the retained candidates, not the retrieved pool (#135).
     *
     * <p>{@code fetchDocumentMetadata} issues one {@code SysContent} point query per distinct document, so
     * enriching before thresholding paid for documents whose only chunks were about to be dropped. That
     * cost scales with the over-fetched pool, which is what a document budget has to enlarge, so the order
     * is asserted rather than left to be re-derived from reading the method.</p>
     */
    @Test
    void search_enrichesOnlyTheDocumentsOfRetainedCandidates() {
        SemanticSearchService svc = spy(service);
        doReturn(List.of("user")).when(svc).getUserAuthorities(anyString(), anyString());

        when(securityContextService.getCurrentUsername()).thenReturn("user");
        when(embeddingService.embedQuery(any())).thenReturn(List.of(0.1d, 0.2d));
        when(embeddingService.getModelName()).thenReturn("test-model");

        String keptDoc = "11111111-1111-1111-1111-111111111111";
        String droppedDoc = "22222222-2222-2222-2222-222222222222";

        Embedding kept = mock(Embedding.class);
        when(kept.getSysembedScore()).thenReturn(0.8d);
        when(kept.getSysembedText()).thenReturn("relevant chunk");
        when(kept.getSysembedDocId()).thenReturn(keptDoc);
        when(kept.getSysembedId()).thenReturn("emb-1");
        when(kept.getSysembedType()).thenReturn("mxbai");
        when(kept.getSysembedLocation()).thenReturn(null);

        Embedding dropped = mock(Embedding.class);
        when(dropped.getSysembedScore()).thenReturn(0.2d);

        VectorSearchResult vectorResult = mock(VectorSearchResult.class);
        when(vectorResult.getEmbeddings()).thenReturn(List.of(kept, dropped));
        when(vectorResult.getTotalCount()).thenReturn(2L);
        when(hxprService.vectorSearch(any(), any(), any(), anyInt())).thenReturn(vectorResult);

        svc.search(SemanticSearchRequest.builder().query("test").minScore(0.5d).build());

        verify(hxprService, times(1)).query(contains(keptDoc), eq(1), eq(0));
        verify(hxprService, never()).query(contains(droppedDoc), anyInt(), anyInt());
    }

    // -----------------------------------------------------------------------
    // Document budget (#135)
    // -----------------------------------------------------------------------

    /** The point of the feature: a caller asking for documents gets documents, not chunks. */
    @Test
    void search_withTopDocuments_returnsChunksOfThatManyDistinctDocuments() {
        SemanticSearchService svc = budgetService();

        List<Embedding> pool = new java.util.ArrayList<>();
        pool.addAll(retainedChunks(DOC_A, 4));
        pool.addAll(retainedChunks(DOC_B, 4));
        pool.addAll(retainedChunks(DOC_C, 4));
        // Built up front: nesting a when(...) inside another when(...) leaves Mockito mid-stubbing.
        VectorSearchResult result = vectorResultOf(pool);
        // topDocuments=3, chunksPerDocument=1 -> chunkLimit 3, fetchK 6.
        when(hxprService.vectorSearch(any(), any(), any(), eq(6))).thenReturn(result);

        SemanticSearchResponse response = svc.search(SemanticSearchRequest.builder()
                .query("test")
                .topDocuments(3)
                .chunksPerDocument(1)
                .minScore(0.1d)
                .build());

        assertThat(response.getResults()).hasSize(3);
        assertThat(response.getResults())
                .extracting(hit -> hit.getSourceDocument().getDocumentId())
                .containsExactly(DOC_A, DOC_B, DOC_C);
        assertThat(response.getDocumentCount()).isEqualTo(3);
        assertThat(response.getAppliedTopDocuments()).isEqualTo(3);
        assertThat(response.getAppliedChunksPerDocument()).isEqualTo(1);
    }

    /**
     * A document budget has to be able to out-fetch the response ceiling. While MAX_TOP_K bounded both, the
     * pool was capped at the same number as the answer, so N distinct documents could not be guaranteed.
     */
    @Test
    void search_withTopDocuments_asksHxprPastTheResponseCeiling() {
        SemanticSearchService svc = budgetService();
        when(hxprService.vectorSearch(any(), any(), any(), anyInt())).thenReturn(null);

        svc.search(SemanticSearchRequest.builder()
                .query("test")
                .topDocuments(50)
                .chunksPerDocument(10)
                .build());

        // 50 x 10 clamps to the 200-chunk answer ceiling, doubled to a 400-row probe.
        verify(hxprService).vectorSearch(any(), any(), any(), eq(400));
    }

    /** Without a document budget the fetch bound is exactly what it was. */
    @Test
    void search_withoutTopDocuments_asksHxprForNoMoreThanTheResponseCeiling() {
        SemanticSearchService svc = budgetService();
        when(hxprService.vectorSearch(any(), any(), any(), anyInt())).thenReturn(null);

        svc.search(SemanticSearchRequest.builder().query("test").topK(50).build());

        verify(hxprService).vectorSearch(any(), any(), any(), eq(50));
    }

    /**
     * A pool that is all one document starves the budget, and the only remedy is more rows. The first
     * result is discarded rather than merged: a kNN result at a larger limit is a superset in rank order.
     */
    @Test
    void search_whenTheDocumentBudgetIsStarved_requeriesAtADoubledLimit() {
        SemanticSearchService svc = budgetService();

        List<Embedding> deeperPool = new java.util.ArrayList<>();
        deeperPool.addAll(retainedChunks(DOC_A, 4));
        deeperPool.addAll(retainedChunks(DOC_B, 4));
        VectorSearchResult starved = discardedResultOf(countedChunks(DOC_A, 4));
        VectorSearchResult deeper = vectorResultOf(deeperPool);

        // topDocuments=2, chunksPerDocument=1 -> chunkLimit 2, fetchK 4.
        when(hxprService.vectorSearch(any(), any(), any(), eq(4))).thenReturn(starved);
        when(hxprService.vectorSearch(any(), any(), any(), eq(8))).thenReturn(deeper);

        SemanticSearchResponse response = svc.search(twoDocumentRequest());

        verify(hxprService).vectorSearch(any(), any(), any(), eq(4));
        verify(hxprService).vectorSearch(any(), any(), any(), eq(8));
        verify(hxprService, never()).vectorSearch(any(), any(), any(), eq(16));
        assertThat(response.getDocumentCount()).isEqualTo(2);
    }

    /**
     * Fewer rows than asked means the filtered index is exhausted, so a deeper probe cannot find another
     * document. Probing anyway would cost a full kNN call per query on any small or tightly filtered corpus.
     */
    @Test
    void search_whenHxprReturnsFewerRowsThanAsked_doesNotProbeAgain() {
        SemanticSearchService svc = budgetService();

        VectorSearchResult exhausted = vectorResultOf(retainedChunks(DOC_A, 3));
        when(hxprService.vectorSearch(any(), any(), any(), eq(4))).thenReturn(exhausted);

        SemanticSearchResponse response = svc.search(twoDocumentRequest());

        verify(hxprService, times(1)).vectorSearch(any(), any(), any(), anyInt());
        // One document was all there was, and the response says so rather than looking like a shortfall.
        assertThat(response.getResults()).hasSize(1);
        assertThat(response.getDocumentCount()).isEqualTo(1);
    }

    /** Deepening is bounded, so a permanently starved budget cannot turn one slow query into many. */
    @Test
    void search_deepeningStopsAtThreeProbes() {
        SemanticSearchService svc = budgetService();

        VectorSearchResult probe1 = discardedResultOf(countedChunks(DOC_A, 4));
        VectorSearchResult probe2 = discardedResultOf(countedChunks(DOC_A, 8));
        VectorSearchResult probe3 = vectorResultOf(retainedChunks(DOC_A, 16));

        when(hxprService.vectorSearch(any(), any(), any(), eq(4))).thenReturn(probe1);
        when(hxprService.vectorSearch(any(), any(), any(), eq(8))).thenReturn(probe2);
        when(hxprService.vectorSearch(any(), any(), any(), eq(16))).thenReturn(probe3);

        svc.search(twoDocumentRequest());

        verify(hxprService, times(3)).vectorSearch(any(), any(), any(), anyInt());
        verify(hxprService, never()).vectorSearch(any(), any(), any(), eq(32));
    }

    /** A chunk-oriented request never deepens, however skewed the pool it got back. */
    @Test
    void search_withoutTopDocuments_neverProbesTwice() {
        SemanticSearchService svc = budgetService();

        VectorSearchResult skewed = vectorResultOf(retainedChunks(DOC_A, 5));
        when(hxprService.vectorSearch(any(), any(), any(), eq(5))).thenReturn(skewed);

        svc.search(SemanticSearchRequest.builder().query("test").minScore(0.1d).build());

        verify(hxprService, times(1)).vectorSearch(any(), any(), any(), anyInt());
    }

    /**
     * documentCount is reported whatever the caller asked in, and the applied budget only when one was.
     * A chunk-oriented caller otherwise recomputes it by grouping the hits itself.
     */
    @Test
    void search_withoutTopDocuments_stillReportsDocumentCountAndNoAppliedBudget() {
        SemanticSearchService svc = budgetService();

        List<Embedding> pool = new java.util.ArrayList<>();
        pool.addAll(retainedChunks(DOC_A, 3));
        pool.addAll(retainedChunks(DOC_B, 2));
        VectorSearchResult result = vectorResultOf(pool);
        when(hxprService.vectorSearch(any(), any(), any(), eq(5))).thenReturn(result);

        SemanticSearchResponse response = svc.search(
                SemanticSearchRequest.builder().query("test").minScore(0.1d).build());

        assertThat(response.getResultCount()).isEqualTo(5);
        assertThat(response.getDocumentCount()).isEqualTo(2);
        assertThat(response.getAppliedTopDocuments()).isNull();
        assertThat(response.getAppliedChunksPerDocument()).isNull();
    }

    /**
     * Both budgets are part of the cache key. Without them the same query asked with and asked without
     * topDocuments returns whichever of the two ran first, which is the one way this feature could
     * silently return the wrong shape to a caller who asked correctly.
     */
    @Test
    void theDocumentBudgetIsPartOfTheCacheKey() {
        String chunkOriented = cacheKey(SemanticSearchRequest.builder().query("test").build());
        String documentOriented = cacheKey(
                SemanticSearchRequest.builder().query("test").topDocuments(10).build());
        String otherPerDocument = cacheKey(SemanticSearchRequest.builder()
                .query("test").topDocuments(10).chunksPerDocument(2).build());

        assertThat(chunkOriented).isNotEqualTo(documentOriented);
        assertThat(documentOriented).isNotEqualTo(otherPerDocument);
    }

    private String cacheKey(SemanticSearchRequest request) {
        return ReflectionTestUtils.invokeMethod(service, "buildCacheKey", request);
    }

    private static final String DOC_A = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa";
    private static final String DOC_B = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb";
    private static final String DOC_C = "cccccccc-cccc-cccc-cccc-cccccccccccc";

    /** A service whose permission filter and embedding are stubbed, so only retrieval is under test. */
    private SemanticSearchService budgetService() {
        SemanticSearchService svc = spy(service);
        doReturn(List.of("user")).when(svc).getUserAuthorities(anyString(), anyString());
        when(securityContextService.getCurrentUsername()).thenReturn("user");
        when(embeddingService.embedQuery(any())).thenReturn(List.of(0.1d, 0.2d));
        when(embeddingService.getModelName()).thenReturn("test-model");
        return svc;
    }

    private static SemanticSearchRequest twoDocumentRequest() {
        return SemanticSearchRequest.builder()
                .query("test")
                .topDocuments(2)
                .chunksPerDocument(1)
                .minScore(0.1d)
                .build();
    }

    /** Chunks of one document, fully stubbed because these reach hit building. */
    private static List<Embedding> retainedChunks(String docId, int count) {
        List<Embedding> embeddings = new java.util.ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            Embedding embedding = mock(Embedding.class);
            when(embedding.getSysembedScore()).thenReturn(0.9d - (i * 0.01d));
            when(embedding.getSysembedText()).thenReturn(docId + " chunk " + i);
            when(embedding.getSysembedDocId()).thenReturn(docId);
            when(embedding.getSysembedId()).thenReturn(docId + "-emb-" + i);
            when(embedding.getSysembedType()).thenReturn("mxbai");
            when(embedding.getSysembedLocation()).thenReturn(null);
            embeddings.add(embedding);
        }
        return embeddings;
    }

    /**
     * Chunks of one document for a probe whose result is discarded, so only the score and the document are
     * ever read. Stubbing the rest would leave unused stubs, which is itself the assertion that a discarded
     * probe is not enriched.
     */
    private static List<Embedding> countedChunks(String docId, int count) {
        List<Embedding> embeddings = new java.util.ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            Embedding embedding = mock(Embedding.class);
            when(embedding.getSysembedScore()).thenReturn(0.9d);
            when(embedding.getSysembedDocId()).thenReturn(docId);
            embeddings.add(embedding);
        }
        return embeddings;
    }

    private static VectorSearchResult vectorResultOf(List<Embedding> embeddings) {
        VectorSearchResult result = mock(VectorSearchResult.class);
        when(result.getEmbeddings()).thenReturn(embeddings);
        when(result.getTotalCount()).thenReturn((long) embeddings.size());
        return result;
    }

    /**
     * A probe result that a deeper probe replaces. Its total count is deliberately not stubbed: a discarded
     * result is only counted and measured, so anything more would be an unused stub.
     */
    private static VectorSearchResult discardedResultOf(List<Embedding> embeddings) {
        VectorSearchResult result = mock(VectorSearchResult.class);
        when(result.getEmbeddings()).thenReturn(embeddings);
        return result;
    }

    // -----------------------------------------------------------------------
    // Query expansion
    // -----------------------------------------------------------------------

    @Test
    void search_noExpansion_runsExactlyOnePass() {
        SemanticSearchService svc = spy(service);
        doReturn(List.of("user")).when(svc).getUserAuthorities(anyString(), anyString());

        when(securityContextService.getCurrentUsername()).thenReturn("user");
        when(embeddingService.embedQuery(any())).thenReturn(List.of(0.1d, 0.2d));
        when(embeddingService.getModelName()).thenReturn("test-model");
        when(hxprService.vectorSearch(any(), any(), any(), anyInt())).thenReturn(null);
        when(queryExpansionService.expand(any())).thenReturn(null);

        svc.search(SemanticSearchRequest.builder().query("test").build());

        verify(embeddingService, times(1)).embedQuery("test");
        verify(hxprService, times(1)).vectorSearch(any(), any(), any(), anyInt());
        // No fusion, which is what "one pass" means here. Asserted on the RRF constant the fused branch
        // reads rather than on the properties object as a whole, because the single-pass path legitimately
        // reads other configuration (the per-document diversity cap) before it returns.
        verify(ragProperties, never()).getQueryExpansion();
    }

    @Test
    void search_expandedIntoVariants_searchesEachAndFusesTheResults() {
        SemanticSearchService svc = spy(service);
        doReturn(List.of("user")).when(svc).getUserAuthorities(anyString(), anyString());

        when(securityContextService.getCurrentUsername()).thenReturn("user");
        when(embeddingService.getModelName()).thenReturn("test-model");
        when(ragProperties.getQueryExpansion()).thenReturn(new RagProperties.QueryExpansionProperties());

        when(queryExpansionService.expand("test")).thenReturn(List.of(
                QueryVariant.original("test"),
                QueryVariant.rephrased("variant-1", "rephrased")));

        // Built up front: nesting a when(...) inside another when(...) leaves Mockito mid-stubbing.
        VectorSearchResult firstResult = vectorResult(embedding("emb-1", 0.8d, "chunk one"));
        VectorSearchResult secondResult = vectorResult(embedding("emb-2", 0.6d, "chunk two"));

        when(embeddingService.embedQuery("test")).thenReturn(List.of(0.1d, 0.2d));
        when(embeddingService.embedQuery("rephrased")).thenReturn(List.of(0.3d, 0.4d));
        when(hxprService.vectorSearch(eq(List.of(0.1d, 0.2d)), any(), any(), anyInt()))
                .thenReturn(firstResult);
        when(hxprService.vectorSearch(eq(List.of(0.3d, 0.4d)), any(), any(), anyInt()))
                .thenReturn(secondResult);

        SemanticSearchResponse response = svc.search(
                SemanticSearchRequest.builder().query("test").minScore(0.1d).build());

        // Both variants were embedded and searched, and the permission filter was resolved once.
        verify(embeddingService).embedQuery("test");
        verify(embeddingService).embedQuery("rephrased");
        verify(hxprService, times(2)).vectorSearch(any(), any(), any(), anyInt());
        verify(svc, times(1)).getUserAuthorities(anyString(), anyString());

        assertThat(response.getResults()).hasSize(2);
        assertThat(response.getResults()).extracting(SemanticSearchResponse.SearchHit::getChunkText)
                .containsExactly("chunk one", "chunk two");
        // Fusion reassigns rank but leaves each hit's own score alone.
        assertThat(response.getResults()).extracting(SemanticSearchResponse.SearchHit::getRank)
                .containsExactly(1, 2);
        assertThat(response.getResults()).extracting(SemanticSearchResponse.SearchHit::getScore)
                .containsExactly(0.8d, 0.6d);
    }

    @Test
    void search_expansionThrows_fallsBackToTheOriginalQuery() {
        SemanticSearchService svc = spy(service);
        doReturn(List.of("user")).when(svc).getUserAuthorities(anyString(), anyString());

        when(securityContextService.getCurrentUsername()).thenReturn("user");
        when(embeddingService.embedQuery(any())).thenReturn(List.of(0.1d, 0.2d));
        when(embeddingService.getModelName()).thenReturn("test-model");
        when(hxprService.vectorSearch(any(), any(), any(), anyInt())).thenReturn(null);
        when(queryExpansionService.expand(any())).thenThrow(new RuntimeException("expansion down"));

        SemanticSearchResponse response = svc.search(SemanticSearchRequest.builder().query("test").build());

        assertThat(response.getResults()).isEmpty();
        verify(embeddingService, times(1)).embedQuery("test");
    }

    private static Embedding embedding(String id, double score, String text) {
        Embedding embedding = mock(Embedding.class);
        when(embedding.getSysembedScore()).thenReturn(score);
        when(embedding.getSysembedText()).thenReturn(text);
        when(embedding.getSysembedDocId()).thenReturn(null);
        when(embedding.getSysembedId()).thenReturn(id);
        when(embedding.getSysembedType()).thenReturn("mxbai");
        when(embedding.getSysembedLocation()).thenReturn(null);
        return embedding;
    }

    private static VectorSearchResult vectorResult(Embedding... embeddings) {
        VectorSearchResult result = mock(VectorSearchResult.class);
        when(result.getEmbeddings()).thenReturn(List.of(embeddings));
        when(result.getTotalCount()).thenReturn((long) embeddings.length);
        return result;
    }

    @Test
    void search_withSourceType_addsGenericSourceFilterAndNarrowsAuthorities() {
        SemanticSearchService svc = spy(service);
        ReflectionTestUtils.setField(svc, "nuxeoSourceId", "nuxeo-demo");
        doReturn(List.of("user")).when(svc).getUserAuthorities("user", "nuxeo-demo");

        when(securityContextService.getCurrentUsername()).thenReturn("user");
        when(embeddingService.embedQuery(any())).thenReturn(List.of(0.1d, 0.2d));
        when(embeddingService.getModelName()).thenReturn("test-model");
        when(hxprService.vectorSearch(any(), any(), any(), anyInt())).thenReturn(null);

        SemanticSearchRequest request = SemanticSearchRequest.builder()
                .query("test")
                .sourceType("nuxeo")
                .build();

        svc.search(request);

        verify(hxprService).vectorSearch(any(), any(), argThat(filter ->
                filter.contains("cin_ingestProperties.source_type = 'nuxeo'")
                        && filter.contains("sys_racl = 'u:user_#_nuxeo-demo'")
                        && !filter.contains("u:user_#_test-repo")
        ), anyInt());
        verify(svc, never()).getUserAuthorities(eq("user"), eq("test-repo"));
    }
}
