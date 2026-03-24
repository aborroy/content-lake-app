package org.alfresco.contentlake.rag.service;

import org.alfresco.contentlake.client.HxprService;
import org.alfresco.contentlake.hxpr.api.model.Embedding;
import org.alfresco.contentlake.hxpr.api.model.VectorSearchResult;
import org.alfresco.contentlake.rag.model.SemanticSearchRequest;
import org.alfresco.contentlake.rag.model.SemanticSearchResponse;
import org.alfresco.contentlake.security.SecurityContextService;
import org.alfresco.contentlake.service.EmbeddingService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class SemanticSearchServiceTest {

    @Mock HxprService hxprService;
    @Mock EmbeddingService embeddingService;
    @Mock SecurityContextService securityContextService;

    @InjectMocks SemanticSearchService service;

    @BeforeEach
    void setUp() {
        ReflectionTestUtils.setField(service, "repositoryId", "test-repo");
        ReflectionTestUtils.setField(service, "alfrescoUrl", "http://localhost:1");
        ReflectionTestUtils.setField(service, "serviceAccountUsername", "admin");
        ReflectionTestUtils.setField(service, "serviceAccountPassword", "admin");
        ReflectionTestUtils.setField(service, "defaultMinScore", 0.5d);
    }

    // -----------------------------------------------------------------------
    // Permission filter
    // -----------------------------------------------------------------------

    @Test
    void buildPermissionFilter_adminUser_includesEveryoneAndUsername() {
        SemanticSearchService svc = spy(service);
        doReturn(List.of("admin", "GROUP_EVERYONE")).when(svc).getUserAuthorities("admin");

        String filter = svc.buildPermissionFilter("admin", null);

        // __Everyone__ is always included
        assertThat(filter).contains("sys_racl = '__Everyone__'");
        // username is included with source-system suffix
        assertThat(filter).contains("sys_racl = 'admin_#_test-repo'");
        // GROUP_EVERYONE itself is skipped (not added as a clause)
        assertThat(filter).doesNotContain("GROUP_EVERYONE");
    }

    @Test
    void buildPermissionFilter_userWithGroups_includesGroupRaclFormat() {
        SemanticSearchService svc = spy(service);
        doReturn(List.of("alice", "GROUP_EVERYONE", "GROUP_DEVELOPERS"))
                .when(svc).getUserAuthorities("alice");

        String filter = svc.buildPermissionFilter("alice", null);

        // Groups are prefixed with "g:" in sys_racl
        assertThat(filter).contains("sys_racl = 'g:GROUP_DEVELOPERS_#_test-repo'");
        // Username also included
        assertThat(filter).contains("sys_racl = 'alice_#_test-repo'");
    }

    @Test
    void buildPermissionFilter_withAdditionalFilter_combinesWithAnd() {
        SemanticSearchService svc = spy(service);
        doReturn(List.of("alice")).when(svc).getUserAuthorities("alice");

        String filter = svc.buildPermissionFilter("alice", "cin_sourceId = 'my-repo'");

        assertThat(filter).contains(" AND ");
        assertThat(filter).contains("cin_sourceId = 'my-repo'");
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
        when(securityContextService.getCurrentUsername()).thenReturn("user");
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
        doReturn(List.of("user")).when(svc).getUserAuthorities(any());

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
        doReturn(List.of("user")).when(svc).getUserAuthorities(any());

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

        Embedding lowScore = mock(Embedding.class);
        when(lowScore.getSysembedScore()).thenReturn(0.2d);
        when(lowScore.getSysembedDocId()).thenReturn(null);

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
}
