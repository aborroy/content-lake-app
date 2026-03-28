package org.hyland.contentlake.rag.controller;

import org.hyland.contentlake.rag.config.HybridSearchProperties;
import org.hyland.contentlake.rag.model.HybridSearchRequest;
import org.hyland.contentlake.rag.model.HybridSearchResponse;
import org.hyland.contentlake.rag.service.HybridSearchService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class HybridSearchControllerTest {

    @Mock
    HybridSearchService hybridSearchService;

    @Mock
    HybridSearchProperties hybridSearchProperties;

    @InjectMocks
    HybridSearchController controller;

    @Test
    void search_blankQuery_returnsBadRequest() {
        HybridSearchRequest request = HybridSearchRequest.builder().query(" ").build();

        ResponseEntity<HybridSearchResponse> response = controller.search(request);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(response.getBody()).isNotNull();
        assertThat(response.getBody().getResultCount()).isZero();
        verifyNoInteractions(hybridSearchService);
    }

    @Test
    void search_whenDisabled_returnsServiceUnavailable() {
        when(hybridSearchProperties.isEnabled()).thenReturn(false);
        HybridSearchRequest request = HybridSearchRequest.builder().query("test").build();

        ResponseEntity<HybridSearchResponse> response = controller.search(request);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.SERVICE_UNAVAILABLE);
        assertThat(response.getBody()).isNotNull();
        assertThat(response.getBody().getStrategy()).isEqualTo("disabled");
        assertThat(response.getBody().getResultCount()).isZero();
        verifyNoInteractions(hybridSearchService);
    }

    @Test
    void search_whenEnabled_delegatesToService() {
        when(hybridSearchProperties.isEnabled()).thenReturn(true);
        HybridSearchRequest request = HybridSearchRequest.builder().query("test").build();
        HybridSearchResponse expected = HybridSearchResponse.builder()
                .query("test")
                .strategy("rrf")
                .resultCount(1)
                .build();
        when(hybridSearchService.search(request)).thenReturn(expected);

        ResponseEntity<HybridSearchResponse> response = controller.search(request);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).isEqualTo(expected);
        verify(hybridSearchService).search(request);
    }
}
