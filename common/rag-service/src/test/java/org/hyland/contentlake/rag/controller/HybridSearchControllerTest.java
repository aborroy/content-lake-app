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

    /**
     * A zero or negative document budget is rejected, not clamped (#135). Unlike a value over the maximum,
     * it has no sensible reading: "zero documents" is not the same request with a bound applied. The check
     * runs before the enabled check, so no service stubbing is needed here.
     */
    @Test
    void search_nonPositiveDocumentBudget_returnsBadRequest() {
        for (HybridSearchRequest request : java.util.List.of(
                HybridSearchRequest.builder().query("test").topDocuments(0).build(),
                HybridSearchRequest.builder().query("test").topDocuments(-1).build(),
                HybridSearchRequest.builder().query("test").chunksPerDocument(0).build(),
                HybridSearchRequest.builder().query("test").chunksPerDocument(-5).build())) {

            ResponseEntity<HybridSearchResponse> response = controller.search(request);

            assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
            assertThat(response.getBody()).isNotNull();
            assertThat(response.getBody().getResultCount()).isZero();
        }
        verifyNoInteractions(hybridSearchService);
    }

    /** Absence must not be read as zero, or every caller that never mentioned documents would get a 400. */
    @Test
    void search_withNoDocumentBudget_isNotRejected() {
        when(hybridSearchProperties.isEnabled()).thenReturn(true);
        HybridSearchRequest request = HybridSearchRequest.builder().query("test").build();
        when(hybridSearchService.search(request))
                .thenReturn(HybridSearchResponse.builder().query("test").build());

        assertThat(controller.search(request).getStatusCode()).isEqualTo(HttpStatus.OK);
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
