package org.hyland.contentlake.rag.controller;

import org.hyland.contentlake.rag.model.SemanticSearchRequest;
import org.hyland.contentlake.rag.model.SemanticSearchResponse;
import org.hyland.contentlake.rag.service.SemanticSearchService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class SemanticSearchControllerTest {

    @Mock
    SemanticSearchService semanticSearchService;

    @InjectMocks
    SemanticSearchController controller;

    @Test
    void search_blankQuery_returnsBadRequest() {
        SemanticSearchRequest request = SemanticSearchRequest.builder().query(" ").build();

        ResponseEntity<SemanticSearchResponse> response = controller.search(request);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(response.getBody()).isNotNull();
        assertThat(response.getBody().getResultCount()).isZero();
        verifyNoInteractions(semanticSearchService);
    }

    /**
     * A zero or negative document budget is rejected, not clamped (#135). Unlike a value over the maximum,
     * it has no sensible reading: "zero documents" is not the same request with a bound applied.
     */
    @Test
    void search_nonPositiveDocumentBudget_returnsBadRequest() {
        for (SemanticSearchRequest request : List.of(
                SemanticSearchRequest.builder().query("test").topDocuments(0).build(),
                SemanticSearchRequest.builder().query("test").topDocuments(-1).build(),
                SemanticSearchRequest.builder().query("test").chunksPerDocument(0).build(),
                SemanticSearchRequest.builder().query("test").chunksPerDocument(-5).build())) {

            ResponseEntity<SemanticSearchResponse> response = controller.search(request);

            assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
            assertThat(response.getBody()).isNotNull();
            assertThat(response.getBody().getQuery()).isEqualTo("test");
            assertThat(response.getBody().getResultCount()).isZero();
        }
        verifyNoInteractions(semanticSearchService);
    }

    /** Absence must not be read as zero, or every caller that never mentioned documents would get a 400. */
    @Test
    void search_withNoDocumentBudget_isNotRejected() {
        SemanticSearchRequest request = SemanticSearchRequest.builder().query("test").build();
        when(semanticSearchService.search(request))
                .thenReturn(SemanticSearchResponse.builder().query("test").build());

        assertThat(controller.search(request).getStatusCode()).isEqualTo(HttpStatus.OK);
    }

    /** A budget over the maximum clamps in the service rather than failing, consistently with topK. */
    @Test
    void search_withAnOversizedDocumentBudget_isNotRejected() {
        SemanticSearchRequest request = SemanticSearchRequest.builder()
                .query("test")
                .topDocuments(5000)
                .chunksPerDocument(5000)
                .build();
        when(semanticSearchService.search(request))
                .thenReturn(SemanticSearchResponse.builder().query("test").appliedTopDocuments(50).build());

        ResponseEntity<SemanticSearchResponse> response = controller.search(request);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).isNotNull();
        assertThat(response.getBody().getAppliedTopDocuments()).isEqualTo(50);
    }

    @Test
    void search_delegatesToService() {
        SemanticSearchRequest request = SemanticSearchRequest.builder().query("test").topK(3).build();
        SemanticSearchResponse expected = SemanticSearchResponse.builder()
                .query("test")
                .resultCount(3)
                .documentCount(2)
                .build();
        when(semanticSearchService.search(request)).thenReturn(expected);

        ResponseEntity<SemanticSearchResponse> response = controller.search(request);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).isEqualTo(expected);
    }
}
