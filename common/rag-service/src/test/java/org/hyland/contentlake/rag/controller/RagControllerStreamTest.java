package org.hyland.contentlake.rag.controller;

import org.hyland.contentlake.rag.model.RagPromptRequest;
import org.hyland.contentlake.rag.service.RagService;
import org.hyland.contentlake.rag.service.SemanticSearchService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.ai.chat.model.ChatModel;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class RagControllerStreamTest {

    @Mock
    RagService ragService;

    @Mock
    SemanticSearchService semanticSearchService;

    @Mock
    ChatModel chatModel;

    @InjectMocks
    RagController controller;

    @Test
    void streamGet_blankQuestion_returnsBadRequest() {
        RagPromptRequest request = RagPromptRequest.builder().question(" ").build();

        ResponseEntity<SseEmitter> response = controller.streamGet(request);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        verifyNoInteractions(ragService);
    }

    @Test
    void streamPost_blankQuestion_returnsBadRequest() {
        RagPromptRequest request = RagPromptRequest.builder().question("").build();

        ResponseEntity<SseEmitter> response = controller.streamPost(request);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        verifyNoInteractions(ragService);
    }

    @Test
    void streamGet_validRequest_delegatesToService() {
        RagPromptRequest request = RagPromptRequest.builder().question("What changed in Q4?").build();
        SseEmitter emitter = new SseEmitter();
        when(ragService.streamPrompt(request)).thenReturn(emitter);

        ResponseEntity<SseEmitter> response = controller.streamGet(request);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).isSameAs(emitter);
        verify(ragService).streamPrompt(request);
    }

    @Test
    void streamPost_validRequest_delegatesToService() {
        RagPromptRequest request = RagPromptRequest.builder().question("What changed in Q4?").build();
        SseEmitter emitter = new SseEmitter();
        when(ragService.streamPrompt(request)).thenReturn(emitter);

        ResponseEntity<SseEmitter> response = controller.streamPost(request);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).isSameAs(emitter);
        verify(ragService).streamPrompt(request);
    }
}
