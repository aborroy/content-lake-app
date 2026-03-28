package org.hyland.contentlake.rag.service;

import org.hyland.contentlake.rag.conversation.ConversationTurn;
import org.junit.jupiter.api.Test;
import org.springframework.ai.chat.model.ChatModel;
import org.springframework.ai.chat.model.ChatResponse;
import org.springframework.ai.chat.prompt.Prompt;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class QueryReformulationServiceTest {

    @Test
    void reformulate_withoutHistory_returnsOriginalQuestion() {
        ChatModel chatModel = mock(ChatModel.class);
        QueryReformulationService service = new QueryReformulationService(chatModel);

        String result = service.reformulate("Can you expand on it?", List.of());

        assertThat(result).isEqualTo("Can you expand on it?");
        verifyNoInteractions(chatModel);
    }

    @Test
    void reformulate_withHistory_returnsTrimmedRewrittenQuery() {
        ChatModel chatModel = mock(ChatModel.class);
        ChatResponse chatResponse = mock(ChatResponse.class, RETURNS_DEEP_STUBS);
        when(chatResponse.getResult().getOutput().getText()).thenReturn("  expand second point from q4 report  ");
        when(chatModel.call(any(Prompt.class))).thenReturn(chatResponse);

        QueryReformulationService service = new QueryReformulationService(chatModel);
        List<ConversationTurn> history = List.of(
                ConversationTurn.builder().role(ConversationTurn.Role.USER).content("Summarize Q4 report").timestamp(Instant.now()).build(),
                ConversationTurn.builder().role(ConversationTurn.Role.ASSISTANT).content("Revenue grew 12%").timestamp(Instant.now()).build()
        );

        String result = service.reformulate("Can you expand on the second point?", history);

        assertThat(result).isEqualTo("expand second point from q4 report");
    }

    @Test
    void reformulate_onModelError_returnsOriginalQuestion() {
        ChatModel chatModel = mock(ChatModel.class);
        when(chatModel.call(any(Prompt.class))).thenThrow(new RuntimeException("model unavailable"));

        QueryReformulationService service = new QueryReformulationService(chatModel);
        List<ConversationTurn> history = List.of(
                ConversationTurn.builder().role(ConversationTurn.Role.USER).content("Question").timestamp(Instant.now()).build()
        );

        String result = service.reformulate("Follow up?", history);

        assertThat(result).isEqualTo("Follow up?");
    }
}
