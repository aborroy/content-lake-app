package org.alfresco.contentlake.rag.service;

import org.alfresco.contentlake.rag.config.RagProperties;
import org.alfresco.contentlake.rag.conversation.ConversationMemoryService;
import org.alfresco.contentlake.rag.conversation.ConversationTurn;
import org.alfresco.contentlake.rag.model.RagPromptRequest;
import org.alfresco.contentlake.rag.model.RagPromptResponse;
import org.alfresco.contentlake.rag.model.SemanticSearchRequest;
import org.alfresco.contentlake.rag.model.SemanticSearchResponse;
import org.alfresco.contentlake.security.SecurityContextService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.ai.chat.model.ChatModel;
import org.springframework.ai.chat.prompt.Prompt;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class RagServiceConversationTest {

    @Mock SemanticSearchService semanticSearchService;
    @Mock ChatModel chatModel;
    @Mock ConversationMemoryService conversationMemoryService;
    @Mock QueryReformulationService queryReformulationService;
    @Mock RerankService rerankService;
    @Mock SecurityContextService securityContextService;

    private RagProperties properties;
    private RagService ragService;

    @BeforeEach
    void setUp() {
        properties = new RagProperties();
        properties.setDefaultTopK(5);
        properties.setDefaultMinScore(0.5);
        properties.setMaxContextLength(12000);
        properties.setDefaultSystemPrompt("system prompt");

        RagProperties.ConversationProperties conversation = new RagProperties.ConversationProperties();
        conversation.setEnabled(true);
        conversation.setMaxHistoryTurns(10);
        conversation.setSessionTtlMinutes(30);
        conversation.setQueryReformulation(true);
        properties.setConversation(conversation);

        ragService = new RagService(
                semanticSearchService,
                chatModel,
                properties,
                conversationMemoryService,
                queryReformulationService,
                rerankService,
                securityContextService
        );
    }

    @Test
    void prompt_withConversationEnabled_usesReformulatedQueryAndPersistsTurns() {
        List<ConversationTurn> history = List.of(
                ConversationTurn.builder().role(ConversationTurn.Role.USER).content("Summarize Q4 report").timestamp(Instant.now()).build(),
                ConversationTurn.builder().role(ConversationTurn.Role.ASSISTANT).content("Revenue grew 12%").timestamp(Instant.now()).build()
        );
        when(conversationMemoryService.getRecentTurns("session-1")).thenReturn(history);
        when(queryReformulationService.reformulate("Can you expand on the second point?", history))
                .thenReturn("expand second point from Q4 report");

        SemanticSearchResponse emptySearch = SemanticSearchResponse.builder()
                .query("expand second point from Q4 report")
                .results(List.of())
                .searchTimeMs(9)
                .build();
        when(semanticSearchService.search(any())).thenReturn(emptySearch);
        when(rerankService.rerank(eq("expand second point from Q4 report"), any())).thenReturn(List.of());

        RagPromptRequest request = RagPromptRequest.builder()
                .question("Can you expand on the second point?")
                .sessionId("session-1")
                .sourceType("nuxeo")
                .build();

        RagPromptResponse response = ragService.prompt(request);

        ArgumentCaptor<SemanticSearchRequest> searchCaptor = ArgumentCaptor.forClass(SemanticSearchRequest.class);
        verify(semanticSearchService).search(searchCaptor.capture());
        assertThat(searchCaptor.getValue().getQuery()).isEqualTo("expand second point from Q4 report");
        assertThat(searchCaptor.getValue().getSourceType()).isEqualTo("nuxeo");
        assertThat(response.getRetrievalQuery()).isEqualTo("expand second point from Q4 report");
        assertThat(response.getSessionId()).isEqualTo("session-1");
        assertThat(response.getHistoryTurnsUsed()).isEqualTo(2);

        verify(conversationMemoryService).appendUserTurn("session-1", "Can you expand on the second point?");
        verify(conversationMemoryService).appendAssistantTurn(eq("session-1"), contains("I couldn't find any relevant documents"));
        verify(queryReformulationService).reformulate("Can you expand on the second point?", history);
        verify(rerankService).rerank(eq("expand second point from Q4 report"), anyList());
        verifyNoInteractions(chatModel);
    }

    @Test
    void prompt_withConversationDisabled_skipsMemoryAndUsesOriginalQuery() {
        properties.getConversation().setEnabled(false);

        SemanticSearchResponse emptySearch = SemanticSearchResponse.builder()
                .query("What is new?")
                .results(List.of())
                .searchTimeMs(4)
                .build();
        when(semanticSearchService.search(any())).thenReturn(emptySearch);
        when(rerankService.rerank(eq("What is new?"), any())).thenReturn(List.of());

        RagPromptRequest request = RagPromptRequest.builder()
                .question("What is new?")
                .build();

        RagPromptResponse response = ragService.prompt(request);

        ArgumentCaptor<SemanticSearchRequest> searchCaptor = ArgumentCaptor.forClass(SemanticSearchRequest.class);
        verify(semanticSearchService).search(searchCaptor.capture());
        assertThat(searchCaptor.getValue().getQuery()).isEqualTo("What is new?");
        assertThat(response.getRetrievalQuery()).isEqualTo("What is new?");
        assertThat(response.getSessionId()).isNull();
        assertThat(response.getHistoryTurnsUsed()).isNull();

        verify(rerankService).rerank(eq("What is new?"), anyList());
        verifyNoInteractions(conversationMemoryService, queryReformulationService, securityContextService);
        verifyNoInteractions(chatModel);
    }

    @Test
    void prompt_withResetSession_flagResetsBeforeReadingHistory() {
        when(conversationMemoryService.getRecentTurns("session-reset")).thenReturn(List.of());

        SemanticSearchResponse emptySearch = SemanticSearchResponse.builder()
                .query("question")
                .results(List.of())
                .searchTimeMs(1)
                .build();
        when(semanticSearchService.search(any())).thenReturn(emptySearch);
        when(rerankService.rerank(eq("question"), any())).thenReturn(List.of());

        RagPromptRequest request = RagPromptRequest.builder()
                .question("question")
                .sessionId("session-reset")
                .resetSession(true)
                .build();

        ragService.prompt(request);

        verify(conversationMemoryService).resetSession("session-reset");
        verify(conversationMemoryService).getRecentTurns("session-reset");
        verify(rerankService).rerank(eq("question"), anyList());
    }

    @Test
    void prompt_withoutSessionId_usesUserScopedSessionId() {
        when(securityContextService.getCurrentUsername()).thenReturn("alice");
        when(conversationMemoryService.getRecentTurns("user:alice")).thenReturn(List.of());

        SemanticSearchResponse emptySearch = SemanticSearchResponse.builder()
                .query("question")
                .results(List.of())
                .searchTimeMs(1)
                .build();
        when(semanticSearchService.search(any())).thenReturn(emptySearch);
        when(rerankService.rerank(eq("question"), any())).thenReturn(List.of());

        RagPromptRequest request = RagPromptRequest.builder()
                .question("question")
                .build();

        RagPromptResponse response = ragService.prompt(request);

        assertThat(response.getSessionId()).isEqualTo("user:alice");
        verify(conversationMemoryService).getRecentTurns("user:alice");
        verify(conversationMemoryService).appendUserTurn("user:alice", "question");
        verify(rerankService).rerank(eq("question"), anyList());
        verify(securityContextService).getCurrentUsername();
    }

    @Test
    void prompt_withReformulationDisabled_usesOriginalQuery() {
        properties.getConversation().setQueryReformulation(false);
        List<ConversationTurn> history = List.of(
                ConversationTurn.builder().role(ConversationTurn.Role.USER).content("prior").timestamp(Instant.now()).build()
        );
        when(conversationMemoryService.getRecentTurns("session-x")).thenReturn(history);

        SemanticSearchResponse emptySearch = SemanticSearchResponse.builder()
                .query("follow up")
                .results(List.of())
                .searchTimeMs(2)
                .build();
        when(semanticSearchService.search(any())).thenReturn(emptySearch);
        when(rerankService.rerank(eq("follow up"), any())).thenReturn(List.of());

        RagPromptResponse response = ragService.prompt(RagPromptRequest.builder()
                .question("follow up")
                .sessionId("session-x")
                .build());

        assertThat(response.getRetrievalQuery()).isEqualTo("follow up");
        verifyNoInteractions(queryReformulationService);
        verify(rerankService).rerank(eq("follow up"), anyList());
    }

    @Test
    void prompt_withRetrievedContext_callsLlmAndMapsMetadata() {
        properties.getConversation().setEnabled(false);

        SemanticSearchResponse.SourceDocument source = SemanticSearchResponse.SourceDocument.builder()
                .documentId("doc-1")
                .nodeId("node-1")
                .sourceId("nuxeo:nuxeo-demo")
                .sourceType("nuxeo")
                .name("Q4.pdf")
                .path("/default-domain/workspaces/finance")
                .openInSourceUrl("http://localhost:8081/nuxeo/ui/#!/browse/default-domain/workspaces/finance/Q4.pdf")
                .build();
        SemanticSearchResponse.SearchHit hit = SemanticSearchResponse.SearchHit.builder()
                .rank(1)
                .score(0.91d)
                .chunkText("Revenue increased by 12% in Q4.")
                .sourceDocument(source)
                .build();

        when(semanticSearchService.search(any())).thenReturn(SemanticSearchResponse.builder()
                .query("What changed in Q4?")
                .searchTimeMs(12)
                .results(List.of(hit))
                .build());
        when(rerankService.rerank(eq("What changed in Q4?"), anyList())).thenReturn(List.of(hit));

        var chatResponse = mock(org.springframework.ai.chat.model.ChatResponse.class, RETURNS_DEEP_STUBS);
        when(chatResponse.getResult().getOutput().getText()).thenReturn("Revenue increased by 12% in Q4.");
        when(chatResponse.getMetadata().getModel()).thenReturn("ai/gpt-oss");
        when(chatResponse.getMetadata().getUsage().getTotalTokens()).thenReturn(321);
        when(chatModel.call(any(Prompt.class))).thenReturn(chatResponse);

        RagPromptResponse response = ragService.prompt(RagPromptRequest.builder()
                .question("What changed in Q4?")
                .includeContext(true)
                .build());

        assertThat(response.getAnswer()).isEqualTo("Revenue increased by 12% in Q4.");
        assertThat(response.getModel()).isEqualTo("ai/gpt-oss");
        assertThat(response.getTokenCount()).isEqualTo(321);
        assertThat(response.getSourcesUsed()).isEqualTo(1);
        assertThat(response.getSources().getFirst().getSourceType()).isEqualTo("nuxeo");
        assertThat(response.getSources().getFirst().getOpenInSourceUrl())
                .isEqualTo("http://localhost:8081/nuxeo/ui/#!/browse/default-domain/workspaces/finance/Q4.pdf");
        assertThat(response.getContext()).hasSize(1);
        assertThat(response.getContext().getFirst().getSourceType()).isEqualTo("nuxeo");
        assertThat(response.getContext().getFirst().getOpenInSourceUrl())
                .isEqualTo("http://localhost:8081/nuxeo/ui/#!/browse/default-domain/workspaces/finance/Q4.pdf");
        verify(chatModel).call(any(Prompt.class));
        verifyNoInteractions(conversationMemoryService, queryReformulationService, securityContextService);
    }

    @Test
    void streamPrompt_withoutRetrievedContext_streamsFallbackWithoutLlmCall() {
        when(conversationMemoryService.getRecentTurns("session-stream")).thenReturn(List.of());
        when(semanticSearchService.search(any())).thenReturn(SemanticSearchResponse.builder()
                .query("question")
                .searchTimeMs(3)
                .results(List.of())
                .build());
        when(rerankService.rerank(eq("question"), anyList())).thenReturn(List.of());

        SseEmitter emitter = ragService.streamPrompt(RagPromptRequest.builder()
                .question("question")
                .sessionId("session-stream")
                .build());

        assertThat(emitter).isNotNull();
        verify(conversationMemoryService).appendUserTurn("session-stream", "question");
        verify(conversationMemoryService).appendAssistantTurn(eq("session-stream"), contains("I couldn't find any relevant documents"));
        verifyNoInteractions(chatModel);
    }
}
