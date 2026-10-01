package org.hyland.contentlake.rag.service;

import org.hyland.contentlake.rag.config.HybridSearchProperties;
import org.hyland.contentlake.rag.config.RagProperties;
import org.hyland.contentlake.rag.observability.RagObservations;
import org.hyland.contentlake.rag.observability.RetrievalFeatureSet;
import org.hyland.contentlake.rag.conversation.ConversationKeys;
import org.hyland.contentlake.rag.conversation.ConversationMemoryService;
import org.hyland.contentlake.rag.conversation.ConversationTurn;
import org.hyland.contentlake.rag.model.RagPromptRequest;
import org.hyland.contentlake.rag.model.RagPromptResponse;
import org.hyland.contentlake.rag.model.SemanticSearchRequest;
import org.hyland.contentlake.rag.model.SemanticSearchResponse;
import org.hyland.contentlake.security.SecurityContextService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.ai.chat.client.ChatClient;
import org.springframework.ai.chat.messages.AssistantMessage;
import org.springframework.ai.chat.metadata.ChatResponseMetadata;
import org.springframework.ai.chat.metadata.DefaultUsage;
import org.springframework.ai.chat.model.ChatModel;
import org.springframework.ai.chat.model.ChatResponse;
import org.springframework.ai.chat.model.Generation;
import org.springframework.ai.chat.prompt.ChatOptions;
import org.springframework.ai.chat.prompt.Prompt;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/**
 * Behavioral tests for {@link RagService} exercised through the real advisor-based
 * {@link ChatClient} wiring (a real {@link ContentLakeRetrievalAdvisor} +
 * {@link HxprDocumentRetriever} over mocked search services and a mocked {@link ChatModel}).
 *
 * <p>This preserves the pre-refactor assertions — reformulated retrieval query, session
 * handling, empty-context fallback without an LLM call, and metadata mapping — while
 * validating that they hold once retrieval/augmentation runs inside the advisor pipeline.</p>
 */
@ExtendWith(MockitoExtension.class)
class RagServiceConversationTest {

    @Mock SemanticSearchService semanticSearchService;
    @Mock HybridSearchService hybridSearchService;
    @Mock ChatModel chatModel;
    @Mock ConversationMemoryService conversationMemoryService;
    @Mock QueryReformulationService queryReformulationService;
    @Mock RerankService rerankService;
    @Mock DiversitySelector diversitySelector;
    @Mock SecurityContextService securityContextService;
    @Mock FilterInferenceService filterInferenceService;
    @Mock org.hyland.contentlake.rag.conversation.SessionSummaryService sessionSummaryService;
    @Mock CitationVerifier citationVerifier;
    @Mock org.hyland.contentlake.client.HxprDocumentApi hxprDocumentApi;

    private RagProperties properties;
    private RagService ragService;

    @BeforeEach
    void setUp() {
        properties = new RagProperties();
        properties.setDefaultTopK(5);
        properties.setDefaultMinScore(0.5);
        properties.setMaxContextLength(12000);
        properties.setDefaultSystemPrompt("system prompt");
        // Use semantic-only path so assertions against semanticSearchService still apply.
        properties.setUseHybridSearch(false);

        RagProperties.ConversationProperties conversation = new RagProperties.ConversationProperties();
        conversation.setEnabled(true);
        conversation.setMaxHistoryTurns(10);
        conversation.setSessionTtlMinutes(30);
        conversation.setQueryReformulation(true);
        properties.setConversation(conversation);

        // ChatClient request-building reads the model's options; the mock returns null by default.
        lenient().when(chatModel.getDefaultOptions()).thenReturn(ChatOptions.builder().build());
        lenient().when(chatModel.getOptions()).thenReturn(ChatOptions.builder().build());
        lenient().when(securityContextService.getCurrentUsername()).thenReturn("alice");

        HxprDocumentRetriever retriever =
                new HxprDocumentRetriever(semanticSearchService, hybridSearchService, properties);
        SectionExpansionService sectionExpansionService =
                new SectionExpansionService(hxprDocumentApi, properties);
        ContentLakeRetrievalAdvisor advisor = new ContentLakeRetrievalAdvisor(
                retriever, diversitySelector, rerankService, new NoOpRetrievalGrader(), properties,
                sectionExpansionService, new PromptInjectionScanner(),
                RagObservations.NOOP, new RetrievalFeatureSet(properties, new HybridSearchProperties()));
        ChatClient chatClient = ChatClient.builder(chatModel)
                .defaultAdvisors(advisor)
                .build();

        ragService = new RagService(
                chatClient,
                properties,
                conversationMemoryService,
                queryReformulationService,
                securityContextService,
                filterInferenceService,
                sessionSummaryService,
                citationVerifier,
                new StructuredAnswerService(new StructuredLlmCaller(chatModel)),
                // Agentic tools are disabled by default in these tests, so the toolset is never invoked.
                null,
                // Tracing collaborator (#73) is optional; null falls back to running steps untraced.
                null,
                // Feature-set tag (#116): unused without an observation registry.
                null
        );
    }

    /** The key conversation memory is stored under for a session the test user "alice" owns. */
    private static String key(String sessionId) {
        return ConversationKeys.of("alice", sessionId);
    }

    private static ChatResponse chatResponse(String text, String model, int totalTokens) {
        ChatResponseMetadata metadata = ChatResponseMetadata.builder()
                .model(model)
                .usage(new DefaultUsage(0, totalTokens, totalTokens))
                .build();
        return new ChatResponse(List.of(new Generation(new AssistantMessage(text))), metadata);
    }

    @Test
    void prompt_withConversationEnabled_usesReformulatedQueryAndPersistsTurns() {
        List<ConversationTurn> history = List.of(
                ConversationTurn.builder().role(ConversationTurn.Role.USER).content("Summarize Q4 report").timestamp(Instant.now()).build(),
                ConversationTurn.builder().role(ConversationTurn.Role.ASSISTANT).content("Revenue grew 12%").timestamp(Instant.now()).build()
        );
        when(conversationMemoryService.getRecentTurns(key("session-1"))).thenReturn(history);
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

        verify(conversationMemoryService).appendUserTurn(key("session-1"), "Can you expand on the second point?");
        verify(conversationMemoryService).appendAssistantTurn(eq(key("session-1")), contains("I couldn't find any relevant documents"));
        verify(queryReformulationService).reformulate("Can you expand on the second point?", history);
        verify(rerankService).rerank(eq("expand second point from Q4 report"), anyList());
        // Empty context short-circuits the LLM call.
        verify(chatModel, never()).call(any(Prompt.class));
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
        // Every answer carries a correlation id for feedback (#74).
        assertThat(response.getRequestId()).isNotBlank();

        verify(rerankService).rerank(eq("What is new?"), anyList());
        verifyNoInteractions(conversationMemoryService, queryReformulationService, securityContextService);
        verify(chatModel, never()).call(any(Prompt.class));
    }

    @Test
    void prompt_withResetSession_flagResetsBeforeReadingHistory() {
        when(conversationMemoryService.getRecentTurns(key("session-reset"))).thenReturn(List.of());

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

        verify(conversationMemoryService).resetSession(key("session-reset"));
        verify(conversationMemoryService).getRecentTurns(key("session-reset"));
        verify(rerankService).rerank(eq("question"), anyList());
    }

    @Test
    void prompt_withoutSessionId_usesUserScopedSessionId() {
        when(conversationMemoryService.getRecentTurns(key("user:alice"))).thenReturn(List.of());

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
        verify(conversationMemoryService).getRecentTurns(key("user:alice"));
        verify(conversationMemoryService).appendUserTurn(key("user:alice"), "question");
        verify(rerankService).rerank(eq("question"), anyList());
        verify(securityContextService).getCurrentUsername();
    }

    @Test
    void prompt_withAnotherUsersSessionId_neverReachesTheirMemory() {
        when(securityContextService.getCurrentUsername()).thenReturn("bob");
        when(conversationMemoryService.getRecentTurns(ConversationKeys.of("bob", "alice-session")))
                .thenReturn(List.of());
        when(semanticSearchService.search(any())).thenReturn(SemanticSearchResponse.builder()
                .query("question")
                .results(List.of())
                .searchTimeMs(1)
                .build());
        when(rerankService.rerank(eq("question"), any())).thenReturn(List.of());

        RagPromptResponse response = ragService.prompt(RagPromptRequest.builder()
                .question("question")
                .sessionId("alice-session")
                .resetSession(true)
                .build());

        assertThat(response.getSessionId()).isEqualTo("alice-session");
        verify(conversationMemoryService).resetSession(ConversationKeys.of("bob", "alice-session"));
        verify(conversationMemoryService, never()).resetSession(key("alice-session"));
        verify(conversationMemoryService, never()).getRecentTurns(key("alice-session"));
    }

    @Test
    void prompt_withReformulationDisabled_usesOriginalQuery() {
        properties.getConversation().setQueryReformulation(false);
        List<ConversationTurn> history = List.of(
                ConversationTurn.builder().role(ConversationTurn.Role.USER).content("prior").timestamp(Instant.now()).build()
        );
        when(conversationMemoryService.getRecentTurns(key("session-x"))).thenReturn(history);

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
        when(chatModel.call(any(Prompt.class)))
                .thenReturn(chatResponse("Revenue increased by 12% in Q4.", "ai/gpt-oss", 321));

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
    void prompt_withRetrievedContext_augmentsUserPromptWithContext() {
        properties.getConversation().setEnabled(false);

        SemanticSearchResponse.SearchHit hit = SemanticSearchResponse.SearchHit.builder()
                .rank(1)
                .score(0.91d)
                .chunkText("Revenue increased by 12% in Q4.")
                .sourceDocument(SemanticSearchResponse.SourceDocument.builder().name("Q4.pdf").build())
                .build();
        when(semanticSearchService.search(any())).thenReturn(SemanticSearchResponse.builder()
                .query("What changed in Q4?")
                .searchTimeMs(12)
                .results(List.of(hit))
                .build());
        when(rerankService.rerank(eq("What changed in Q4?"), anyList())).thenReturn(List.of(hit));
        when(chatModel.call(any(Prompt.class))).thenReturn(chatResponse("ok", "ai/gpt-oss", 10));

        ragService.prompt(RagPromptRequest.builder().question("What changed in Q4?").build());

        ArgumentCaptor<Prompt> promptCaptor = ArgumentCaptor.forClass(Prompt.class);
        verify(chatModel).call(promptCaptor.capture());
        String userText = promptCaptor.getValue().getUserMessage().getText();
        // The grounded document context and the original question are both present in the augmented prompt.
        assertThat(userText).contains("Revenue increased by 12% in Q4.");
        assertThat(userText).contains("Question: What changed in Q4?");
        assertThat(userText).contains("DOCUMENT CONTEXT");
    }

    @Test
    void streamPrompt_withoutRetrievedContext_streamsFallbackWithoutLlmCall() {
        when(conversationMemoryService.getRecentTurns(key("session-stream"))).thenReturn(List.of());
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
        verify(conversationMemoryService).appendUserTurn(key("session-stream"), "question");
        verify(conversationMemoryService).appendAssistantTurn(eq(key("session-stream")), contains("I couldn't find any relevant documents"));
        verify(chatModel, never()).stream(any(Prompt.class));
    }
}
