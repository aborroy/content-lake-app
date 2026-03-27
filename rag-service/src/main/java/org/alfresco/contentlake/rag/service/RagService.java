package org.alfresco.contentlake.rag.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.alfresco.contentlake.rag.conversation.ConversationMemoryService;
import org.alfresco.contentlake.rag.conversation.ConversationTurn;
import org.alfresco.contentlake.rag.config.RagProperties;
import org.alfresco.contentlake.rag.model.RagPromptRequest;
import org.alfresco.contentlake.rag.model.RagPromptResponse;
import org.alfresco.contentlake.rag.model.RagPromptResponse.ContextChunk;
import org.alfresco.contentlake.rag.model.RagPromptResponse.Source;
import org.alfresco.contentlake.rag.model.SemanticSearchRequest;
import org.alfresco.contentlake.rag.model.SemanticSearchResponse;
import org.alfresco.contentlake.rag.model.SemanticSearchResponse.SearchHit;
import org.alfresco.contentlake.security.SecurityContextService;
import org.springframework.ai.chat.client.ChatClient;
import org.springframework.ai.chat.messages.SystemMessage;
import org.springframework.ai.chat.messages.UserMessage;
import org.springframework.ai.chat.model.ChatModel;
import org.springframework.ai.chat.model.ChatResponse;
import org.springframework.ai.chat.prompt.Prompt;
import org.springframework.stereotype.Service;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;
import reactor.core.Disposable;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * RAG (Retrieval-Augmented Generation) service.
 *
 * <p>Orchestrates the three-phase pipeline:
 * <ol>
 *   <li><strong>Retrieve</strong> — Permission-filtered semantic search via {@link SemanticSearchService}</li>
 *   <li><strong>Augment</strong> — Assembles a grounded prompt with retrieved chunks as context</li>
 *   <li><strong>Generate</strong> — Calls the LLM via Spring AI {@link ChatModel}</li>
 * </ol>
 *
 * <p>The context sent to the LLM is capped at {@code rag.max-context-length} characters
 * to stay within reasonable token limits for the model.</p>
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class RagService {

    private static final Pattern TOKEN_PATTERN = Pattern.compile("\\S+");

    @Value("${spring.ai.openai.chat.options.model:}")
    private String configuredModel;

    private final SemanticSearchService semanticSearchService;
    private final ChatModel chatModel;
    private final RagProperties ragProperties;
    private final ConversationMemoryService conversationMemoryService;
    private final QueryReformulationService queryReformulationService;
    private final RerankService rerankService;
    private final SecurityContextService securityContextService;

    /**
     * Executes the full RAG pipeline for a given question.
     *
     * @param request the RAG prompt request
     * @return response with generated answer, sources, and timing
     */
    public RagPromptResponse prompt(RagPromptRequest request) {
        long totalStart = System.currentTimeMillis();

        PromptPreparation preparation = preparePromptGeneration(request);
        GenerationResult generation = generateAnswer(preparation);
        long totalTimeMs = System.currentTimeMillis() - totalStart;

        persistConversationTurn(request, preparation, generation);
        return buildPromptResponse(request, preparation, generation, totalTimeMs);
    }

    /**
     * Streams LLM output token-by-token over SSE.
     *
     * <p>This uses Spring AI {@link ChatClient#prompt()} streaming API and emits:</p>
     * <ul>
     *   <li>{@code event: token} for every non-empty token delta</li>
     *   <li>{@code event: metadata} with final {@link RagPromptResponse}</li>
     *   <li>{@code event: done} when the stream completes</li>
     *   <li>{@code event: error} on terminal failures</li>
     * </ul>
     */
    public SseEmitter streamPrompt(RagPromptRequest request) {
        SseEmitter emitter = new SseEmitter(0L);
        final long totalStart = System.currentTimeMillis();

        final PromptPreparation preparation;
        try {
            preparation = preparePromptGeneration(request);
        } catch (Exception e) {
            log.error("RAG stream preparation failed: {}", e.getMessage(), e);
            sendErrorEvent(emitter, "Failed to prepare RAG stream: " + e.getMessage());
            emitter.complete();
            return emitter;
        }

        if (preparation.retrieval().rerankedHits().isEmpty()) {
            String answer = "I couldn't find any relevant documents to answer your question. "
                    + "Please try rephrasing your query or ensure the relevant documents have been ingested.";
            Integer tokenCount = estimateTotalTokenCount(
                    preparation.prompt().systemPrompt(),
                    preparation.prompt().userPrompt(),
                    answer
            );
            GenerationResult generation = new GenerationResult(answer, "none (no context available)", tokenCount, 0L);
            sendTokenEvent(emitter, answer);
            persistConversationTurn(request, preparation, generation);
            RagPromptResponse response = buildPromptResponse(
                    request, preparation, generation, System.currentTimeMillis() - totalStart
            );
            sendMetadataEvent(emitter, response);
            sendDoneEvent(emitter);
            emitter.complete();
            return emitter;
        }

        StringBuilder answerBuilder = new StringBuilder();
        StreamAccumulator accumulator = new StreamAccumulator();
        long generationStart = System.currentTimeMillis();

        Disposable subscription = ChatClient.create(chatModel)
                .prompt(buildPrompt(preparation))
                .stream()
                .chatResponse()
                .subscribe(chatResponse -> {
                            if (chatResponse == null) {
                                return;
                            }
                            updateStreamMetadata(accumulator, chatResponse);
                            String chunkText = extractChunkText(chatResponse);
                            String delta = resolveDeltaToken(answerBuilder, chunkText);
                            if (delta == null || delta.isEmpty()) {
                                return;
                            }
                            answerBuilder.append(delta);
                            sendTokenEvent(emitter, delta);
                        },
                        error -> {
                            log.error("RAG streaming generation failed: {}", error.getMessage(), error);
                            sendErrorEvent(emitter, error.getMessage());
                            emitter.complete();
                        },
                        () -> {
                            long generationTimeMs = System.currentTimeMillis() - generationStart;
                            String answer = answerBuilder.toString();
                            GenerationResult generation = new GenerationResult(
                                    answer,
                                    resolveStreamModel(accumulator),
                                    resolveStreamTokenCount(accumulator, preparation, answer),
                                    generationTimeMs
                            );
                            persistConversationTurn(request, preparation, generation);
                            RagPromptResponse response = buildPromptResponse(
                                    request, preparation, generation, System.currentTimeMillis() - totalStart
                            );
                            sendMetadataEvent(emitter, response);
                            sendDoneEvent(emitter);
                            emitter.complete();
                        });

        emitter.onCompletion(subscription::dispose);
        emitter.onTimeout(() -> {
            subscription.dispose();
            emitter.complete();
        });

        return emitter;
    }

    /**
     * Shared preparation phase for both synchronous and streaming generation.
     *
     * <p>Computes conversation context, retrieval query, reranked hits, and rendered prompts.</p>
     */
    private PromptPreparation preparePromptGeneration(RagPromptRequest request) {
        ConversationState conversation = prepareConversationState(request);
        RetrievalState retrieval = retrieveContext(request, conversation.history());
        PromptState prompt = preparePromptState(request, conversation.history(), retrieval);
        return new PromptPreparation(conversation, retrieval, prompt);
    }

    private ConversationState prepareConversationState(RagPromptRequest request) {
        boolean conversationEnabled = ragProperties.getConversation().isEnabled();
        if (!conversationEnabled) {
            return new ConversationState(false, null, List.of());
        }

        String sessionId = resolveSessionId(request);
        if (request.isResetSession()) {
            conversationMemoryService.resetSession(sessionId);
        }

        List<ConversationTurn> history = conversationMemoryService.getRecentTurns(sessionId);
        return new ConversationState(true, sessionId, history != null ? history : List.of());
    }

    private RetrievalState retrieveContext(RagPromptRequest request, List<ConversationTurn> history) {
        String retrievalQuery = resolveRetrievalQuery(request.getQuestion(), history);

        int topK = request.getTopK() > 0 ? request.getTopK() : ragProperties.getDefaultTopK();
        double minScore = request.getMinScore() > 0 ? request.getMinScore() : ragProperties.getDefaultMinScore();

        SemanticSearchRequest searchRequest = SemanticSearchRequest.builder()
                .query(retrievalQuery)
                .topK(topK)
                .minScore(minScore)
                .filter(request.getFilter())
                .sourceType(request.getSourceType())
                .embeddingType(request.getEmbeddingType())
                .build();

        log.info("RAG retrieve phase: query=\"{}\", topK={}, minScore={}", retrievalQuery, topK, minScore);
        SemanticSearchResponse searchResponse = semanticSearchService.search(searchRequest);
        long searchTimeMs = searchResponse.getSearchTimeMs();

        List<SearchHit> hits = searchResponse.getResults() != null ? searchResponse.getResults() : List.of();
        List<SearchHit> reranked = rerankService.rerank(retrievalQuery, hits);
        List<SearchHit> rerankedHits = reranked != null ? reranked : List.of();

        log.info("RAG retrieve phase complete: {} chunks retrieved in {}ms (reranked={})",
                hits.size(), searchTimeMs, rerankedHits.size());

        return new RetrievalState(retrievalQuery, searchTimeMs, rerankedHits);
    }

    private PromptState preparePromptState(RagPromptRequest request,
                                           List<ConversationTurn> history,
                                           RetrievalState retrieval) {
        String contextBlock = assembleContext(retrieval.rerankedHits());
        String historyBlock = assembleConversationHistory(history);
        String systemPrompt = resolveSystemPrompt(request);
        String userPrompt = buildUserPrompt(request.getQuestion(), retrieval.retrievalQuery(), historyBlock, contextBlock);

        log.debug("RAG augment phase: context length={} chars, {} sources, history turns={}",
                contextBlock.length(), retrieval.rerankedHits().size(), history.size());

        return new PromptState(systemPrompt, userPrompt);
    }

    private Prompt buildPrompt(PromptPreparation preparation) {
        return new Prompt(List.of(
                new SystemMessage(preparation.prompt().systemPrompt()),
                new UserMessage(preparation.prompt().userPrompt())
        ));
    }

    private GenerationResult generateAnswer(PromptPreparation preparation) {
        long generationStart = System.currentTimeMillis();

        if (preparation.retrieval().rerankedHits().isEmpty()) {
            long generationTimeMs = System.currentTimeMillis() - generationStart;
            return new GenerationResult(
                    "I couldn't find any relevant documents to answer your question. "
                            + "Please try rephrasing your query or ensure the relevant documents have been ingested.",
                    "none (no context available)",
                    null,
                    generationTimeMs
            );
        }

        try {
            ChatResponse chatResponse = chatModel.call(buildPrompt(preparation));

            String answer = chatResponse.getResult().getOutput().getText();
            String modelName = configuredModel != null && !configuredModel.isBlank()
                    ? configuredModel
                    : (chatResponse.getMetadata() != null && chatResponse.getMetadata().getModel() != null
                            ? chatResponse.getMetadata().getModel()
                            : "unknown");
            Integer tokenCount = chatResponse.getMetadata() != null && chatResponse.getMetadata().getUsage() != null
                    ? chatResponse.getMetadata().getUsage().getTotalTokens()
                    : null;

            log.info("RAG generate phase complete: model={}, answer length={} chars", modelName, answer.length());

            long generationTimeMs = System.currentTimeMillis() - generationStart;
            return new GenerationResult(answer, modelName, tokenCount, generationTimeMs);
        } catch (Exception e) {
            log.error("LLM generation failed: {}", e.getMessage(), e);
            long generationTimeMs = System.currentTimeMillis() - generationStart;
            return new GenerationResult(
                    "An error occurred while generating the answer: " + e.getMessage(),
                    "error",
                    null,
                    generationTimeMs
            );
        }
    }

    private void persistConversationTurn(RagPromptRequest request,
                                         PromptPreparation preparation,
                                         GenerationResult generation) {
        if (!preparation.conversation().enabled()) {
            return;
        }
        conversationMemoryService.appendUserTurn(preparation.conversation().sessionId(), request.getQuestion());
        conversationMemoryService.appendAssistantTurn(preparation.conversation().sessionId(), generation.answer());
    }

    private RagPromptResponse buildPromptResponse(RagPromptRequest request,
                                                  PromptPreparation preparation,
                                                  GenerationResult generation,
                                                  long totalTimeMs) {
        List<SearchHit> rerankedHits = preparation.retrieval().rerankedHits();
        List<Source> sources = mapSources(rerankedHits);
        List<ContextChunk> contextChunks = request.isIncludeContext() ? mapContextChunks(rerankedHits) : null;

        return RagPromptResponse.builder()
                .answer(generation.answer())
                .question(request.getQuestion())
                .sessionId(preparation.conversation().sessionId())
                .retrievalQuery(preparation.retrieval().retrievalQuery())
                .historyTurnsUsed(preparation.conversation().enabled()
                        ? preparation.conversation().history().size()
                        : null)
                .model(generation.modelName())
                .tokenCount(generation.tokenCount())
                .searchTimeMs(preparation.retrieval().searchTimeMs())
                .generationTimeMs(generation.generationTimeMs())
                .totalTimeMs(totalTimeMs)
                .sourcesUsed(sources.size())
                .sources(sources)
                .context(contextChunks)
                .build();
    }

    private List<Source> mapSources(List<SearchHit> hits) {
        return hits.stream()
                .map(hit -> Source.builder()
                        .documentId(hit.getSourceDocument() != null ? hit.getSourceDocument().getDocumentId() : null)
                        .nodeId(hit.getSourceDocument() != null ? hit.getSourceDocument().getNodeId() : null)
                        .sourceId(hit.getSourceDocument() != null ? hit.getSourceDocument().getSourceId() : null)
                        .sourceType(hit.getSourceDocument() != null ? hit.getSourceDocument().getSourceType() : null)
                        .name(hit.getSourceDocument() != null ? hit.getSourceDocument().getName() : null)
                        .path(hit.getSourceDocument() != null ? hit.getSourceDocument().getPath() : null)
                        .openInSourceUrl(hit.getSourceDocument() != null ? hit.getSourceDocument().getOpenInSourceUrl() : null)
                        .chunkText(hit.getChunkText())
                        .score(hit.getScore())
                        .build())
                .toList();
    }

    private List<ContextChunk> mapContextChunks(List<SearchHit> hits) {
        return hits.stream()
                .map(hit -> ContextChunk.builder()
                        .rank(hit.getRank())
                        .score(hit.getScore())
                        .text(hit.getChunkText())
                        .sourceName(hit.getSourceDocument() != null ? hit.getSourceDocument().getName() : null)
                        .sourcePath(hit.getSourceDocument() != null ? hit.getSourceDocument().getPath() : null)
                        .sourceType(hit.getSourceDocument() != null ? hit.getSourceDocument().getSourceType() : null)
                        .openInSourceUrl(hit.getSourceDocument() != null ? hit.getSourceDocument().getOpenInSourceUrl() : null)
                        .build())
                .toList();
    }

    private void sendTokenEvent(SseEmitter emitter, String token) {
        try {
            emitter.send(SseEmitter.event()
                    .name("token")
                    .data(Map.of("token", token)));
        } catch (IOException e) {
            throw new IllegalStateException("Failed to send SSE token event", e);
        }
    }

    private void sendDoneEvent(SseEmitter emitter) {
        try {
            emitter.send(SseEmitter.event()
                    .name("done")
                    .data(Map.of("status", "ok")));
        } catch (IOException e) {
            throw new IllegalStateException("Failed to send SSE done event", e);
        }
    }

    private void sendMetadataEvent(SseEmitter emitter, RagPromptResponse response) {
        try {
            emitter.send(SseEmitter.event()
                    .name("metadata")
                    .data(response));
        } catch (IOException e) {
            throw new IllegalStateException("Failed to send SSE metadata event", e);
        }
    }

    private void sendErrorEvent(SseEmitter emitter, String message) {
        try {
            emitter.send(SseEmitter.event()
                    .name("error")
                    .data(Map.of("message", message != null && !message.isBlank() ? message : "Stream failed")));
        } catch (IOException e) {
            log.warn("Unable to send SSE error event: {}", e.getMessage());
        }
    }

    private void updateStreamMetadata(StreamAccumulator accumulator, ChatResponse chatResponse) {
        if (chatResponse.getMetadata() == null) {
            return;
        }

        String model = chatResponse.getMetadata().getModel();
        if (model != null && !model.isBlank()) {
            accumulator.model = model;
        }

        if (chatResponse.getMetadata().getUsage() != null) {
            Integer totalTokens = chatResponse.getMetadata().getUsage().getTotalTokens();
            if (totalTokens != null && totalTokens > 0) {
                accumulator.tokenCount = totalTokens;
            }
        }
    }

    private String extractChunkText(ChatResponse chatResponse) {
        if (chatResponse.getResult() == null || chatResponse.getResult().getOutput() == null) {
            return "";
        }
        String text = chatResponse.getResult().getOutput().getText();
        return text != null ? text : "";
    }

    private String resolveDeltaToken(StringBuilder currentAnswer, String chunkText) {
        if (chunkText == null || chunkText.isEmpty()) {
            return "";
        }
        String existing = currentAnswer.toString();
        if (!existing.isEmpty() && chunkText.startsWith(existing)) {
            return chunkText.substring(existing.length());
        }
        return chunkText;
    }

    private String resolveStreamModel(StreamAccumulator accumulator) {
        if (configuredModel != null && !configuredModel.isBlank()) {
            return configuredModel;
        }
        if (accumulator.model != null && !accumulator.model.isBlank()) {
            return accumulator.model;
        }
        return "unknown";
    }

    private Integer resolveStreamTokenCount(StreamAccumulator accumulator,
                                            PromptPreparation preparation,
                                            String answer) {
        if (accumulator.tokenCount != null && accumulator.tokenCount > 0) {
            return accumulator.tokenCount;
        }
        return estimateTotalTokenCount(preparation.prompt().systemPrompt(), preparation.prompt().userPrompt(), answer);
    }

    private int estimateTotalTokenCount(String... parts) {
        int total = 0;
        for (String part : parts) {
            total += estimateTokenCount(part);
        }
        return total;
    }

    private int estimateTokenCount(String text) {
        if (text == null || text.isBlank()) {
            return 0;
        }
        int count = 0;
        var matcher = TOKEN_PATTERN.matcher(text);
        while (matcher.find()) {
            count++;
        }
        return count;
    }

    // ---------------------------------------------------------------
    // Context assembly
    // ---------------------------------------------------------------

    /**
     * Assembles chunk texts into a single context string, respecting the max length.
     * Each chunk is wrapped with source attribution for the LLM.
     */
    private String assembleContext(List<SearchHit> hits) {
        if (hits.isEmpty()) {
            return "";
        }

        int maxLength = ragProperties.getMaxContextLength();
        StringBuilder context = new StringBuilder();
        int chunkIndex = 1;

        for (SearchHit hit : hits) {
            String sourceName = hit.getSourceDocument() != null && hit.getSourceDocument().getName() != null
                    ? hit.getSourceDocument().getName()
                    : "Unknown document";

            String chunkEntry = String.format(
                    "[Source %d: %s (score: %.2f)]\n%s\n\n",
                    chunkIndex++, sourceName, hit.getScore(), hit.getChunkText()
            );

            if (context.length() + chunkEntry.length() > maxLength) {
                int remaining = maxLength - context.length();
                if (remaining > 100) {
                    context.append(chunkEntry, 0, remaining);
                    context.append("\n... (context truncated)");
                }
                break;
            }

            context.append(chunkEntry);
        }

        return context.toString().trim();
    }

    // ---------------------------------------------------------------
    // Prompt building
    // ---------------------------------------------------------------

    private String resolveSystemPrompt(RagPromptRequest request) {
        if (request.getSystemPrompt() != null && !request.getSystemPrompt().isBlank()) {
            return request.getSystemPrompt();
        }
        return ragProperties.getDefaultSystemPrompt();
    }

    private String buildUserPrompt(String question, String context) {
        return buildUserPrompt(question, question, "", context);
    }

    private String buildUserPrompt(String question, String retrievalQuery, String history, String context) {
        String conversationSection = history == null || history.isBlank() ? "(none)" : history;
        return String.format("""
                Based on the conversation history and document context, answer the current question.

                --- CONVERSATION HISTORY ---
                %s
                --- END CONVERSATION HISTORY ---

                --- DOCUMENT CONTEXT ---
                %s
                --- END CONTEXT ---

                Current question: %s
                Retrieval query: %s

                Answer:""",
                conversationSection, context, question, retrievalQuery);
    }

    private String assembleConversationHistory(List<ConversationTurn> turns) {
        if (turns == null || turns.isEmpty()) {
            return "";
        }

        StringBuilder history = new StringBuilder();
        for (ConversationTurn turn : turns) {
            String role = turn.getRole() == ConversationTurn.Role.ASSISTANT ? "Assistant" : "User";
            if (turn.getContent() != null && !turn.getContent().isBlank()) {
                history.append(role).append(": ").append(turn.getContent().trim()).append("\n");
            }
        }
        return history.toString().trim();
    }

    private String resolveRetrievalQuery(String originalQuestion, List<ConversationTurn> history) {
        if (!ragProperties.getConversation().isEnabled()) {
            return originalQuestion;
        }
        if (!ragProperties.getConversation().isQueryReformulation() || history.isEmpty()) {
            return originalQuestion;
        }
        String rewritten = queryReformulationService.reformulate(originalQuestion, history);
        if (rewritten == null || rewritten.isBlank()) {
            return originalQuestion;
        }
        return rewritten;
    }

    private String resolveSessionId(RagPromptRequest request) {
        if (request.getSessionId() != null && !request.getSessionId().isBlank()) {
            return request.getSessionId().trim();
        }
        String username = securityContextService.getCurrentUsername();
        if (username == null || username.isBlank()) {
            return "user:anonymous";
        }
        return "user:" + username.trim();
    }

    // ---------------------------------------------------------------
    // Pipeline state (shared by sync + stream flows)
    // ---------------------------------------------------------------

    private record ConversationState(boolean enabled, String sessionId, List<ConversationTurn> history) {
    }

    private record RetrievalState(String retrievalQuery, long searchTimeMs, List<SearchHit> rerankedHits) {
    }

    private record PromptState(String systemPrompt, String userPrompt) {
    }

    private record PromptPreparation(ConversationState conversation, RetrievalState retrieval, PromptState prompt) {
    }

    private record GenerationResult(String answer, String modelName, Integer tokenCount, long generationTimeMs) {
    }

    private static final class StreamAccumulator {
        private String model;
        private Integer tokenCount;
    }
}
