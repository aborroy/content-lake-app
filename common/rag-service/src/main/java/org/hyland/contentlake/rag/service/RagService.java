package org.hyland.contentlake.rag.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.hyland.contentlake.rag.conversation.ConversationKeys;
import org.hyland.contentlake.rag.conversation.ConversationMemoryService;
import org.hyland.contentlake.rag.conversation.ConversationTurn;
import org.hyland.contentlake.rag.conversation.SessionSummaryService;
import org.hyland.contentlake.rag.config.RagProperties;
import org.hyland.contentlake.rag.observability.RagObservations;
import org.hyland.contentlake.rag.observability.RetrievalFeatureSet;
import org.hyland.contentlake.rag.observability.TokenEstimator;
import org.hyland.contentlake.rag.model.HybridSearchRequest;
import org.hyland.contentlake.rag.model.RagPromptRequest;
import org.hyland.contentlake.rag.model.RagPromptResponse;
import org.hyland.contentlake.rag.model.ResponseFormat;
import org.hyland.contentlake.rag.model.StructuredAnswer;
import org.hyland.contentlake.rag.model.RagPromptResponse.ContextChunk;
import org.hyland.contentlake.rag.model.RagPromptResponse.Source;
import org.hyland.contentlake.rag.model.SemanticSearchResponse.SearchHit;
import org.hyland.contentlake.security.SecurityContextService;
import org.springframework.ai.chat.client.ChatClient;
import org.springframework.ai.chat.client.ChatClient.ChatClientRequestSpec;
import org.springframework.ai.chat.model.ChatResponse;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Service;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;
import reactor.core.Disposable;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

/**
 * RAG (Retrieval-Augmented Generation) service.
 *
 * <p>Both the synchronous and streaming paths drive a single Spring AI {@link ChatClient}
 * whose default advisor ({@link ContentLakeRetrievalAdvisor}) performs the
 * retrieve -&gt; rerank -&gt; augment steps as a composable unit. This service is now
 * responsible only for:</p>
 * <ol>
 *   <li>conversation state (session resolution, history, query reformulation),</li>
 *   <li>invoking the {@link ChatClient} with the right advisor params, and</li>
 *   <li>mapping the reranked hits recorded in a {@link RetrievalTrace} into the response.</li>
 * </ol>
 *
 * <p>Retrieval uses {@link HybridSearchService} when {@code rag.use-hybrid-search=true}
 * (default) and {@link SemanticSearchService} otherwise; the choice lives in the retriever.
 * The context sent to the LLM is capped at {@code rag.max-context-length} characters by the
 * advisor.</p>
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class RagService {

    private static final Pattern TOKEN_PATTERN = Pattern.compile("\\S+");

    @Value("${spring.ai.openai.chat.options.model:}")
    private String configuredModel;

    private final ChatClient ragChatClient;
    private final RagProperties ragProperties;
    private final ConversationMemoryService conversationMemoryService;
    private final QueryReformulationService queryReformulationService;
    private final SecurityContextService securityContextService;
    private final FilterInferenceService filterInferenceService;
    private final SessionSummaryService sessionSummaryService;
    private final CitationVerifier citationVerifier;
    private final StructuredAnswerService structuredAnswerService;
    private final RagToolset ragToolset;
    /** Optional (#73): null in unit tests that construct this service without the tracing collaborator. */
    private final RagObservations observations;
    /**
     * Which retrieval features configuration has enabled, as one low-cardinality span tag (#116).
     * Optional for the same reason as {@code observations}.
     */
    private final RetrievalFeatureSet retrievalFeatures;

    /**
     * Executes the full RAG pipeline for a given question.
     *
     * @param request the RAG prompt request
     * @return response with generated answer, sources, and timing
     */
    public RagPromptResponse prompt(RagPromptRequest request) {
        long totalStart = System.currentTimeMillis();
        String requestId = java.util.UUID.randomUUID().toString();

        // rag.request is the root of the RAG span tree: retrieval, augmentation and generation are its
        // descendants, created inside the advisor. Model name and token usage land here rather than on
        // rag.generate because they are only available once the ChatClient call returns, and because a
        // request can involve extra LLM calls (reformulation, HyDE, grading, citation verification)
        // that rag.generate does not cover.
        return obs().observe("rag.request",
                span -> tagRequest(span, request, "sync"),
                span -> {
                    PromptContext promptContext = prepareContext(request);
                    GenerationResult generation = generateAnswer(promptContext);
                    long totalTimeMs = System.currentTimeMillis() - totalStart;

                    persistConversationTurn(request, promptContext, generation);
                    RagPromptResponse response = buildPromptResponse(request, promptContext, generation,
                            totalTimeMs, requestId,
                            resolveStructuredAnswer(request, promptContext, generation));

                    enrichRequestSpan(span, requestId, request, promptContext, generation,
                            generation.tokenCount(), null);
                    return response;
                });
    }

    /**
     * Low-cardinality tags. Bounded by deployment configuration or by a small enum, so each is safe as
     * a metric tag; anything per-request goes on as an attribute instead.
     */
    private void tagRequest(RagObservations.Span span, RagPromptRequest request, String path) {
        span.tag("rag.path", path)
            .tag("rag.model", configuredModel != null && !configuredModel.isBlank() ? configuredModel : "unknown")
            .tag("rag.response.format", request.getResponseFormat() != null
                    ? request.getResponseFormat().name() : "TEXT");
        if (retrievalFeatures != null) {
            span.tag("rag.features", retrievalFeatures.value());
        }
    }

    /**
     * Attaches what the request cost and what it returned.
     *
     * <p>Everything here reads already-computed values, so it is safe to call from the streaming
     * completion callback <em>after</em> the metadata event has been sent.</p>
     *
     * @param usageTokens provider-reported total tokens, or null when the provider did not report any
     * @param accumulator streaming accumulator, or null on the synchronous path
     */
    private void enrichRequestSpan(RagObservations.Span span,
                                   String requestId,
                                   RagPromptRequest request,
                                   PromptContext promptContext,
                                   GenerationResult generation,
                                   Integer usageTokens,
                                   StreamAccumulator accumulator) {
        if (!obs().payloadsEnabled()) {
            return;
        }

        span.attr("rag.request.id", requestId)
            .attr("rag.answer.length", generation.answer() == null ? 0 : generation.answer().length())
            .attr("rag.generate.time_ms", generation.generationTimeMs())
            .attr("rag.retrieve.reranked", promptContext.trace().rerankedHits().size())
            .attr("rag.outcome", promptContext.trace().rerankedHits().isEmpty() ? "no-context" : "answered")
            .content("rag.query.text", request.getQuestion())
            .content("rag.answer.text", generation.answer());

        if (promptContext.retrievalQuery() != null
                && !promptContext.retrievalQuery().equals(request.getQuestion())) {
            span.attr("rag.query.reformulated", true)
                .content("rag.query.retrieval_text", promptContext.retrievalQuery());
        }

        String sessionId = promptContext.conversation() != null ? promptContext.conversation().sessionId() : null;
        if (sessionId != null) {
            // Never the raw session id: it is "user:<username>", so exporting it would put a username
            // in a third-party backend. A truncated hash still correlates a conversation's spans.
            span.attr("rag.session.principal_hash", principalHash(sessionId));
        }

        // Token provenance is the point. getPromptTokens() is absent or zero on the streaming path with
        // several local backends, so an unlabelled prompt-token number is a trap for anyone comparing a
        // streamed run against a synchronous one and concluding the prompt grew.
        if (usageTokens != null && usageTokens > 0) {
            span.attr("rag.tokens.total", usageTokens)
                .attr("rag.tokens.total.source", "usage");
        } else {
            span.attr("rag.tokens.total", TokenEstimator.estimate(generation.answer()))
                .attr("rag.tokens.total.source", accumulator != null ? "estimated" : "unavailable");
        }
    }

    /** SHA-256 of the session id, truncated. Correlates a conversation without exporting a username. */
    private static String principalHash(String sessionId) {
        try {
            byte[] digest = java.security.MessageDigest.getInstance("SHA-256")
                    .digest(sessionId.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            StringBuilder hex = new StringBuilder();
            for (int i = 0; i < 8; i++) {
                hex.append(String.format("%02x", digest[i]));
            }
            return hex.toString();
        } catch (Exception e) {
            return "unavailable";
        }
    }

    /**
     * Streams LLM output token-by-token over SSE.
     *
     * <p>Emits:</p>
     * <ul>
     *   <li>{@code event: token} for every non-empty token delta</li>
     *   <li>{@code event: metadata} with the final {@link RagPromptResponse}</li>
     *   <li>{@code event: structured} with the {@link StructuredAnswer}, only when the request asked
     *       for {@code responseFormat=STRUCTURED} and something was retrieved. It follows
     *       {@code metadata} rather than travelling inside it because deriving it is a second LLM
     *       pass over the finished answer, and the client should not wait for it to see its
     *       sources.</li>
     *   <li>{@code event: done} when the stream completes</li>
     *   <li>{@code event: error} on terminal failures</li>
     * </ul>
     */
    public SseEmitter streamPrompt(RagPromptRequest request) {
        SseEmitter emitter = newEmitter();
        final long totalStart = System.currentTimeMillis();
        final String requestId = java.util.UUID.randomUUID().toString();

        final PromptContext promptContext;
        try {
            promptContext = prepareContext(request);
        } catch (Exception e) {
            log.error("RAG stream preparation failed: {}", e.getMessage(), e);
            sendErrorEvent(emitter, "Failed to prepare RAG stream: " + e.getMessage());
            emitter.complete();
            return emitter;
        }

        StringBuilder answerBuilder = new StringBuilder();
        StreamAccumulator accumulator = new StreamAccumulator();
        long generationStart = System.currentTimeMillis();

        // Opened manually rather than around a lambda: the answer arrives over a reactive stream, so
        // the span has to outlive this method. openScope() makes it current on the subscribing thread,
        // which is where the advisor runs retrieval and augmentation eagerly, so those spans are
        // correctly parented. No Reactor context propagation is needed and none should be added: the
        // token callbacks attach nothing, and the completion callback only enriches a span it holds a
        // reference to. Span creation needs thread-local context; attribute attachment does not.
        RagObservations.Span requestSpan = obs().start("rag.request",
                span -> tagRequest(span, request, "stream"));

        Disposable subscription;
        try (AutoCloseable ignored = requestSpan.openScope()) {
            subscription = requestSpec(promptContext)
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
                            requestSpan.error(error).close();
                            sendErrorEvent(emitter, error.getMessage());
                            emitter.complete();
                        },
                        () -> {
                            long generationTimeMs = System.currentTimeMillis() - generationStart;
                            String answer = answerBuilder.toString();
                            boolean hasContext = !promptContext.trace().rerankedHits().isEmpty();
                            GenerationResult generation = new GenerationResult(
                                    answer,
                                    hasContext ? resolveStreamModel(accumulator) : "none (no context available)",
                                    resolveStreamTokenCount(accumulator, answer),
                                    generationTimeMs
                            );
                            // Both of these must stay cheap: the client has the whole answer on
                            // screen already but cannot render its sources until the metadata event
                            // below arrives, so anything slow here reads as a stall. The running
                            // summary refresh this triggers is queued, not awaited.
                            persistConversationTurn(request, promptContext, generation);
                            RagPromptResponse response = buildPromptResponse(
                                    request, promptContext, generation,
                                    System.currentTimeMillis() - totalStart, requestId, null
                            );
                            sendMetadataEvent(emitter, response);

                            // Deliberately after the metadata send, so the streaming-latency
                            // guarantee is structural rather than "the payload happens to be small".
                            // Everything attached here reads already-computed values.
                            enrichRequestSpan(requestSpan, requestId, request, promptContext, generation,
                                    accumulator.tokenCount, accumulator);
                            requestSpan.close();

                            // The typed view is a second LLM pass over the finished answer, so it
                            // gets its own event: holding the metadata back for it would leave the
                            // client with a complete answer and no sources for the whole call.
                            StructuredAnswer structured =
                                    resolveStructuredAnswer(request, promptContext, generation);
                            if (structured != null) {
                                sendStructuredEvent(emitter, structured);
                            }
                            sendDoneEvent(emitter);
                            emitter.complete();
                        });
        } catch (Exception e) {
            // openScope()'s close() is declared to throw; a failure there must not lose the request.
            log.warn("RAG stream scope handling failed: {}", e.getMessage());
            requestSpan.close();
            sendErrorEvent(emitter, "Failed to start RAG stream: " + e.getMessage());
            emitter.complete();
            return emitter;
        }

        final Disposable finalSubscription = subscription;
        // close() is idempotent, so covering every terminal signal cannot double-stop the span.
        emitter.onCompletion(() -> {
            finalSubscription.dispose();
            requestSpan.close();
        });
        emitter.onTimeout(() -> {
            finalSubscription.dispose();
            requestSpan.close();
            emitter.complete();
        });

        return emitter;
    }

    // ---------------------------------------------------------------
    // ChatClient invocation
    // ---------------------------------------------------------------

    /**
     * Builds the {@link ChatClientRequestSpec} shared by the sync and stream paths.
     * The system + user messages carry the prompt; advisor params carry the reformulated
     * retrieval query, the conversation-history block, and the {@link RetrievalTrace} holder
     * that the advisor fills during retrieval.
     */
    private ChatClientRequestSpec requestSpec(PromptContext promptContext) {
        ChatClientRequestSpec spec = ragChatClient.prompt()
                .system(promptContext.systemPrompt())
                .user(promptContext.question())
                .advisors(advisor -> advisor
                        .param(ContentLakeRetrievalAdvisor.PARAM_RETRIEVAL_QUERY, promptContext.retrievalQuery())
                        .param(ContentLakeRetrievalAdvisor.PARAM_HISTORY_BLOCK, promptContext.historyBlock())
                        .param(RetrievalTrace.PARAM_KEY, promptContext.trace())
                        .param(HxprDocumentRetriever.CTX_TOP_K, promptContext.topK())
                        .param(HxprDocumentRetriever.CTX_MIN_SCORE, promptContext.minScore())
                        .params(promptContext.optionalRetrievalParams()));

        // Agentic tool-calling (#65): register the toolset and capture the request-thread
        // Authentication so tools apply the same ACL filtering even when executed on a Reactor
        // (streaming) thread. Off by default; identity is never taken from tool arguments.
        if (ragProperties.getAgenticTools().isEnabled()) {
            Map<String, Object> toolContext = new HashMap<>();
            Authentication auth = SecurityContextHolder.getContext().getAuthentication();
            if (auth != null) {
                toolContext.put(RagToolset.CTX_AUTH, auth);
            }
            toolContext.put(RagToolset.CTX_ITERATIONS, new AtomicInteger(0));
            spec = spec.tools(ragToolset).toolContext(toolContext);
        }
        return spec;
    }

    private GenerationResult generateAnswer(PromptContext promptContext) {
        long generationStart = System.currentTimeMillis();

        try {
            // rag.generate is created by ContentLakeRetrievalAdvisor, which is the only code that
            // sits between retrieval and the model call. Wrapping this call instead put retrieval
            // inside the generation span, because the advisor's before-phase runs within it.
            ChatResponse chatResponse = requestSpec(promptContext).call().chatResponse();
            long generationTimeMs = System.currentTimeMillis() - generationStart;

            boolean hasContext = !promptContext.trace().rerankedHits().isEmpty();
            String answer = chatResponse != null && chatResponse.getResult() != null
                    ? chatResponse.getResult().getOutput().getText()
                    : "";
            String modelName = hasContext ? resolveModelName(chatResponse) : "none (no context available)";
            Integer tokenCount = hasContext ? resolveTokenCount(chatResponse) : null;

            log.info("RAG generate phase complete: model={}, answer length={} chars",
                    modelName, answer != null ? answer.length() : 0);

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

    /**
     * The SSE emitter for one streaming request. No timeout: generation over a local model routinely
     * outlasts any default.
     *
     * <p>A factory method rather than a direct {@code new} so a test can observe the order in which
     * events are sent. That ordering is the streaming-latency guarantee: span enrichment must happen
     * after the {@code metadata} event, because the client cannot render sources until it arrives.</p>
     */
    protected SseEmitter newEmitter() {
        return new SseEmitter(0L);
    }

    /** Observation collaborator, normalised so no call site branches on whether it is wired. */
    private RagObservations obs() {
        return observations != null ? observations : RagObservations.NOOP;
    }

    private String resolveModelName(ChatResponse chatResponse) {
        if (configuredModel != null && !configuredModel.isBlank()) {
            return configuredModel;
        }
        if (chatResponse != null && chatResponse.getMetadata() != null && chatResponse.getMetadata().getModel() != null) {
            return chatResponse.getMetadata().getModel();
        }
        return "unknown";
    }

    private Integer resolveTokenCount(ChatResponse chatResponse) {
        if (chatResponse != null && chatResponse.getMetadata() != null && chatResponse.getMetadata().getUsage() != null) {
            return chatResponse.getMetadata().getUsage().getTotalTokens();
        }
        return null;
    }

    // ---------------------------------------------------------------
    // Preparation (conversation state + retrieval query + prompt)
    // ---------------------------------------------------------------

    private PromptContext prepareContext(RagPromptRequest request) {
        ConversationState conversation = prepareConversationState(request);
        String retrievalQuery = resolveRetrievalQuery(request.getQuestion(), conversation.history());
        String historyBlock = assembleConversationHistory(conversation.history());
        // Long-term memory: prepend the persisted running summary so facts and intent from before the
        // sliding window (or before a restart) still reach the model. No-op when the feature is off.
        historyBlock = prependSessionSummary(conversation, historyBlock);
        String systemPrompt = resolveSystemPrompt(request);

        int topK = request.getTopK() > 0 ? request.getTopK() : ragProperties.getDefaultTopK();
        // An explicit 0.0 means "no threshold" and must survive; only an absent value falls back.
        double minScore = request.getMinScore() != null
                ? request.getMinScore()
                : ragProperties.getDefaultMinScore();

        // Intent-aware filter inference (opt-in). Additive to any caller-supplied filter; the
        // service returns null on failure so retrieval simply proceeds unfiltered.
        HybridSearchRequest.MetadataFilter inferredFilter = request.isInferFilters()
                ? filterInferenceService.infer(request.getQuestion())
                : null;

        return new PromptContext(
                conversation,
                request.getQuestion(),
                retrievalQuery,
                historyBlock,
                systemPrompt,
                topK,
                minScore,
                request.getFilter(),
                request.getSourceType(),
                request.getEmbeddingType(),
                inferredFilter,
                new RetrievalTrace()
        );
    }

    private ConversationState prepareConversationState(RagPromptRequest request) {
        boolean conversationEnabled = ragProperties.getConversation().isEnabled();
        if (!conversationEnabled) {
            return new ConversationState(false, null, null, List.of());
        }

        String owner = securityContextService.getCurrentUsername().trim();
        String sessionId = resolveSessionId(request, owner);
        String memoryKey = ConversationKeys.of(owner, sessionId);
        if (request.isResetSession()) {
            conversationMemoryService.resetSession(memoryKey);
        }

        List<ConversationTurn> history = conversationMemoryService.getRecentTurns(memoryKey);
        return new ConversationState(true, sessionId, memoryKey, history != null ? history : List.of());
    }

    private void persistConversationTurn(RagPromptRequest request,
                                         PromptContext promptContext,
                                         GenerationResult generation) {
        if (!promptContext.conversation().enabled()) {
            return;
        }
        conversationMemoryService.appendUserTurn(promptContext.conversation().memoryKey(), request.getQuestion());
        conversationMemoryService.appendAssistantTurn(promptContext.conversation().memoryKey(), generation.answer());
    }

    private RagPromptResponse buildPromptResponse(RagPromptRequest request,
                                                  PromptContext promptContext,
                                                  GenerationResult generation,
                                                  long totalTimeMs,
                                                  String requestId,
                                                  StructuredAnswer structured) {
        List<SearchHit> rerankedHits = promptContext.trace().rerankedHits();
        List<Source> sources = mapSources(rerankedHits);
        List<ContextChunk> contextChunks = request.isIncludeContext() ? mapContextChunks(rerankedHits) : null;
        String retrievalQuery = promptContext.trace().retrievalQuery() != null
                ? promptContext.trace().retrievalQuery()
                : promptContext.retrievalQuery();

        // Post-generation faithfulness check (opt-in). Null result leaves both fields absent.
        CitationVerifier.VerificationResult verification =
                citationVerifier.verify(generation.answer(), rerankedHits);

        // Persistent conversation summary (#50) surfaced to the caller so a client can show a
        // conversation-memory panel. Present only when the session carries memory and the feature
        // is enabled; null (and omitted from the JSON) otherwise.
        String currentSummary = (promptContext.conversation().enabled()
                && promptContext.conversation().sessionId() != null
                && sessionSummaryService.isEnabled())
                ? sessionSummaryService.loadSummary(promptContext.conversation().memoryKey())
                : null;

        return RagPromptResponse.builder()
                .answer(generation.answer())
                .requestId(requestId)
                .question(request.getQuestion())
                .sessionId(promptContext.conversation().sessionId())
                .currentSummary(currentSummary)
                .retrievalQuery(retrievalQuery)
                .historyTurnsUsed(promptContext.conversation().enabled()
                        ? promptContext.conversation().history().size()
                        : null)
                .model(generation.modelName())
                .tokenCount(generation.tokenCount())
                .searchTimeMs(promptContext.trace().searchTimeMs())
                .generationTimeMs(generation.generationTimeMs())
                .totalTimeMs(totalTimeMs)
                .sourcesUsed(sources.size())
                .sources(sources)
                .context(contextChunks)
                .verified(verification != null ? verification.verified() : null)
                .unsupportedClaims(verification != null ? verification.unsupportedClaims() : null)
                .structured(structured)
                .build();
    }

    /**
     * Derives the typed view of an answer (#70), or {@code null} when the caller did not ask for one
     * or nothing was retrieved to ground it.
     */
    private StructuredAnswer resolveStructuredAnswer(RagPromptRequest request,
                                                     PromptContext promptContext,
                                                     GenerationResult generation) {
        List<SearchHit> rerankedHits = promptContext.trace().rerankedHits();
        if (request.getResponseFormat() != ResponseFormat.STRUCTURED || rerankedHits.isEmpty()) {
            return null;
        }
        return structuredAnswerService.summarize(request.getQuestion(), generation.answer(), rerankedHits);
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
                        .chunkType(hit.getChunkMetadata() != null ? hit.getChunkMetadata().getChunkType() : null)
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
                        .chunkType(hit.getChunkMetadata() != null ? hit.getChunkMetadata().getChunkType() : null)
                        .build())
                .toList();
    }

    // ---------------------------------------------------------------
    // SSE helpers
    // ---------------------------------------------------------------

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

    private void sendStructuredEvent(SseEmitter emitter, StructuredAnswer structured) {
        try {
            emitter.send(SseEmitter.event()
                    .name("structured")
                    .data(structured));
        } catch (IOException e) {
            throw new IllegalStateException("Failed to send SSE structured event", e);
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

    private Integer resolveStreamTokenCount(StreamAccumulator accumulator, String answer) {
        if (accumulator.tokenCount != null && accumulator.tokenCount > 0) {
            return accumulator.tokenCount;
        }
        return estimateTokenCount(answer);
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
    // Prompt building / conversation
    // ---------------------------------------------------------------

    private String resolveSystemPrompt(RagPromptRequest request) {
        if (request.getSystemPrompt() != null && !request.getSystemPrompt().isBlank()) {
            return request.getSystemPrompt();
        }
        return ragProperties.getDefaultSystemPrompt();
    }

    private String prependSessionSummary(ConversationState conversation, String historyBlock) {
        if (!conversation.enabled() || conversation.sessionId() == null
                || !sessionSummaryService.isEnabled()) {
            return historyBlock;
        }
        String summary = sessionSummaryService.loadSummary(conversation.memoryKey());
        if (summary == null || summary.isBlank()) {
            return historyBlock;
        }
        String summaryBlock = "Conversation summary so far:\n" + summary.trim();
        return (historyBlock == null || historyBlock.isBlank())
                ? summaryBlock
                : summaryBlock + "\n\n" + historyBlock;
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

    private String resolveSessionId(RagPromptRequest request, String owner) {
        if (request.getSessionId() != null && !request.getSessionId().isBlank()) {
            return request.getSessionId().trim();
        }
        return "user:" + owner;
    }

    // ---------------------------------------------------------------
    // Pipeline state
    // ---------------------------------------------------------------

    /**
     * @param sessionId the client's session id, echoed back in the response
     * @param memoryKey the owner-scoped key memory and summaries are stored under; see {@link ConversationKeys}
     */
    private record ConversationState(boolean enabled, String sessionId, String memoryKey,
                                     List<ConversationTurn> history) {
    }

    private record PromptContext(ConversationState conversation,
                                 String question,
                                 String retrievalQuery,
                                 String historyBlock,
                                 String systemPrompt,
                                 int topK,
                                 double minScore,
                                 String filter,
                                 String sourceType,
                                 String embeddingType,
                                 HybridSearchRequest.MetadataFilter metadataFilter,
                                 RetrievalTrace trace) {

        /** Non-null optional retrieval params for the advisor context (filter/sourceType/embeddingType/metadata). */
        Map<String, Object> optionalRetrievalParams() {
            java.util.Map<String, Object> params = new java.util.HashMap<>();
            if (filter != null) {
                params.put(HxprDocumentRetriever.CTX_FILTER, filter);
            }
            if (sourceType != null) {
                params.put(HxprDocumentRetriever.CTX_SOURCE_TYPE, sourceType);
            }
            if (embeddingType != null) {
                params.put(HxprDocumentRetriever.CTX_EMBEDDING_TYPE, embeddingType);
            }
            if (metadataFilter != null) {
                params.put(HxprDocumentRetriever.CTX_METADATA_FILTER, metadataFilter);
            }
            return params;
        }
    }

    private record GenerationResult(String answer, String modelName, Integer tokenCount, long generationTimeMs) {
    }

    private static final class StreamAccumulator {
        private String model;
        private Integer tokenCount;
    }
}
