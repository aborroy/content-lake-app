package org.hyland.contentlake.rag.config;

import lombok.Data;
import org.hyland.contentlake.client.EmbeddingTypeCatalog;
import org.hyland.contentlake.client.HxprDocumentApi;
import org.hyland.contentlake.client.HxprQueryApi;
import org.hyland.contentlake.client.HxprService;
import org.hyland.contentlake.client.HxprVocabularyApi;
import org.hyland.contentlake.client.NamedQueryService;
import org.hyland.contentlake.client.VocabularyService;
import org.hyland.contentlake.rag.conversation.ConversationMemoryStore;
import org.hyland.contentlake.rag.conversation.InMemoryConversationMemoryStore;
import org.hyland.contentlake.rag.service.MultiTypeVectorSearchService;
import org.hyland.contentlake.service.EmbeddingBackfillService;
import org.hyland.contentlake.service.EmbeddingService;
import org.hyland.contentlake.service.EmbeddingTypeResolver;
import org.hyland.contentlake.service.chunking.NoiseReductionService;
import org.hyland.contentlake.service.chunking.SimpleChunkingService;
import org.hyland.contentlake.service.chunking.strategy.ChunkingStrategy;
import org.springframework.ai.embedding.EmbeddingModel;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.ClientHttpRequestInterceptor;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.support.RestClientAdapter;
import org.springframework.web.service.invoker.HttpServiceProxyFactory;

import java.time.Clock;
import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Central Spring configuration for the RAG service infrastructure.
 *
 * <p>Wires hxpr clients, embedding service, and token provider — the same
 * dependencies used by the batch-ingester but without ingestion-specific beans
 * (chunker, transform client, batch executor, etc.).</p>
 */
@Configuration
@EnableConfigurationProperties({
        RagAppConfig.HxprProperties.class
})
public class RagAppConfig {

    public static final String HXCS_REPOSITORY = "HXCS-REPOSITORY";

    // ----------------------------------------------------------------------
    // HXPR (Content Lake) wiring
    // ----------------------------------------------------------------------

    @Bean
    public RestClient hxprRestClient(HxprProperties props) {
        return RestClient.builder()
                .baseUrl(props.getUrl())
                .requestInterceptor(hxprAuthInterceptor(props))
                .build();
    }

    @Bean
    public HxprDocumentApi hxprDocumentApi(RestClient hxprRestClient) {
        return httpProxyFactory(hxprRestClient).createClient(HxprDocumentApi.class);
    }

    @Bean
    public HxprQueryApi hxprQueryApi(RestClient hxprRestClient) {
        return httpProxyFactory(hxprRestClient).createClient(HxprQueryApi.class);
    }

    /**
     * Vocabulary client. Registered here only — vocabulary normalization is consumed by
     * rag-service alone, so the ingester configs deliberately do not wire this bean.
     */
    @Bean
    public HxprVocabularyApi hxprVocabularyApi(RestClient hxprRestClient) {
        return httpProxyFactory(hxprRestClient).createClient(HxprVocabularyApi.class);
    }

    @Bean
    public VocabularyService vocabularyService(HxprVocabularyApi hxprVocabularyApi) {
        return new VocabularyService(hxprVocabularyApi);
    }

    @Bean
    public HxprService hxprService(HxprDocumentApi documentApi,
                                   HxprQueryApi queryApi,
                                   RestClient hxprRestClient,
                                   @Value("${spring.ai.openai.embedding.model:}") String embeddingModelName) {
        return new HxprService(documentApi, queryApi, hxprRestClient,
                EmbeddingTypeResolver.toEmbeddingType(embeddingModelName));
    }

    @Bean
    public NamedQueryService namedQueryService(HxprService hxprService) {
        return new NamedQueryService(hxprService);
    }

    // ----------------------------------------------------------------------
    // Embedding service
    // ----------------------------------------------------------------------

    /**
     * The model name must be the configured model, not the implementation class name: it is
     * reported as the embedding model on search responses and it is the value the ingesters derive
     * the hxpr embedding type from, so a class name here made rag-service disagree with every
     * writer about which model the corpus was embedded under.
     */
    @Bean
    public EmbeddingService embeddingService(
            EmbeddingModel embeddingModel,
            @Value("${spring.ai.openai.embedding.model:}") String embeddingModelName) {
        return new EmbeddingService(embeddingModel, embeddingModelName);
    }

    // ----------------------------------------------------------------------
    // Multi-embedding-type retrieval and backfill (#121)
    // ----------------------------------------------------------------------

    /**
     * The embedding types present in the index.
     *
     * <p>Seeded with the configured type, which is what it falls back to when discovery is off or the
     * lookup fails, so retrieval keeps working on an index it cannot enumerate.</p>
     */
    /**
     * Text embedded once to obtain a probe vector for the type scan. Its content is immaterial: the scan
     * reads each returned row's {@code sysembed_type} and discards the scores, exactly as
     * {@code IndexProofService} does for its chunk count.
     */
    private static final String TYPE_DISCOVERY_PROBE_TEXT = "content lake embedding type discovery probe";

    @Bean
    public EmbeddingTypeCatalog embeddingTypeCatalog(HxprService hxprService,
                                                     EmbeddingService embeddingService,
                                                     RagProperties ragProperties,
                                                     Clock clock) {
        RagProperties.EmbeddingProperties.TypeDiscoveryProperties discovery =
                ragProperties.getEmbedding().getTypeDiscovery();
        return new EmbeddingTypeCatalog(
                hxprService,
                () -> embeddingService.embed(TYPE_DISCOVERY_PROBE_TEXT),
                hxprService.getEmbeddingType(),
                discovery.isEnabled(),
                Duration.ofSeconds(discovery.getTtlSeconds()),
                clock);
    }

    @Bean
    public MultiTypeVectorSearchService multiTypeVectorSearchService(HxprService hxprService,
                                                                    EmbeddingTypeCatalog embeddingTypeCatalog,
                                                                    EmbeddingService embeddingService,
                                                                    RagProperties ragProperties) {
        return new MultiTypeVectorSearchService(hxprService, embeddingTypeCatalog, embeddingService,
                ragProperties.getEmbedding().getAdditionalModels());
    }

    /**
     * Chunking for the backfill only.
     *
     * <p>Configured from the same {@code EMBEDDING_*} settings the ingesters use, because the backfill
     * re-chunks stored text and chunk boundaries that disagreed with the ingesters' would produce a
     * corpus chunked two different ways.</p>
     */
    @Bean
    public SimpleChunkingService backfillChunkingService(
            @Value("${embedding.min-chunk-size:200}") int minChunkSize,
            @Value("${embedding.chunk-size:1024}") int chunkSize,
            @Value("${embedding.chunk-overlap:256}") int chunkOverlap,
            @Value("${embedding.similarity-threshold:0.75}") double similarityThreshold,
            @Value("${embedding.noise-reduction.enabled:true}") boolean noiseReductionEnabled,
            @Value("${embedding.noise-reduction.aggressive:false}") boolean noiseReductionAggressive) {
        return new SimpleChunkingService(
                new NoiseReductionService(noiseReductionEnabled, noiseReductionAggressive),
                new ChunkingStrategy.ChunkingConfig(minChunkSize, chunkSize, chunkOverlap, similarityThreshold));
    }

    /**
     * Single-threaded on purpose: the job's whole point is to spend embedding throughput at a rate an
     * operator chose, and a pool would make that rate meaningless.
     */
    @Bean(destroyMethod = "shutdownNow")
    public ExecutorService embeddingBackfillExecutor() {
        return Executors.newSingleThreadExecutor(runnable -> {
            Thread thread = new Thread(runnable, "embedding-backfill");
            thread.setDaemon(true);
            return thread;
        });
    }

    @Bean
    public EmbeddingBackfillService embeddingBackfillService(HxprService hxprService,
                                                            EmbeddingService embeddingService,
                                                            SimpleChunkingService backfillChunkingService,
                                                            ExecutorService embeddingBackfillExecutor) {
        return new EmbeddingBackfillService(
                hxprService,
                embeddingService,
                backfillChunkingService::chunk,
                embeddingBackfillExecutor,
                Thread::sleep);
    }

    @Bean
    public Clock clock() {
        return Clock.systemUTC();
    }

    @Bean
    @ConditionalOnMissingBean(ConversationMemoryStore.class)
    public ConversationMemoryStore conversationMemoryStore() {
        return new InMemoryConversationMemoryStore();
    }

    // ----------------------------------------------------------------------
    // Helpers
    // ----------------------------------------------------------------------

    private static ClientHttpRequestInterceptor hxprAuthInterceptor(HxprProperties props) {
        return (request, body, execution) -> {
            request.getHeaders().setBasicAuth(props.getUsername(), props.getPassword());
            request.getHeaders().set(HXCS_REPOSITORY, props.getRepositoryId());
            return execution.execute(request, body);
        };
    }

    private static HttpServiceProxyFactory httpProxyFactory(RestClient restClient) {
        return HttpServiceProxyFactory
                .builderFor(RestClientAdapter.create(restClient))
                .build();
    }

    // ----------------------------------------------------------------------
    // Configuration properties
    // ----------------------------------------------------------------------

    @Data
    @ConfigurationProperties(prefix = "hxpr")
    public static class HxprProperties {
        private String url = "http://localhost:8080";
        private String repositoryId = "default";

        /** HTTP Basic credentials for the ai-ready-index engine (filestore user store). */
        private String username;
        private String password;
    }
}
