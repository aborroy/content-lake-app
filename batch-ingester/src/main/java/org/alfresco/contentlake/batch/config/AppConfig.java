package org.alfresco.contentlake.batch.config;

import lombok.Data;
import org.alfresco.contentlake.client.HxprDocumentApi;
import org.alfresco.contentlake.client.HxprQueryApi;
import org.alfresco.contentlake.client.HxprService;
import org.alfresco.contentlake.client.HxprTokenProvider;
import org.alfresco.contentlake.client.TransformClient;
import org.alfresco.contentlake.service.Chunker;
import org.alfresco.contentlake.service.EmbeddingService;
import org.alfresco.contentlake.service.NodeSyncService;
import org.alfresco.contentlake.service.chunking.*;
import org.alfresco.contentlake.service.chunking.strategy.ChunkingStrategy.ChunkingConfig;
import org.springframework.ai.embedding.EmbeddingModel;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.ClientHttpRequestInterceptor;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.support.RestClientAdapter;
import org.springframework.web.service.invoker.HttpServiceProxyFactory;

/**
 * Central Spring configuration for batch-ingester infrastructure.
 */
@Configuration
@EnableConfigurationProperties({
        IngestionProperties.class,
        AppConfig.HxprProperties.class,
        AppConfig.TransformProperties.class
})
public class AppConfig {

    public static final String HXCS_REPOSITORY = "HXCS-REPOSITORY";

    // ----------------------------------------------------------------------
    // HXPR (Content Lake) wiring
    // ----------------------------------------------------------------------

    /**
     * Provides OAuth / IDP tokens for HXPR requests.
     */
    @Bean
    public HxprTokenProvider hxprTokenProvider(HxprProperties props) {
        HxprProperties.IdpConfig idp = props.getIdp();
        return new HxprTokenProvider(
                idp.getTokenUrl(),
                idp.getClientId(),
                idp.getClientSecret(),
                idp.getUsername(),
                idp.getPassword()
        );
    }

    /**
     * Base RestClient for HXPR with authentication and repository scoping.
     */
    @Bean
    public RestClient hxprRestClient(HxprProperties props, HxprTokenProvider tokenProvider) {
        return RestClient.builder()
                .baseUrl(props.getUrl())
                .requestInterceptor(hxprAuthInterceptor(props, tokenProvider))
                .build();
    }

    /**
     * HXPR document API proxy.
     */
    @Bean
    public HxprDocumentApi hxprDocumentApi(RestClient hxprRestClient) {
        return httpProxyFactory(hxprRestClient).createClient(HxprDocumentApi.class);
    }

    /**
     * HXPR query/search API proxy.
     */
    @Bean
    public HxprQueryApi hxprQueryApi(RestClient hxprRestClient) {
        return httpProxyFactory(hxprRestClient).createClient(HxprQueryApi.class);
    }

    /**
     * High-level HXPR service combining document and query APIs.
     */
    @Bean
    public HxprService hxprService(HxprDocumentApi documentApi,
                                   HxprQueryApi queryApi,
                                   RestClient hxprRestClient,
                                   HxprProperties props) {
        return new HxprService(documentApi, queryApi, hxprRestClient, props.getRepositoryId());
    }

    // ----------------------------------------------------------------------
    // Transform Service
    // ----------------------------------------------------------------------

    /**
     * Client for the Transform Service.
     */
    @Bean
    public TransformClient transformClient(TransformProperties props) {
        return new TransformClient(props.getUrl(), props.getTimeoutMs());
    }

    // ----------------------------------------------------------------------
    // Embedding pipeline helpers
    // ----------------------------------------------------------------------

    /**
     * Chunker used to split extracted text before embedding.
     *
     * <p>Chunk size and overlap are passed explicitly where this bean is created
     * (or can be refactored later into a dedicated properties class).
     */
    @Bean
    public Chunker chunker(IngestionProperties props) {
        return new Chunker(
                props.getEmbedding().getChunkSize(),
                props.getEmbedding().getChunkOverlap()
        );
    }

    /**
     * Embedding service wrapping the Spring AI embedding model.
     */
    @Bean
    public EmbeddingService embeddingService(EmbeddingModel embeddingModel, IngestionProperties props) {
        return new EmbeddingService(embeddingModel, props.getEmbedding().getModelName());
    }

    // ----------------------------------------------------------------------
    // Chunking pipeline
    // ----------------------------------------------------------------------

    @Bean
    public NoiseReductionService noiseReductionService(IngestionProperties props) {
        return new NoiseReductionService(
                props.getEmbedding().getNoiseReduction().isAggressive()
        );
    }

    @Bean
    public ChunkingConfig chunkingConfig() {
        return new ChunkingConfig(
                200,   // minChunkSize: Small enough for short paragraphs
                1000,  // maxChunkSize: Safe for 512-token embedding models
                120,   // overlapSize: Context preservation across boundaries
                0.75   // similarityThreshold: Unused in adaptive strategy
        );
    }

    @Bean
    public SimpleChunkingService chunkingService(
            NoiseReductionService noiseReduction,
            ChunkingConfig config) {
        return new SimpleChunkingService(noiseReduction, config);
    }

    @Bean
    public NodeSyncService nodeSyncService(
            org.alfresco.contentlake.client.AlfrescoClient alfrescoClient,
            HxprDocumentApi documentApi,
            HxprService hxprService,
            TransformClient transformClient,
            EmbeddingService embeddingService,
            SimpleChunkingService chunkingService,
            HxprProperties props
    ) {
        return new NodeSyncService(
                alfrescoClient,
                documentApi,
                hxprService,
                transformClient,
                embeddingService,
                chunkingService,
                props.getTargetPath(),
                props.getPathRepositoryId()
        );
    }

    // ----------------------------------------------------------------------
    // Helpers
    // ----------------------------------------------------------------------

    private static ClientHttpRequestInterceptor hxprAuthInterceptor(HxprProperties props,
                                                                    HxprTokenProvider tokenProvider) {
        return (request, body, execution) -> {
            request.getHeaders().setBearerAuth(tokenProvider.getToken());
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

    /**
     * HXPR connection and authentication properties.
     *
     * <p>Bound from {@code hxpr.*} in {@code application.yml}.
     */
    @Data
    @ConfigurationProperties(prefix = "hxpr")
    public static class HxprProperties {
        private String url = "http://localhost:8080";
        private String repositoryId = "default";
        /**
         * Base target path in HXPR where Alfresco structures are created.
         * <p>Hierarchy is built under this root using:
         * {@code {targetPath}/{pathRepositoryId or alfrescoRepositoryId}/{alfrescoPath}}.</p>
         */
        private String targetPath = "/alfresco-sync";
        /**
         * Optional repository-id override used only for path prefixing in Content Lake.
         * <p>When empty, the ingester uses Alfresco Discovery repository id.
         * Set this when HXPR permissions require a fixed writable path prefix.</p>
         */
        private String pathRepositoryId;
        private IdpConfig idp = new IdpConfig();

        @Data
        public static class IdpConfig {
            private String tokenUrl;
            private String clientId;
            private String clientSecret;
            private String username;
            private String password;
        }
    }

    /**
     * Transform Service properties.
     *
     * <p>Bound from {@code transform.*} in {@code application.yml}.
     */
    @Data
    @ConfigurationProperties(prefix = "transform")
    public static class TransformProperties {
        private String url = "http://localhost:8090";
        private long timeoutMs = 120000;
        private boolean enabled = true;
    }
}
