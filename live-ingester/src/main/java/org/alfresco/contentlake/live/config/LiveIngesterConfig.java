package org.alfresco.contentlake.live.config;

import lombok.Data;
import org.alfresco.contentlake.client.AlfrescoClient;
import org.alfresco.contentlake.client.HxprDocumentApi;
import org.alfresco.contentlake.client.HxprQueryApi;
import org.alfresco.contentlake.client.HxprService;
import org.alfresco.contentlake.client.HxprTokenProvider;
import org.alfresco.contentlake.client.TransformClient;
import org.alfresco.contentlake.service.EmbeddingService;
import org.alfresco.contentlake.service.NodeSyncService;
import org.alfresco.contentlake.service.chunking.NoiseReductionService;
import org.alfresco.contentlake.service.chunking.SimpleChunkingService;
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
 * Central Spring configuration for the live-ingester module.
 *
 * <p>Wires the shared ingestion infrastructure and lets the Alfresco Java SDK
 * starter own the Event2 ActiveMQ subscription.</p>
 */
@Configuration
@EnableConfigurationProperties({
        LiveIngesterProperties.class,
        LiveIngesterConfig.HxprProperties.class,
        LiveIngesterConfig.TransformProperties.class,
        LiveIngesterConfig.EmbeddingProperties.class
})
public class LiveIngesterConfig {

    public static final String HXCS_REPOSITORY = "HXCS-REPOSITORY";

    // ──────────────────────────────────────────────────────────────────────
    // HXPR (Content Lake) wiring
    // ──────────────────────────────────────────────────────────────────────

    @Bean
    public HxprTokenProvider hxprTokenProvider(HxprProperties props) {
        var idp = props.getIdp();
        return new HxprTokenProvider(
                idp.getTokenUrl(), idp.getClientId(), idp.getClientSecret(),
                idp.getUsername(), idp.getPassword()
        );
    }

    @Bean
    public RestClient hxprRestClient(HxprProperties props, HxprTokenProvider tokenProvider) {
        return RestClient.builder()
                .baseUrl(props.getUrl())
                .requestInterceptor(hxprAuthInterceptor(props, tokenProvider))
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

    @Bean
    public HxprService hxprService(HxprDocumentApi documentApi, HxprQueryApi queryApi,
                                   RestClient hxprRestClient, HxprProperties props) {
        return new HxprService(documentApi, queryApi, hxprRestClient, props.getRepositoryId());
    }

    // ──────────────────────────────────────────────────────────────────────
    // Transform Service
    // ──────────────────────────────────────────────────────────────────────

    @Bean
    public TransformClient transformClient(TransformProperties props) {
        return new TransformClient(props.getUrl(), props.getTimeoutMs());
    }

    // ──────────────────────────────────────────────────────────────────────
    // Embedding pipeline
    // ──────────────────────────────────────────────────────────────────────

    @Bean
    public EmbeddingService embeddingService(EmbeddingModel embeddingModel, EmbeddingProperties props) {
        return new EmbeddingService(embeddingModel, props.getModelName());
    }

    @Bean
    public NoiseReductionService noiseReductionService() {
        return new NoiseReductionService(false);
    }

    @Bean
    public ChunkingConfig chunkingConfig() {
        return new ChunkingConfig(200, 1000, 120, 0.75);
    }

    @Bean
    public SimpleChunkingService chunkingService(NoiseReductionService nr, ChunkingConfig cfg) {
        return new SimpleChunkingService(nr, cfg);
    }

    // ──────────────────────────────────────────────────────────────────────
    // NodeSyncService (shared pipeline)
    // ──────────────────────────────────────────────────────────────────────

    @Bean
    public NodeSyncService nodeSyncService(
            AlfrescoClient alfrescoClient,
            HxprDocumentApi documentApi,
            HxprService hxprService,
            TransformClient transformClient,
            EmbeddingService embeddingService,
            SimpleChunkingService chunkingService,
            HxprProperties hxprProps
    ) {
        return new NodeSyncService(
                alfrescoClient, documentApi, hxprService,
                transformClient, embeddingService, chunkingService,
                hxprProps.getTargetPath(),
                hxprProps.getPathRepositoryId()
        );
    }

    // ──────────────────────────────────────────────────────────────────────
    // Helpers
    // ──────────────────────────────────────────────────────────────────────

    private static ClientHttpRequestInterceptor hxprAuthInterceptor(
            HxprProperties props, HxprTokenProvider tokenProvider) {
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

    // ──────────────────────────────────────────────────────────────────────
    // Configuration properties (same shape as batch-ingester)
    // ──────────────────────────────────────────────────────────────────────

    @Data
    @ConfigurationProperties(prefix = "hxpr")
    public static class HxprProperties {
        private String url = "http://localhost:8080";
        private String repositoryId = "default";
        private String targetPath = "/alfresco-sync";
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

    @Data
    @ConfigurationProperties(prefix = "transform")
    public static class TransformProperties {
        private String url = "http://localhost:10090";
        private long timeoutMs = 300000;
        private boolean enabled = true;
    }

    @Data
    @ConfigurationProperties(prefix = "embedding")
    public static class EmbeddingProperties {
        private String modelName = "default";
    }
}
