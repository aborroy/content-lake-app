package org.hyland.alfresco.contentlake.batch.config;

import org.hyland.alfresco.contentlake.client.AlfrescoClient;
import org.hyland.contentlake.client.HxprDocumentApi;
import org.hyland.contentlake.client.HxprQueryApi;
import org.hyland.contentlake.client.HxprService;
import org.hyland.contentlake.client.HxprTokenProvider;
import org.hyland.alfresco.contentlake.client.TransformClient;
import org.hyland.contentlake.config.HxprProperties;
import org.hyland.alfresco.contentlake.config.TransformProperties;
import org.hyland.alfresco.contentlake.service.ContentLakeScopeResolver;
import org.hyland.contentlake.service.Chunker;
import org.hyland.contentlake.service.EmbeddingService;
import org.hyland.contentlake.service.NodeSyncService;
import org.hyland.contentlake.service.chunking.*;
import org.hyland.contentlake.service.chunking.strategy.ChunkingStrategy.ChunkingConfig;
import org.springframework.ai.embedding.EmbeddingModel;
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
        HxprProperties.class,
        TransformProperties.class
})
public class AppConfig {

    public static final String HXCS_REPOSITORY = "HXCS-REPOSITORY";

    // ----------------------------------------------------------------------
    // HXPR (Content Lake) wiring
    // ----------------------------------------------------------------------

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
    public HxprService hxprService(HxprDocumentApi documentApi,
                                   HxprQueryApi queryApi,
                                   RestClient hxprRestClient) {
        return new HxprService(documentApi, queryApi, hxprRestClient);
    }

    // ----------------------------------------------------------------------
    // Transform Service
    // ----------------------------------------------------------------------

    @Bean
    public TransformClient transformClient(TransformProperties props) {
        return new TransformClient(props.getUrl(), props.getTimeoutMs());
    }

    // ----------------------------------------------------------------------
    // Embedding pipeline
    // ----------------------------------------------------------------------

    @Bean
    public Chunker chunker(IngestionProperties props) {
        return new Chunker(
                props.getEmbedding().getChunkSize(),
                props.getEmbedding().getChunkOverlap()
        );
    }

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
    public ChunkingConfig chunkingConfig(IngestionProperties props) {
        IngestionProperties.Embedding emb = props.getEmbedding();
        return new ChunkingConfig(
                emb.getMinChunkSize(),
                emb.getChunkSize(),
                emb.getChunkOverlap(),
                emb.getSimilarityThreshold()
        );
    }

    @Bean
    public SimpleChunkingService chunkingService(
            NoiseReductionService noiseReduction,
            ChunkingConfig config) {
        return new SimpleChunkingService(noiseReduction, config);
    }

    @Bean
    public ContentLakeScopeResolver contentLakeScopeResolver(IngestionProperties props,
                                                              org.hyland.alfresco.contentlake.client.AlfrescoClient alfrescoClient) {
        return new ContentLakeScopeResolver(
                props.getExclude().getPaths(),
                props.getExclude().getAspects(),
                alfrescoClient
        );
    }

    @Bean
    public NodeSyncService nodeSyncService(
            AlfrescoClient alfrescoClient,
            HxprDocumentApi documentApi,
            HxprService hxprService,
            TransformClient transformClient,
            EmbeddingService embeddingService,
            SimpleChunkingService chunkingService,
            HxprProperties props
    ) {
        return new NodeSyncService(
                alfrescoClient,    // ContentSourceClient
                documentApi,
                hxprService,
                transformClient,   // TextExtractor
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
}
