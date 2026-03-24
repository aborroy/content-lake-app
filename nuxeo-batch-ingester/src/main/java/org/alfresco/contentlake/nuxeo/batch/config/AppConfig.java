package org.alfresco.contentlake.nuxeo.batch.config;

import org.alfresco.contentlake.client.HxprDocumentApi;
import org.alfresco.contentlake.client.HxprQueryApi;
import org.alfresco.contentlake.client.HxprService;
import org.alfresco.contentlake.client.HxprTokenProvider;
import org.alfresco.contentlake.client.NuxeoClient;
import org.alfresco.contentlake.config.HxprProperties;
import org.alfresco.contentlake.config.NuxeoProperties;
import org.alfresco.contentlake.nuxeo.batch.service.NuxeoDiscoveryService;
import org.alfresco.contentlake.service.EmbeddingService;
import org.alfresco.contentlake.service.NodeSyncService;
import org.alfresco.contentlake.service.NuxeoTextExtractor;
import org.alfresco.contentlake.service.chunking.NoiseReductionService;
import org.alfresco.contentlake.service.chunking.SimpleChunkingService;
import org.alfresco.contentlake.service.chunking.strategy.ChunkingStrategy.ChunkingConfig;
import org.springframework.ai.embedding.EmbeddingModel;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.ClientHttpRequestInterceptor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.support.RestClientAdapter;
import org.springframework.web.service.invoker.HttpServiceProxyFactory;

import java.util.concurrent.Executor;

@Configuration
@EnableConfigurationProperties({
        HxprProperties.class,
        NuxeoProperties.class,
        NuxeoBatchProperties.class
})
public class AppConfig {

    public static final String HXCS_REPOSITORY = "HXCS-REPOSITORY";

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

    @Bean
    public NuxeoClient nuxeoClient(NuxeoProperties props) {
        return new NuxeoClient(props);
    }

    @Bean
    public NuxeoTextExtractor nuxeoTextExtractor() {
        return new NuxeoTextExtractor();
    }

    @Bean
    public EmbeddingService embeddingService(EmbeddingModel embeddingModel, NuxeoBatchProperties props) {
        return new EmbeddingService(embeddingModel, props.getEmbedding().getModelName());
    }

    @Bean
    public NoiseReductionService noiseReductionService(NuxeoBatchProperties props) {
        NuxeoBatchProperties.NoiseReduction cfg = props.getEmbedding().getNoiseReduction();
        return new NoiseReductionService(cfg.isEnabled(), cfg.isAggressive());
    }

    @Bean
    public ChunkingConfig chunkingConfig(NuxeoBatchProperties props) {
        NuxeoBatchProperties.Embedding embedding = props.getEmbedding();
        return new ChunkingConfig(
                embedding.getMinChunkSize(),
                embedding.getChunkSize(),
                embedding.getChunkOverlap(),
                embedding.getSimilarityThreshold()
        );
    }

    @Bean
    public SimpleChunkingService chunkingService(NoiseReductionService noiseReductionService,
                                                 ChunkingConfig chunkingConfig) {
        return new SimpleChunkingService(noiseReductionService, chunkingConfig);
    }

    @Bean
    public NodeSyncService nodeSyncService(NuxeoClient nuxeoClient,
                                           HxprDocumentApi documentApi,
                                           HxprService hxprService,
                                           NuxeoTextExtractor nuxeoTextExtractor,
                                           EmbeddingService embeddingService,
                                           SimpleChunkingService chunkingService,
                                           HxprProperties props) {
        return new NodeSyncService(
                nuxeoClient,
                documentApi,
                hxprService,
                nuxeoTextExtractor,
                embeddingService,
                chunkingService,
                props.getTargetPath(),
                props.getPathRepositoryId()
        );
    }

    @Bean(name = "nuxeoBatchIngestionExecutor")
    public Executor nuxeoBatchIngestionExecutor(NuxeoBatchProperties props) {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(props.getExecutor().getCoreSize());
        executor.setMaxPoolSize(props.getExecutor().getMaxSize());
        executor.setQueueCapacity(props.getExecutor().getQueueCapacity());
        executor.setThreadNamePrefix("nuxeo-batch-");
        executor.setWaitForTasksToCompleteOnShutdown(true);
        executor.setAwaitTerminationSeconds(props.getExecutor().getAwaitTerminationSeconds());
        executor.initialize();
        return executor;
    }

    private static ClientHttpRequestInterceptor hxprAuthInterceptor(HxprProperties props,
                                                                    HxprTokenProvider tokenProvider) {
        return (request, body, execution) -> {
            request.getHeaders().setBearerAuth(tokenProvider.getToken());
            request.getHeaders().set(HXCS_REPOSITORY, props.getRepositoryId());
            return execution.execute(request, body);
        };
    }

    private static HttpServiceProxyFactory httpProxyFactory(RestClient restClient) {
        return HttpServiceProxyFactory.builderFor(RestClientAdapter.create(restClient)).build();
    }
}
