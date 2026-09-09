package org.hyland.nuxeo.contentlake.batch.config;

import org.hyland.contentlake.client.HxprDocumentApi;
import org.hyland.contentlake.client.HxprQueryApi;
import org.hyland.contentlake.client.HxprService;
import org.hyland.nuxeo.contentlake.client.NuxeoClient;
import org.springframework.beans.factory.ObjectProvider;
import org.hyland.contentlake.extractor.ExtractionBackend;
import org.hyland.contentlake.extractor.ExtractionChain;
import org.hyland.contentlake.extractor.ExtractionFormat;
import org.hyland.contentlake.extractor.TikaTextExtractor;
import org.hyland.contentlake.extractor.TransformEngineTextExtractor;
import org.hyland.contentlake.spi.TextExtractor;
import org.hyland.nuxeo.contentlake.client.NuxeoConversionClient;
import org.hyland.contentlake.config.HxprProperties;
import org.hyland.nuxeo.contentlake.config.NuxeoProperties;
import org.hyland.nuxeo.contentlake.batch.service.NuxeoDiscoveryService;
import org.hyland.contentlake.service.EmbeddingService;
import org.hyland.contentlake.service.EmbeddingTypeResolver;
import org.hyland.nuxeo.contentlake.service.NuxeoScopeResolver;
import org.hyland.contentlake.service.IndexReconciliationService;
import org.hyland.contentlake.service.NodeSyncService;
import org.hyland.contentlake.service.chunking.NoiseReductionService;
import org.hyland.contentlake.service.chunking.SimpleChunkingService;
import org.hyland.contentlake.service.chunking.strategy.ChunkingStrategy.ChunkingConfig;
import org.springframework.ai.embedding.EmbeddingModel;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.ClientHttpRequestInterceptor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.support.RestClientAdapter;
import org.springframework.web.service.invoker.HttpServiceProxyFactory;

import java.util.ArrayList;
import java.util.List;
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

    @Bean
    public HxprService hxprService(HxprDocumentApi documentApi,
                                   HxprQueryApi queryApi,
                                   RestClient hxprRestClient,
                                   NuxeoBatchProperties props) {
        return new HxprService(documentApi, queryApi, hxprRestClient,
                EmbeddingTypeResolver.toEmbeddingType(props.getEmbedding().getModelName()));
    }

    @Bean
    public NuxeoClient nuxeoClient(NuxeoProperties props) {
        return new NuxeoClient(props);
    }

    @Bean
    public NuxeoConversionClient nuxeoConversionClient(NuxeoProperties props) {
        return new NuxeoConversionClient(props);
    }

    /**
     * Extraction chain, ordered most structural first.
     *
     * <p>A transform engine is included only when {@code extraction.engine-url} is set, so a
     * deployment without one behaves exactly as it did before. Configure it to gain structure-aware
     * extraction here: the engine speaks the transform protocol directly rather than through a
     * repository, so it is not tied to Alfresco.</p>
     *
     * <p>{@code extraction.format} decides whether the engine is asked for markdown, and defaults to
     * {@code plaintext}. Markdown is requested only where the engine advertises it, and extraction
     * always degrades to the next entry rather than failing an ingest.</p>
     */
    @Bean
    public TextExtractor textExtractor(
            NuxeoConversionClient nuxeoConversionClient,
            @org.springframework.beans.factory.annotation.Value("${extraction.engine-urls:}")
            String engineUrls,
            @org.springframework.beans.factory.annotation.Value("${extraction.engine-timeout-ms:300000}")
            long engineTimeoutMs,
            @org.springframework.beans.factory.annotation.Value("${extraction.format:plaintext}")
            String extractionFormat,
            ObjectProvider<ExtractionBackend> backendProvider
    ) {
        // Nuxeo's own ConversionService converts from the blob identity, with no temp download, so it
        // sits after the engines but before Tika.
        return ExtractionChain.of(engineUrls, engineTimeoutMs, ExtractionFormat.parse(extractionFormat),
                backendProvider.orderedStream().toList(), nuxeoConversionClient, new TikaTextExtractor());
    }

    @Bean
    public NuxeoScopeResolver nuxeoScopeResolver(NuxeoProperties props, NuxeoClient nuxeoClient) {
        return new NuxeoScopeResolver(
                props.getScope().getIncludedRoots(),
                props.getScope().getIncludedTypes(),
                props.getScope().getExcludedLifecycleStates(),
                nuxeoClient
        );
    }

    @Bean
    public IndexReconciliationService indexReconciliationService(HxprService hxprService,
                                                                NodeSyncService nodeSyncService,
                                                                NuxeoClient nuxeoClient) {
        return new IndexReconciliationService(hxprService, nodeSyncService, nuxeoClient);
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
                                           TextExtractor textExtractor,
                                           EmbeddingService embeddingService,
                                           SimpleChunkingService chunkingService,
                                           HxprProperties props,
                                           @org.springframework.beans.factory.annotation.Value(
                                                   "${content-lake.ingest.keyword-context-enrichment-enabled:false}")
                                           boolean keywordContextEnrichmentEnabled) {
        return new NodeSyncService(
                nuxeoClient,
                documentApi,
                hxprService,
                textExtractor,
                embeddingService,
                chunkingService,
                props.getTargetPath(),
                props.getPathRepositoryId(),
                keywordContextEnrichmentEnabled
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

    private static ClientHttpRequestInterceptor hxprAuthInterceptor(HxprProperties props) {
        return (request, body, execution) -> {
            request.getHeaders().setBasicAuth(props.getUsername(), props.getPassword());
            request.getHeaders().set(HXCS_REPOSITORY, props.getRepositoryId());
            return execution.execute(request, body);
        };
    }

    private static HttpServiceProxyFactory httpProxyFactory(RestClient restClient) {
        return HttpServiceProxyFactory.builderFor(RestClientAdapter.create(restClient)).build();
    }
}
