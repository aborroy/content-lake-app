package org.hyland.filesystem.contentlake.batch.config;

import org.hyland.contentlake.client.HxprDocumentApi;
import org.hyland.contentlake.client.HxprQueryApi;
import org.hyland.contentlake.client.HxprService;
import org.hyland.contentlake.config.HxprProperties;
import org.hyland.contentlake.connector.ConnectorRegistry;
import org.hyland.contentlake.connector.ConnectorSchemaController;
import org.hyland.contentlake.spi.ContentSourceClient;
import org.springframework.beans.factory.ObjectProvider;
import org.hyland.contentlake.extractor.ExtractionBackend;
import org.hyland.contentlake.extractor.ExtractionChain;
import org.hyland.contentlake.extractor.ExtractionFormat;
import org.hyland.contentlake.extractor.TikaTextExtractor;
import org.hyland.contentlake.extractor.TransformEngineTextExtractor;
import org.hyland.contentlake.service.EmbeddingService;
import org.hyland.contentlake.service.EmbeddingTypeResolver;
import org.hyland.contentlake.service.IndexReconciliationService;
import org.hyland.contentlake.service.NodeSyncService;
import org.hyland.contentlake.service.chunking.NoiseReductionService;
import org.hyland.contentlake.service.chunking.SimpleChunkingService;
import org.hyland.contentlake.service.chunking.strategy.ChunkingStrategy.ChunkingConfig;
import org.hyland.contentlake.spi.TextExtractor;

import java.util.ArrayList;
import java.util.List;
import org.hyland.filesystem.contentlake.client.FileSystemSourceClient;
import org.hyland.filesystem.contentlake.config.FileSystemProperties;
import org.hyland.filesystem.contentlake.service.FileSystemScopeResolver;
import org.springframework.ai.embedding.EmbeddingModel;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
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
        FileSystemProperties.class,
        FilesystemBatchProperties.class
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
    public HxprService hxprService(HxprDocumentApi documentApi, HxprQueryApi queryApi,
                                   RestClient hxprRestClient, FilesystemBatchProperties props) {
        return new HxprService(documentApi, queryApi, hxprRestClient,
                EmbeddingTypeResolver.toEmbeddingType(props.getEmbedding().getModelName()));
    }

    @Bean
    public FileSystemSourceClient fileSystemSourceClient(FileSystemProperties props) {
        return new FileSystemSourceClient(props);
    }

    /**
     * Extraction chain. The filesystem source has no repository transform service behind it, so
     * in-process Tika is the baseline and always the last resort.
     *
     * <p>A transform engine is included only when {@code extraction.engine-url} is set, so a
     * deployment without one behaves exactly as it did before. Configuring one is what gives this
     * source structure-aware extraction at all: the engine speaks the transform protocol directly,
     * so nothing here is tied to Alfresco.</p>
     *
     * <p>{@code extraction.format} decides whether the engine is asked for markdown, and defaults to
     * {@code plaintext}. Markdown is requested only where the engine advertises it, and extraction
     * always degrades to Tika rather than failing an ingest.</p>
     */
    @Bean
    @ConditionalOnMissingBean(TextExtractor.class)
    public TextExtractor textExtractor(
            @Value("${extraction.engine-urls:}") String engineUrls,
            @Value("${extraction.engine-timeout-ms:300000}") long engineTimeoutMs,
            @Value("${extraction.format:plaintext}") String extractionFormat,
            ObjectProvider<ExtractionBackend> backendProvider
    ) {
        return ExtractionChain.of(engineUrls, engineTimeoutMs, ExtractionFormat.parse(extractionFormat),
                backendProvider.orderedStream().toList(), new TikaTextExtractor());
    }

    @Bean
    public FileSystemScopeResolver fileSystemScopeResolver(FileSystemProperties props) {
        return new FileSystemScopeResolver(props);
    }

    @Bean
    public IndexReconciliationService indexReconciliationService(HxprService hxprService,
                                                                NodeSyncService nodeSyncService,
                                                                FileSystemSourceClient sourceClient) {
        return new IndexReconciliationService(hxprService, nodeSyncService, sourceClient);
    }

    @Bean
    public EmbeddingService embeddingService(EmbeddingModel embeddingModel, FilesystemBatchProperties props) {
        return new EmbeddingService(embeddingModel, props.getEmbedding().getModelName());
    }

    @Bean
    public NoiseReductionService noiseReductionService(FilesystemBatchProperties props) {
        FilesystemBatchProperties.NoiseReduction cfg = props.getEmbedding().getNoiseReduction();
        return new NoiseReductionService(cfg.isEnabled(), cfg.isAggressive());
    }

    @Bean
    public ChunkingConfig chunkingConfig(FilesystemBatchProperties props) {
        FilesystemBatchProperties.Embedding embedding = props.getEmbedding();
        return new ChunkingConfig(
                embedding.getMinChunkSize(),
                embedding.getChunkSize(),
                embedding.getChunkOverlap(),
                embedding.getSimilarityThreshold());
    }

    @Bean
    public SimpleChunkingService chunkingService(NoiseReductionService noiseReductionService,
                                                 ChunkingConfig chunkingConfig) {
        return new SimpleChunkingService(noiseReductionService, chunkingConfig);
    }

    @Bean
    public NodeSyncService nodeSyncService(FileSystemSourceClient fileSystemSourceClient,
                                           HxprDocumentApi documentApi,
                                           HxprService hxprService,
                                           TextExtractor textExtractor,
                                           EmbeddingService embeddingService,
                                           SimpleChunkingService chunkingService,
                                           HxprProperties props,
                                           @Value("${content-lake.ingest.keyword-context-enrichment-enabled:false}")
                                           boolean keywordContextEnrichmentEnabled,
                                           @Value("${content-lake.ingest.content-reuse-enabled:true}")
                                           boolean contentReuseEnabled) {
        return new NodeSyncService(
                fileSystemSourceClient,
                documentApi,
                hxprService,
                textExtractor,
                embeddingService,
                chunkingService,
                props.getTargetPath(),
                props.getPathRepositoryId(),
                keywordContextEnrichmentEnabled,
                contentReuseEnabled);
    }

    @Bean(name = "filesystemBatchIngestionExecutor")
    public Executor filesystemBatchIngestionExecutor(FilesystemBatchProperties props) {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(props.getExecutor().getCoreSize());
        executor.setMaxPoolSize(props.getExecutor().getMaxSize());
        executor.setQueueCapacity(props.getExecutor().getQueueCapacity());
        executor.setThreadNamePrefix("filesystem-batch-");
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
