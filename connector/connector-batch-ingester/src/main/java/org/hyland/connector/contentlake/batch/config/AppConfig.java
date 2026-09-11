package org.hyland.connector.contentlake.batch.config;

import lombok.extern.slf4j.Slf4j;
import org.hyland.connector.contentlake.batch.service.ConnectorBatchIngestionService;
import org.hyland.connector.contentlake.batch.service.ConnectorDiscoveryService;
import org.hyland.contentlake.client.HxprDocumentApi;
import org.hyland.contentlake.client.HxprQueryApi;
import org.hyland.contentlake.client.HxprService;
import org.hyland.contentlake.config.HxprProperties;
import org.hyland.contentlake.connector.ConnectorPluginConfiguration;
import org.hyland.contentlake.connector.ConnectorRegistry;
import org.hyland.contentlake.connector.ConnectorSchemaController;
import org.hyland.contentlake.extractor.ExtractionBackend;
import org.hyland.contentlake.extractor.ExtractionChain;
import org.hyland.contentlake.extractor.ExtractionFormat;
import org.hyland.contentlake.extractor.TikaTextExtractor;
import org.hyland.contentlake.service.EmbeddingService;
import org.hyland.contentlake.service.EmbeddingTypeResolver;
import org.hyland.contentlake.service.IndexReconciliationService;
import org.hyland.contentlake.service.NodeSyncService;
import org.hyland.contentlake.service.chunking.NoiseReductionService;
import org.hyland.contentlake.service.chunking.SimpleChunkingService;
import org.hyland.contentlake.service.chunking.strategy.ChunkingStrategy.ChunkingConfig;
import org.hyland.contentlake.spi.ContentSourceClient;
import org.springframework.ai.embedding.EmbeddingModel;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.ClientHttpRequestInterceptor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.support.RestClientAdapter;
import org.springframework.web.service.invoker.HttpServiceProxyFactory;

import java.util.List;
import java.util.concurrent.Executor;

/**
 * Wiring for the plugin-connector batch ingester (#132).
 *
 * <p>The same pipeline every batch ingester builds, with one difference: the client, the scope rules and
 * possibly the extractor come out of {@link ConnectorRegistry} instead of being named here. Nothing that
 * came from a plugin is registered as a bean of its SPI type; see {@link SelectedConnector} for why that
 * would be both ambiguous and, for the client specifically, a dependency cycle.</p>
 *
 * <p>Discovery and ingestion are {@code @Bean} methods rather than {@code @Service} classes for the same
 * reason: they are constructed from the selected connector, which does not exist until the registry has
 * been read.</p>
 */
@Slf4j
@Configuration
@EnableConfigurationProperties({
        HxprProperties.class,
        ConnectorBatchProperties.class
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
                                   RestClient hxprRestClient, ConnectorBatchProperties props) {
        return new HxprService(documentApi, queryApi, hxprRestClient,
                EmbeddingTypeResolver.toEmbeddingType(props.getEmbedding().getModelName()));
    }

    /**
     * The connector this ingester runs, resolved from the registry at startup.
     *
     * <p>Fails startup when there is none, or several with no choice made. That is deliberate: this service
     * has no source of its own, so an empty registry is a misconfigured deployment rather than an idle one,
     * and a container that restarts saying so is easier to diagnose than one that reports zero documents.</p>
     *
     * <p>The extraction chain is passed as a supplier so it is built only when the connector brings no
     * extractor of its own. A connector that converts content at the source (the way Nuxeo's conversion
     * service does) then never constructs a chain it would not call.</p>
     */
    @Bean
    public SelectedConnector selectedConnector(
            ConnectorRegistry registry,
            ConnectorBatchProperties props,
            @Value("${content-lake.connector.plugin-directory:"
                    + ConnectorPluginConfiguration.DEFAULT_PLUGIN_DIRECTORY + "}") String pluginDirectory,
            @Value("${extraction.engine-urls:}") String engineUrls,
            @Value("${extraction.engine-timeout-ms:300000}") long engineTimeoutMs,
            @Value("${extraction.format:plaintext}") String extractionFormat,
            ObjectProvider<ExtractionBackend> backendProvider) {

        SelectedConnector selected = SelectedConnector.from(
                registry,
                props.getSourceType(),
                pluginDirectory,
                () -> ExtractionChain.of(engineUrls, engineTimeoutMs,
                        ExtractionFormat.parse(extractionFormat),
                        backendProvider.orderedStream().toList(),
                        new TikaTextExtractor()));

        log.info("Ingesting with connector '{}' ({}) from {}; extraction: {}",
                selected.sourceType(), selected.displayName(), selected.origin(),
                selected.ownExtractor() ? "the connector's own extractor" : "the host extraction chain");
        return selected;
    }

    @Bean
    public NodeSyncService nodeSyncService(SelectedConnector connector,
                                           HxprDocumentApi documentApi,
                                           HxprService hxprService,
                                           EmbeddingService embeddingService,
                                           SimpleChunkingService chunkingService,
                                           HxprProperties props,
                                           @Value("${content-lake.ingest.keyword-context-enrichment-enabled:false}")
                                           boolean keywordContextEnrichmentEnabled,
                                           @Value("${content-lake.ingest.content-reuse-enabled:true}")
                                           boolean contentReuseEnabled) {
        return new NodeSyncService(
                connector.client(),
                documentApi,
                hxprService,
                connector.textExtractor(),
                embeddingService,
                chunkingService,
                props.getTargetPath(),
                props.getPathRepositoryId(),
                keywordContextEnrichmentEnabled,
                contentReuseEnabled);
    }

    @Bean
    public IndexReconciliationService indexReconciliationService(HxprService hxprService,
                                                                 NodeSyncService nodeSyncService,
                                                                 SelectedConnector connector) {
        return new IndexReconciliationService(hxprService, nodeSyncService, connector.client());
    }

    /**
     * Discovery, with the roots resolved here so a connector that names none and is configured with none
     * fails the container rather than reporting an empty source on every run.
     */
    @Bean
    public ConnectorDiscoveryService connectorDiscoveryService(SelectedConnector connector,
                                                              ConnectorBatchProperties props) {
        List<String> roots = ConnectorDiscoveryService.resolveRoots(props.getRoots(), connector.client());
        log.info("Connector '{}' will be walked from {} root(s): {}",
                connector.sourceType(), roots.size(), roots);
        return new ConnectorDiscoveryService(connector, roots, props);
    }

    @Bean
    public ConnectorBatchIngestionService connectorBatchIngestionService(
            ConnectorDiscoveryService discoveryService,
            NodeSyncService nodeSyncService,
            Executor connectorBatchIngestionExecutor,
            IndexReconciliationService reconciliationService,
            SelectedConnector connector,
            ConnectorBatchProperties props) {
        return new ConnectorBatchIngestionService(discoveryService, nodeSyncService,
                connectorBatchIngestionExecutor, reconciliationService, connector, props);
    }

    @Bean
    public EmbeddingService embeddingService(EmbeddingModel embeddingModel, ConnectorBatchProperties props) {
        return new EmbeddingService(embeddingModel, props.getEmbedding().getModelName());
    }

    @Bean
    public NoiseReductionService noiseReductionService(ConnectorBatchProperties props) {
        ConnectorBatchProperties.NoiseReduction cfg = props.getEmbedding().getNoiseReduction();
        return new NoiseReductionService(cfg.isEnabled(), cfg.isAggressive());
    }

    @Bean
    public ChunkingConfig chunkingConfig(ConnectorBatchProperties props) {
        ConnectorBatchProperties.Embedding embedding = props.getEmbedding();
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

    @Bean(name = "connectorBatchIngestionExecutor")
    public Executor connectorBatchIngestionExecutor(ConnectorBatchProperties props) {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(props.getExecutor().getCoreSize());
        executor.setMaxPoolSize(props.getExecutor().getMaxSize());
        executor.setQueueCapacity(props.getExecutor().getQueueCapacity());
        executor.setThreadNamePrefix("connector-batch-");
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
