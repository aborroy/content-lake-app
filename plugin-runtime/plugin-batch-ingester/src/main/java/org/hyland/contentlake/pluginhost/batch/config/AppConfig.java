package org.hyland.contentlake.pluginhost.batch.config;

import lombok.extern.slf4j.Slf4j;
import org.hyland.contentlake.pluginhost.batch.service.ConnectorBatchIngestionService;
import org.hyland.contentlake.pluginhost.batch.service.ConnectorDiscoveryService;
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
import org.hyland.contentlake.service.FileRootSelectionStore;
import org.hyland.contentlake.service.FileSyncCursorStore;
import org.hyland.contentlake.service.HxprRootSelectionStore;
import org.hyland.contentlake.service.HxprSyncCursorStore;
import org.hyland.contentlake.service.InMemoryRootSelectionStore;
import org.hyland.contentlake.service.InMemorySyncCursorStore;
import org.hyland.contentlake.service.IndexReconciliationService;
import org.hyland.contentlake.service.NodeSyncService;
import org.hyland.contentlake.service.RootSelectionStore;
import org.hyland.contentlake.service.SyncCursorStore;
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

import java.nio.file.Path;
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
     * Where the roots an operator chose are kept.
     *
     * <p>{@code NONE} by default, which is what keeps this feature additive: with no store the precedence chain
     * is the one that existed before it, so no deployment changes behaviour by upgrading.</p>
     */
    @Bean
    public RootSelectionStore rootSelectionStore(HxprService hxprService,
                                                 HxprDocumentApi documentApi,
                                                 ConnectorBatchProperties props) {
        ConnectorBatchProperties.Selection selection = props.getSelection();
        RootSelectionStore store = switch (selection.getStore()) {
            case NONE -> null;
            case HXPR -> new HxprRootSelectionStore(hxprService, documentApi, selection.getHxprPath());
            case FILE -> new FileRootSelectionStore(Path.of(selection.getFile()));
            case MEMORY -> new InMemoryRootSelectionStore();
        };
        if (store != null) {
            log.info("Root selections for this connector are kept in {}, and can be changed through "
                    + "/api/selection without a restart", selection.getStore());
        }
        return store;
    }

    /**
     * Discovery, with the roots resolved once per pass rather than here.
     *
     * <p>They used to be resolved at startup, which made a connector that names none and is configured with
     * none fail the container. Two things made that untenable. An operator cannot change a scope that was read
     * once into an immutable list, and a connector that resolves its own roots over the network would put that
     * call on the startup path, turning a transient source outage into a boot loop rather than a failed job.</p>
     *
     * <p>The startup check is kept where it still applies. With no selection store there is nothing that could
     * supply roots later, so a deployment that can name none still fails here, exactly as before. With a store
     * configured, an absent selection is a state an operator is expected to resolve through the API, so it
     * warns and starts, and a pass with no roots reports itself incomplete rather than authoritative.</p>
     */
    @Bean
    public ConnectorDiscoveryService connectorDiscoveryService(
            SelectedConnector connector,
            ConnectorBatchProperties props,
            ObjectProvider<RootSelectionStore> selectionStores) {

        RootSelectionStore selectionStore = selectionStores.getIfAvailable();
        String qualifiedSourceId = IndexReconciliationService.qualifiedSourceId(connector.client());

        if (selectionStore == null) {
            List<String> roots = ConnectorDiscoveryService.resolveRoots(props.getRoots(), connector.client());
            log.info("Connector '{}' will be walked from {} root(s): {}",
                    connector.sourceType(), roots.size(), roots);
            return new ConnectorDiscoveryService(connector, () -> roots, props);
        }

        logInitialScope(connector, props, selectionStore, qualifiedSourceId);

        return new ConnectorDiscoveryService(connector,
                () -> ConnectorDiscoveryService.resolveRoots(
                        selectionStore.load(qualifiedSourceId), props.getRoots(), connector.client()),
                props);
    }

    /**
     * One line at startup saying what the next pass would walk.
     *
     * <p>Only a log line: it must not fail the container, because with a selection store the operator's next
     * step is to choose roots through the API, and refusing to start would make that impossible.
     */
    private void logInitialScope(SelectedConnector connector,
                                 ConnectorBatchProperties props,
                                 RootSelectionStore selectionStore,
                                 String qualifiedSourceId) {
        try {
            List<String> roots = ConnectorDiscoveryService.resolveRoots(
                    selectionStore.load(qualifiedSourceId), props.getRoots(), connector.client());
            if (roots.isEmpty()) {
                log.warn("Connector '{}' has an empty root selection, so a sync now would index nothing and "
                        + "report itself incomplete. Choose roots with PUT /api/selection.",
                        connector.sourceType());
            } else {
                log.info("Connector '{}' will be walked from {} root(s): {}",
                        connector.sourceType(), roots.size(), roots);
            }
        } catch (RuntimeException e) {
            // Includes the source being unreachable at boot, which is exactly the boot loop this change exists
            // to prevent. Roots are resolved again per pass, so this costs a log line and nothing else.
            log.warn("Could not determine the initial scope for connector '{}': {}. It is resolved again on "
                    + "each pass, so this is not fatal.", connector.sourceType(), e.getMessage());
        }
    }

    /**
     * Where a connector's change-feed position is kept between passes.
     *
     * <p>hxpr by default because this container has no writable mount: both its volumes are read-only, so a
     * file store would silently fail to persist and every pass would walk. hxpr is somewhere the ingester can
     * already write, and the state document is deliberately shaped so no sweep can see it.</p>
     *
     * <p>Built even when {@code connector.change-feed.enabled} is false, so that turning the feature on is a
     * property change rather than a different wiring, and so a misconfigured path fails at startup rather
     * than on the first pass that needed it.</p>
     */
    @Bean
    public SyncCursorStore syncCursorStore(HxprService hxprService,
                                           HxprDocumentApi documentApi,
                                           ConnectorBatchProperties props) {
        ConnectorBatchProperties.Cursor cursor = props.getCursor();
        SyncCursorStore store = switch (cursor.getStore()) {
            case HXPR -> new HxprSyncCursorStore(hxprService, documentApi, cursor.getHxprPath());
            case FILE -> new FileSyncCursorStore(Path.of(cursor.getFile()));
            case MEMORY -> new InMemorySyncCursorStore();
        };
        if (props.getChangeFeed().isEnabled()) {
            log.info("Change-feed cursors for this connector are kept in {}", cursor.getStore());
        }
        return store;
    }

    @Bean
    public ConnectorBatchIngestionService connectorBatchIngestionService(
            ConnectorDiscoveryService discoveryService,
            NodeSyncService nodeSyncService,
            Executor connectorBatchIngestionExecutor,
            IndexReconciliationService reconciliationService,
            SyncCursorStore syncCursorStore,
            SelectedConnector connector,
            ConnectorBatchProperties props) {
        return new ConnectorBatchIngestionService(discoveryService, nodeSyncService,
                connectorBatchIngestionExecutor, reconciliationService, syncCursorStore, connector, props);
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
