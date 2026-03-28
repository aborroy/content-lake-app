package org.hyland.nuxeo.contentlake.live.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.hyland.contentlake.client.HxprDocumentApi;
import org.hyland.contentlake.client.HxprQueryApi;
import org.hyland.contentlake.client.HxprService;
import org.hyland.contentlake.client.HxprTokenProvider;
import org.hyland.nuxeo.contentlake.client.NuxeoClient;
import org.hyland.nuxeo.contentlake.client.NuxeoConversionClient;
import org.hyland.contentlake.config.HxprProperties;
import org.hyland.nuxeo.contentlake.config.NuxeoProperties;
import org.hyland.nuxeo.contentlake.live.client.NuxeoAuditClient;
import org.hyland.nuxeo.contentlake.live.service.AuditCursorStore;
import org.hyland.nuxeo.contentlake.live.service.FileAuditCursorStore;
import org.hyland.contentlake.service.EmbeddingService;
import org.hyland.nuxeo.contentlake.service.NuxeoScopeResolver;
import org.hyland.contentlake.service.NodeSyncService;
import org.hyland.contentlake.service.chunking.NoiseReductionService;
import org.hyland.contentlake.service.chunking.SimpleChunkingService;
import org.hyland.contentlake.service.chunking.strategy.ChunkingStrategy.ChunkingConfig;
import org.springframework.ai.embedding.EmbeddingModel;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.ClientHttpRequestInterceptor;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.support.RestClientAdapter;
import org.springframework.web.service.invoker.HttpServiceProxyFactory;

import java.nio.file.Path;
import java.time.Clock;

@Configuration
@EnableConfigurationProperties({
        HxprProperties.class,
        NuxeoProperties.class,
        NuxeoLiveProperties.class
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
    public NuxeoConversionClient nuxeoConversionClient(NuxeoProperties props) {
        return new NuxeoConversionClient(props);
    }

    @Bean
    public NuxeoAuditClient nuxeoAuditClient(NuxeoProperties props) {
        return new NuxeoAuditClient(props);
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
    public Clock clock() {
        return Clock.systemUTC();
    }

    @Bean
    public AuditCursorStore auditCursorStore(NuxeoLiveProperties props,
                                             ObjectMapper objectMapper) {
        return new FileAuditCursorStore(
                Path.of(props.getAudit().getCursorFile()),
                objectMapper
        );
    }

    @Bean(name = "nuxeoLiveRuntime")
    public NuxeoLiveProperties nuxeoLiveRuntime(NuxeoLiveProperties props) {
        return props;
    }

    @Bean
    public EmbeddingService embeddingService(EmbeddingModel embeddingModel, NuxeoLiveProperties props) {
        return new EmbeddingService(embeddingModel, props.getEmbedding().getModelName());
    }

    @Bean
    public NoiseReductionService noiseReductionService(NuxeoLiveProperties props) {
        NuxeoLiveProperties.NoiseReduction cfg = props.getEmbedding().getNoiseReduction();
        return new NoiseReductionService(cfg.isEnabled(), cfg.isAggressive());
    }

    @Bean
    public ChunkingConfig chunkingConfig(NuxeoLiveProperties props) {
        NuxeoLiveProperties.Embedding embedding = props.getEmbedding();
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
                                           NuxeoConversionClient nuxeoConversionClient,
                                           EmbeddingService embeddingService,
                                           SimpleChunkingService chunkingService,
                                           HxprProperties props) {
        return new NodeSyncService(
                nuxeoClient,
                documentApi,
                hxprService,
                nuxeoConversionClient,
                embeddingService,
                chunkingService,
                props.getTargetPath(),
                props.getPathRepositoryId()
        );
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
