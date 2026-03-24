package org.alfresco.contentlake.rag.config;

import lombok.Data;
import org.alfresco.contentlake.client.HxprDocumentApi;
import org.alfresco.contentlake.client.HxprQueryApi;
import org.alfresco.contentlake.client.HxprService;
import org.alfresco.contentlake.client.HxprTokenProvider;
import org.alfresco.contentlake.rag.conversation.ConversationMemoryStore;
import org.alfresco.contentlake.rag.conversation.InMemoryConversationMemoryStore;
import org.alfresco.contentlake.service.EmbeddingService;
import org.springframework.ai.embedding.EmbeddingModel;
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
    // Embedding service
    // ----------------------------------------------------------------------

    @Bean
    public EmbeddingService embeddingService(EmbeddingModel embeddingModel) {
        return new EmbeddingService(embeddingModel,
                embeddingModel.getClass().getSimpleName());
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

    @Data
    @ConfigurationProperties(prefix = "hxpr")
    public static class HxprProperties {
        private String url = "http://localhost:8080";
        private String repositoryId = "default";
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
}
