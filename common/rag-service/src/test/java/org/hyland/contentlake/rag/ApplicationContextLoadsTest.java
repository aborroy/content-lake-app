package org.hyland.contentlake.rag;

import org.hyland.contentlake.rag.config.RagProperties;
import org.hyland.contentlake.rag.observability.RagObservations;
import org.hyland.contentlake.rag.observability.RetrievalFeatureSet;
import org.hyland.contentlake.rag.security.MultiSourceAuthenticationProvider;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Smoke test: the full application context loads with the MCP server enabled.
 *
 * <p>Guards against bean-wiring regressions that unit tests miss - in particular the startup circular
 * dependency where exposing the MCP tools as a {@code ToolCallbackProvider} (collected by Spring AI's
 * tool-calling autoconfiguration to build the {@code ChatModel}) pulled in {@code SemanticSearchService}
 * -> query expansion -> the same {@code ChatModel}. Bean creation does not call any external service,
 * so the context loads offline with dummy endpoints.</p>
 *
 * <p>Also the one place the observability defaults are asserted against a real context. That matters
 * because this context has a real {@code ObservationRegistry} (actuator is on the classpath), which is
 * exactly the condition under which the spans exist whether or not anything is exported.</p>
 */
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.NONE,
        properties = {
                "spring.ai.openai.base-url=http://localhost:0/v1",
                "spring.ai.openai.api-key=test",
                "spring.ai.openai.chat.options.model=test-chat",
                "spring.ai.openai.embedding.model=test-embed",
                "rag.mcp.enabled=true",
                "rag.agentic-tools.enabled=true"
        })
class ApplicationContextLoadsTest {

    @Autowired
    RagProperties ragProperties;

    @Autowired
    RagObservations ragObservations;

    @Autowired
    RetrievalFeatureSet retrievalFeatureSet;

    @Autowired
    MultiSourceAuthenticationProvider callerAuthentication;

    @Test
    void contextLoads() {
        // Success = the ApplicationContext refreshed without an unresolvable circular reference.
    }

    @Test
    void theCallerAuthenticationChainIsRegisteredInOrder() {
        // The only place the runtime order of the chain is protected. It is a security property: it decides
        // which system is shown a caller's credentials first, so an authenticator registered with a lower
        // order silently changes where an existing deployment sends its passwords.
        //
        // Both Alfresco authenticators share one order and both Nuxeo ones share the next, so what this pins
        // is that every Alfresco authority precedes every Nuxeo one. Within a pair the two are mutually
        // exclusive by supports() (a marker-prefixed principal and a password login), so their relative
        // position cannot affect an outcome; it is settled by id only to keep the chain deterministic.
        assertThat(callerAuthentication.authenticatorIds()).containsExactly(
                "alfresco-password", "alfresco-ticket", "nuxeo-password", "nuxeo-token");
    }

    @Test
    void spanPayloadsAndContentCaptureAreBothOffByDefault() {
        assertThat(ragProperties.getObservability().isPayloadsEnabled()).isFalse();
        assertThat(ragProperties.getObservability().isCaptureContent()).isFalse();
        assertThat(ragObservations.payloadsEnabled()).isFalse();
        assertThat(ragObservations.contentCaptureEnabled()).isFalse();
    }

    @Test
    void theRetrievalFeatureSetIsResolvedFromTheRealConfiguration() {
        // A per-process constant, so it must be computable at startup rather than per request.
        assertThat(retrievalFeatureSet.value()).isNotBlank();
    }
}
