package org.hyland.contentlake.rag.config;

import org.hyland.contentlake.rag.security.AlfrescoDirectory;
import org.hyland.contentlake.rag.security.MultiSourceAuthenticationProvider;
import org.hyland.contentlake.rag.security.NuxeoDirectory;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Only the RAG health check and the container probes are public.
 *
 * <p>A permitted path that has no handler in the web slice answers 404, which is the signal that it
 * passed authorization. A blocked path answers 401 before routing.</p>
 */
@WebMvcTest
@Import({RagSecurityConfig.class, RagSecurityConfigDefaultDenyTest.TestConfig.class})
class RagSecurityConfigDefaultDenyTest {

    @Autowired
    private MockMvc mockMvc;

    @MockitoBean
    private MultiSourceAuthenticationProvider multiSourceAuthenticationProvider;

    /**
     * The filter chain builds {@code DualSourceAuthenticationFilter} from these two directly, so without
     * them this slice fails on bean creation rather than on any assertion below.
     */
    @MockitoBean
    private AlfrescoDirectory alfrescoDirectory;

    @MockitoBean
    private NuxeoDirectory nuxeoDirectory;

    @Test
    void sensitiveActuatorEndpoint_requiresAuthentication() throws Exception {
        mockMvc.perform(get("/actuator/metrics"))
                .andExpect(status().isUnauthorized());
    }

    @Test
    void mcpEndpoint_requiresAuthentication() throws Exception {
        mockMvc.perform(get("/mcp"))
                .andExpect(status().isUnauthorized());
    }

    @Test
    void unmappedPath_isDeniedRatherThanPermitted() throws Exception {
        mockMvc.perform(get("/some/unmapped/path"))
                .andExpect(status().isUnauthorized());
    }

    @Test
    void healthProbe_passesTheChainWithoutCredentials() throws Exception {
        mockMvc.perform(get("/actuator/health"))
                .andExpect(status().isNotFound());
    }

    @Test
    void infoProbe_passesTheChainWithoutCredentials() throws Exception {
        mockMvc.perform(get("/actuator/info"))
                .andExpect(status().isNotFound());
    }

    @Test
    void ragHealthCheck_passesTheChainWithoutCredentials() throws Exception {
        mockMvc.perform(get("/api/rag/health"))
                .andExpect(status().isNotFound());
    }

    @TestConfiguration
    static class TestConfig {

        @Bean
        RagProperties ragProperties() {
            return new RagProperties();
        }
    }

    @SpringBootConfiguration
    @EnableAutoConfiguration
    static class TestApplication {
    }
}
