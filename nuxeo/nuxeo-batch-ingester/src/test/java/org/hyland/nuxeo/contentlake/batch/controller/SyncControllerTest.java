package org.hyland.nuxeo.contentlake.batch.controller;

import org.hyland.nuxeo.contentlake.batch.security.NuxeoTokenAuthenticationProvider;
import org.hyland.nuxeo.contentlake.config.NuxeoProperties;
import org.hyland.nuxeo.contentlake.batch.config.NuxeoBatchSecurityConfig;
import org.hyland.nuxeo.contentlake.batch.model.IngestionJob;
import org.hyland.nuxeo.contentlake.batch.service.NuxeoBatchIngestionService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors;
import org.springframework.test.web.servlet.MockMvc;

import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(SyncController.class)
@Import({NuxeoBatchSecurityConfig.class, SyncControllerTest.TestConfig.class})
class SyncControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private NuxeoBatchIngestionService batchIngestionService;

    @MockBean
    private NuxeoTokenAuthenticationProvider nuxeoTokenAuthenticationProvider;

    @Test
    void startConfiguredSync_requiresConfiguredBasicCredentials() throws Exception {
        when(batchIngestionService.startConfiguredSync()).thenReturn(new IngestionJob("job-123"));

        mockMvc.perform(post("/api/sync/configured")
                        .with(SecurityMockMvcRequestPostProcessors.httpBasic("Administrator", "Administrator")))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.jobId").value("job-123"))
                .andExpect(jsonPath("$.sourceType").value("nuxeo"));
    }

    @Test
    void startConfiguredSync_acceptsNuxeoUiTokenHeader() throws Exception {
        when(batchIngestionService.startConfiguredSync()).thenReturn(new IngestionJob("job-456"));
        when(nuxeoTokenAuthenticationProvider.supports(any())).thenReturn(true);
        when(nuxeoTokenAuthenticationProvider.authenticate(any())).thenReturn(new UsernamePasswordAuthenticationToken(
                "ui-user",
                null,
                List.of(new SimpleGrantedAuthority("ROLE_USER"))
        ));

        mockMvc.perform(post("/api/sync/configured")
                        .header("X-Authentication-Token", "ui-token"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.jobId").value("job-456"))
                .andExpect(jsonPath("$.sourceType").value("nuxeo"));
    }

    @Test
    void unauthorizedApiRequest_doesNotTriggerBrowserBasicPopup() throws Exception {
        mockMvc.perform(post("/api/sync/configured"))
                .andExpect(status().isUnauthorized())
                .andExpect(header().doesNotExist("WWW-Authenticate"));
    }

    @TestConfiguration
    static class TestConfig {

        @Bean
        NuxeoProperties nuxeoProperties() {
            NuxeoProperties props = new NuxeoProperties();
            props.setBaseUrl("http://127.0.0.1:65535/nuxeo");
            props.setUsername("Administrator");
            props.setPassword("Administrator");
            return props;
        }
    }
}
