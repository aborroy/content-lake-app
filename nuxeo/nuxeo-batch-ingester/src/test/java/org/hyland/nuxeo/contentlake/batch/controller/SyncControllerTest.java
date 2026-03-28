package org.hyland.nuxeo.contentlake.batch.controller;

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
import org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors;
import org.springframework.test.web.servlet.MockMvc;

import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(SyncController.class)
@Import({NuxeoBatchSecurityConfig.class, SyncControllerTest.TestConfig.class})
class SyncControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private NuxeoBatchIngestionService batchIngestionService;

    @Test
    void startConfiguredSync_requiresConfiguredBasicCredentials() throws Exception {
        when(batchIngestionService.startConfiguredSync()).thenReturn(new IngestionJob("job-123"));

        mockMvc.perform(post("/api/sync/configured")
                        .with(SecurityMockMvcRequestPostProcessors.httpBasic("Administrator", "Administrator")))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.jobId").value("job-123"))
                .andExpect(jsonPath("$.sourceType").value("nuxeo"));
    }

    @TestConfiguration
    static class TestConfig {

        @Bean
        NuxeoProperties nuxeoProperties() {
            NuxeoProperties props = new NuxeoProperties();
            props.setUsername("Administrator");
            props.setPassword("Administrator");
            return props;
        }
    }
}
