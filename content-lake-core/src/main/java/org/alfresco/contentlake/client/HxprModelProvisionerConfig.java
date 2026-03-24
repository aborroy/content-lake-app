package org.alfresco.contentlake.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ResourceLoader;
import org.springframework.web.client.RestClient;

/**
 * Wires HXPR model provisioning support.
 *
 * <p>The provisioner is used by application modules (batch-ingester, etc.)
 * to ensure required HXPR model fragments exist at bootstrap.
 */
@Configuration
public class HxprModelProvisionerConfig {

    @Bean
    public HxprModelProvisioner hxprModelProvisioner(RestClient hxprRestClient,
                                                     ResourceLoader resourceLoader,
                                                     ObjectMapper objectMapper) {
        return new HxprModelProvisioner(hxprRestClient, resourceLoader, objectMapper);
    }
}
