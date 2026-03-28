package org.hyland.contentlake.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * HXPR (Content Lake) connection and authentication properties.
 *
 * <p>Bound from {@code hxpr.*} in {@code application.yml}.
 * Shared by both batch-ingester and live-ingester via
 * {@code @EnableConfigurationProperties(HxprProperties.class)}.</p>
 */
@Data
@ConfigurationProperties(prefix = "hxpr")
public class HxprProperties {

    private String url = "http://localhost:8080";
    private String repositoryId = "default";

    /**
     * Base target path in HXPR where Alfresco structures are created.
     *
     * <p>The full hierarchy is built as:
     * {@code {targetPath}/{pathRepositoryId or alfrescoRepositoryId}/{alfrescoPath}}.</p>
     */
    private String targetPath = "/alfresco-sync";

    /**
     * Optional repository-id override used only for path prefixing in Content Lake.
     *
     * <p>When empty, the ingester falls back to the Alfresco Discovery repository id.
     * Set this when HXPR permissions require a fixed writable path prefix.</p>
     */
    private String pathRepositoryId;

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
