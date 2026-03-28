package org.hyland.contentlake.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Alfresco Transform Service connection properties.
 *
 * <p>Bound from {@code transform.*} in {@code application.yml}.
 * Shared by both batch-ingester and live-ingester via
 * {@code @EnableConfigurationProperties(TransformProperties.class)}.</p>
 */
@Data
@ConfigurationProperties(prefix = "transform")
public class TransformProperties {

    private String url = "http://localhost:10090";
    private long timeoutMs = 300000;
    private boolean enabled = true;
}
