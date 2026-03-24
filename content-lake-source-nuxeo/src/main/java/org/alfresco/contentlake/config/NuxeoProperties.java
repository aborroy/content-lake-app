package org.alfresco.contentlake.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.ArrayList;
import java.util.List;

/**
 * Nuxeo connection and scope properties.
 *
 * <p>This module is adapter-only for now, so these properties are not yet
 * enabled by an application module. They are still defined here so the
 * upcoming Nuxeo ingester can bind them directly.</p>
 */
@Data
@ConfigurationProperties(prefix = "nuxeo")
public class NuxeoProperties {

    private String baseUrl = "http://localhost:8081/nuxeo";
    private String username = "Administrator";
    private String password = "Administrator";
    private String sourceId = "local";
    private String blobXpath = "file:content";
    private Scope scope = new Scope();

    @Data
    public static class Scope {
        private List<String> includedRoots = new ArrayList<>(List.of("/default-domain/workspaces"));
        private List<String> includedTypes = new ArrayList<>(List.of("File", "Note"));
    }
}
