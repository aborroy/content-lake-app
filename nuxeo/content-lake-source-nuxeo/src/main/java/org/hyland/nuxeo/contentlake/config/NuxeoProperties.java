package org.hyland.nuxeo.contentlake.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.ArrayList;
import java.util.List;

/**
 * Nuxeo connection and scope properties shared by the Nuxeo adapter and ingester.
 */
@Data
@ConfigurationProperties(prefix = "nuxeo")
public class NuxeoProperties {

    private String baseUrl = "http://localhost:8081/nuxeo";
    private String username = "Administrator";
    private String password = "Administrator";
    private String sourceId = "local";
    private String blobXpath = "file:content";
    private Conversion conversion = new Conversion();
    private Scope scope = new Scope();
    private Discovery discovery = new Discovery();

    @Data
    public static class Conversion {
        private long timeoutMs = 300000;
        private boolean enabled = true;
    }

    @Data
    public static class Scope {
        private List<String> includedRoots = new ArrayList<>(List.of("/default-domain/workspaces"));
        private List<String> includedTypes = new ArrayList<>(List.of("File", "Note"));
        private List<String> excludedLifecycleStates = new ArrayList<>(List.of("deleted"));
    }

    @Data
    public static class Discovery {
        private int pageSize = 50;
        private Mode mode = Mode.NXQL;
    }

    public enum Mode {
        NXQL,
        CHILDREN
    }
}
