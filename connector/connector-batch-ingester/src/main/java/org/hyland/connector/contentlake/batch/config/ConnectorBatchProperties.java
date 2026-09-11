package org.hyland.connector.contentlake.batch.config;

import lombok.Data;
import org.hyland.contentlake.service.ReconcileProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.ArrayList;
import java.util.List;

/**
 * Settings for the plugin-connector batch ingester.
 *
 * <p>Note the prefix: {@code connector.*} is this host's own configuration, while
 * {@code content-lake.connector.*} is the loader's (plugin directory, validation mode) and belongs to core.
 * A connector's own settings are neither: they are whatever its {@code ConnectorSchema} declares, read
 * straight from the environment by {@code ConnectorContext}.</p>
 */
@Data
@ConfigurationProperties(prefix = "connector")
public class ConnectorBatchProperties {

    /**
     * Which loaded connector to ingest with, by source type.
     *
     * <p>Optional when exactly one connector is loaded, which is the normal deployment. With several
     * mounted, this is required: picking one would make the ingested corpus depend on jar file names.</p>
     */
    private String sourceType;

    /**
     * Containers discovery starts from, as source-system node ids.
     *
     * <p>Empty falls back to {@code ContentSourceClient.getRootNodeId()}. A connector that answers neither
     * fails startup, because a discovery pass with no entry point would report an empty source rather than a
     * misconfiguration.</p>
     */
    private List<String> roots = new ArrayList<>();

    /** Children fetched per container listing. */
    private int pageSize = 100;

    /**
     * How deep the walk may go below a root before it stops and reports the pass incomplete.
     *
     * <p>A backstop, not a scope control. The walk already refuses to revisit a node id, which handles
     * ordinary multi-filing; this catches a source that hands back a fresh id for the same container on
     * every listing, where the visited set cannot help.</p>
     */
    private int maxDepth = 50;

    private Executor executor = new Executor();
    private Embedding embedding = new Embedding();

    /**
     * Post-discovery reconciliation sweep. Off by default, and more cautiously than elsewhere: the sweep's
     * scope comes from the source paths discovery resolved, and a connector whose nodes report {@code /} as
     * their path would hand it everything under this source's target path.
     */
    private ReconcileProperties reconcile = new ReconcileProperties();

    private Security security = new Security();

    /**
     * Credentials for this ingester's own REST API.
     *
     * <p>Both fields are deliberately without defaults. {@code ConnectorBatchSecurityConfig} fails startup
     * when either is blank, so the service can neither run open nor run unreachable behind a password
     * generated at boot. There is no source repository to authenticate against: a plugin connector's
     * credentials are its own and say nothing about who may trigger a re-ingest.</p>
     */
    @Data
    public static class Security {
        private String username;
        private String password;
    }

    @Data
    public static class Executor {
        private int coreSize = 1;
        private int maxSize = 1;
        private int queueCapacity = 1000;
        private int awaitTerminationSeconds = 30;
    }

    @Data
    public static class Embedding {
        private int minChunkSize = 200;
        private int chunkSize = 1000;
        private int chunkOverlap = 120;
        private double similarityThreshold = 0.75;
        private String modelName = "default";
        private NoiseReduction noiseReduction = new NoiseReduction();
    }

    @Data
    public static class NoiseReduction {
        private boolean enabled = true;
        private boolean aggressive = false;
    }
}
