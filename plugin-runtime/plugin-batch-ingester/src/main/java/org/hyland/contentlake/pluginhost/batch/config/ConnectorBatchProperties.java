package org.hyland.contentlake.pluginhost.batch.config;

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

    /**
     * Incremental discovery through the connector's change feed, when it has one. Off by default, so the
     * feature is purely additive: a deployment that does not turn it on walks exactly as it does today.
     */
    private ChangeFeed changeFeed = new ChangeFeed();

    /** Where the host keeps each source's feed position. */
    private Cursor cursor = new Cursor();

    /**
     * Where the host keeps the roots an operator chose, so a scope change needs no restart.
     *
     * <p>Off by default, which preserves exactly today's behaviour: with no store, roots come from
     * {@code connector.roots} and then from the connector, and a deployment that can supply neither still
     * fails at startup rather than reporting an empty source on every run.</p>
     */
    private Selection selection = new Selection();

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

    /**
     * Reading a connector's change feed instead of walking it.
     *
     * <p>Enabling this asks for incremental passes; it does not promise them. A connector that answers
     * {@code false} to {@code ContentSourceClient.supportsChangeFeed()} is walked regardless, so the setting
     * is safe to turn on across a deployment where only some connectors have a feed.</p>
     */
    @Data
    public static class ChangeFeed {

        /** Off by default, so the feature is additive: an untouched deployment walks as it did before. */
        private boolean enabled = false;

        /** Soft bound on the changes asked for per feed page. */
        private int pageSize = 200;

        /**
         * How many pages one pass may consume. A feed with more left is not a failure: the next pass
         * resumes from the cursor this one reached.
         */
        private int maxPages = 100;

        /**
         * Force a full walk plus reconciliation sweep every Nth incremental pass; {@code 0} never does.
         *
         * <p>The only mechanism that catches a deletion the feed never reported, which is why the sweep is
         * suspended during an incremental pass rather than removed for feed-capable sources. On a daily
         * incremental schedule, {@code 24} gives a walk roughly once a day.</p>
         */
        private int fullWalkEvery = 0;
    }

    /**
     * Where the host keeps each source's feed position.
     *
     * <p>The host keeps it, never the connector: a connector that persisted its own cursor would have to be
     * trusted to forget it whenever the index was rebuilt, and nothing could check that it had.</p>
     */
    @Data
    public static class Cursor {

        /** Which store to use. */
        private Store store = Store.HXPR;

        /** Folder for the state documents when {@code store} is {@code HXPR}. */
        private String hxprPath = "/content-lake/_state/cursors";

        /** State file when {@code store} is {@code FILE}. Needs a writable mount. */
        private String file = "/data/connector-cursor.json";

        public enum Store {
            /**
             * One state document per source in hxpr. The default because the deployment gives this
             * container no writable mount, and hxpr is somewhere it can already write.
             */
            HXPR,
            /** A JSON file, for a deployment that does mount writable state. */
            FILE,
            /**
             * Nothing survives a restart, so every run after one is a full walk. For dev, and the honest
             * answer for a container with neither a mount nor write access to hxpr.
             */
            MEMORY
        }
    }

    /**
     * Where the host keeps the roots an operator chose for a source.
     *
     * <p>Mirrors {@link Cursor} rather than sharing it. Both are keyed by the qualified source id, so one
     * store would put a source's cursor and its selection at the same path, and the generation counter a
     * cursor carries has no meaning for a selection.</p>
     *
     * <p>Note that both the {@code HXPR} and {@code FILE} stores are wiped by {@code make clean}: one lives in
     * the index it wipes, the other on the state volume it removes. That is correct, because a selection is
     * the scope of an index that no longer exists. A selection does survive a container restart, which is the
     * case that distinguishes this from the startup configuration it replaces.</p>
     */
    @Data
    public static class Selection {

        /** Which store to use. {@code NONE} is the default and keeps today's behaviour exactly. */
        private Store store = Store.NONE;

        /**
         * Folder for the state documents when {@code store} is {@code HXPR}.
         *
         * <p>Deliberately not the cursor folder. The two would otherwise collide per source.</p>
         */
        private String hxprPath = "/content-lake/_state/roots";

        /** State file when {@code store} is {@code FILE}. Needs a writable mount. */
        private String file = "/var/lib/content-lake/connector/roots.json";

        public enum Store {
            /**
             * No selection is stored and the API that writes one is not available. Roots come from
             * {@code connector.roots} and then from the connector, and a deployment that supplies neither
             * still fails at startup. This is the default so the feature is purely additive.
             */
            NONE,
            /** One state document per source in hxpr, alongside the cursors and invisible to a sweep. */
            HXPR,
            /** A JSON file, for a deployment that mounts writable state. */
            FILE,
            /** Forgotten on restart, which falls back to configured roots. For a test. */
            MEMORY
        }
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
