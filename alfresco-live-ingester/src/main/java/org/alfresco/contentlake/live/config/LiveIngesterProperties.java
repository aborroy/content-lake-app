package org.alfresco.contentlake.live.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * Configuration properties specific to the live-ingester module.
 *
 * <p>Bound from {@code live-ingester.*} in {@code application.yml}.</p>
 */
@Data
@ConfigurationProperties(prefix = "live-ingester")
public class LiveIngesterProperties {

    /** Event filtering rules (mirrors batch-ingester exclusion config). */
    private Filter filter = new Filter();

    /** Text chunking parameters for the embedding pipeline. */
    private Chunking chunking = new Chunking();

    /** In-memory duplicate suppression for repeated Event2 deliveries. */
    private Dedup dedup = new Dedup();

    @Data
    public static class Filter {
        private List<String> excludePaths = new ArrayList<>(List.of(
                "*/surf-config/*",
                "*/thumbnails/*"
        ));
        private List<String> excludeAspects = new ArrayList<>(List.of(
                "cm:workingcopy"
        ));
    }

    @Data
    public static class Chunking {
        /** Minimum chunk size in characters; short paragraphs are merged up to this floor. */
        private int minChunkSize = 200;
        /** Maximum chunk size in characters. */
        private int maxChunkSize = 1000;
        /** Overlap between consecutive chunks in characters. */
        private int overlapSize = 120;
        /** Cosine-similarity threshold for adaptive chunk merging (0.0–1.0). */
        private double similarityThreshold = 0.75;
    }

    @Data
    public static class Dedup {
        private Duration window = Duration.ofMinutes(2);
        private int maxEntries = 10000;
    }
}
