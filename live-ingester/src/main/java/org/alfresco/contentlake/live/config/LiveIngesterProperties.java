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

    /** Scope rules used after fetching the full Alfresco node snapshot. */
    private Scope scope = new Scope();

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
    public static class Scope {
        private List<String> includePaths = new ArrayList<>();
        private List<String> requiredAspects = new ArrayList<>();
    }

    @Data
    public static class Dedup {
        private Duration window = Duration.ofMinutes(2);
        private int maxEntries = 10000;
    }
}
