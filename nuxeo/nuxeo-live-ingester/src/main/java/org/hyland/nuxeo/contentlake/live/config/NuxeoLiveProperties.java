package org.hyland.nuxeo.contentlake.live.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

@Data
@ConfigurationProperties(prefix = "nuxeo.live")
public class NuxeoLiveProperties {

    private Audit audit = new Audit();
    private Embedding embedding = new Embedding();

    @Data
    public static class Audit {
        private boolean enabled = true;
        private Duration fixedDelay = Duration.ofSeconds(30);
        private Duration initialDelay = Duration.ofSeconds(10);
        private Duration initialLookback = Duration.ofMinutes(5);
        private int pageSize = 100;
        private String cursorFile = "/data/audit-cursor.json";
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
