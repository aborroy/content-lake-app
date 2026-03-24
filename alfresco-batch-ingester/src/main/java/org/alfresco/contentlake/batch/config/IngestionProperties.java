package org.alfresco.contentlake.batch.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.ArrayList;
import java.util.List;

/**
 * Configuration for ingestion discovery, filtering, transformation and embedding.
 *
 * <p>Bound from {@code ingestion.*} in {@code application.yml}.</p>
 */
@Data
@ConfigurationProperties(prefix = "ingestion")
public class IngestionProperties {

    private List<Source> sources = new ArrayList<>();
    private Exclude exclude = new Exclude();
    private Transform transform = new Transform();
    private Embedding embedding = new Embedding();

    @Data
    public static class Source {
        private String folder;
        private boolean recursive = true;
        private List<String> types = new ArrayList<>();
    }

    @Data
    public static class Exclude {
        private List<String> paths = new ArrayList<>();
        private List<String> aspects = new ArrayList<>();
    }

    @Data
    public static class Transform {
        private int workerThreads = 4;
        private int queueCapacity = 1000;
    }

    @Data
    public static class Embedding {
        /** Minimum chunk size in characters; short paragraphs are merged up to this floor. */
        private int minChunkSize = 200;
        /** Maximum chunk size in characters; maps to {@code ChunkingConfig.maxChunkSize}. */
        private int chunkSize = 1000;
        /** Overlap between consecutive chunks in characters. */
        private int chunkOverlap = 120;
        /** Cosine-similarity threshold for adaptive chunk merging (0.0–1.0). */
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
