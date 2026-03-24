package org.alfresco.contentlake.service.chunking;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.alfresco.contentlake.model.Chunk;
import org.alfresco.contentlake.service.chunking.strategy.AdaptiveChunkingStrategy;
import org.alfresco.contentlake.service.chunking.strategy.ChunkingStrategy;
import org.alfresco.contentlake.service.chunking.strategy.ChunkingStrategy.ChunkingConfig;

import java.util.List;

/**
 * Simplified chunking service using a single adaptive strategy.
 *
 * <p>Replaces the original semantic chunking pipeline with a simpler approach:
 * <ol>
 *   <li>Noise reduction (clean extracted text)</li>
 *   <li>Adaptive chunking (works for all document types)</li>
 * </ol>
 *
 * <p>This approach eliminates the need for content type classification
 * and multiple strategy implementations.</p>
 */
@Slf4j
@RequiredArgsConstructor
public class SimpleChunkingService {

    private final NoiseReductionService noiseReduction;
    private final ChunkingConfig config;
    private final ChunkingStrategy strategy;

    /**
     * Convenience constructor that uses the default adaptive strategy.
     */
    public SimpleChunkingService(NoiseReductionService noiseReduction, ChunkingConfig config) {
        this(noiseReduction, config, new AdaptiveChunkingStrategy());
    }

    /**
     * Processes text through noise reduction and adaptive chunking.
     *
     * @param text     raw extracted text
     * @param nodeId   Alfresco node ID
     * @param mimeType original document MIME type (for logging only)
     * @return list of semantically coherent chunks
     */
    public List<Chunk> chunk(String text, String nodeId, String mimeType) {
        if (text == null || text.isBlank()) {
            return List.of();
        }

        // Step 1: Clean the text
        String cleanedText = noiseReduction.clean(text);
        if (cleanedText.isBlank()) {
            log.warn("Text became empty after noise reduction for node: {}", nodeId);
            return List.of();
        }

        // Step 2: Chunk using adaptive strategy
        List<Chunk> chunks = strategy.chunk(cleanedText, nodeId, config);

        log.info("Chunking produced {} chunks for node {} (mimeType: {}, strategy: {})",
                chunks.size(), nodeId, mimeType, strategy.strategyName());

        return chunks;
    }
}