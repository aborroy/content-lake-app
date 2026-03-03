package org.alfresco.contentlake.service;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.alfresco.contentlake.model.Chunk;
import org.springframework.ai.embedding.EmbeddingModel;
import org.springframework.ai.embedding.EmbeddingRequest;
import org.springframework.ai.embedding.EmbeddingResponse;
import org.springframework.ai.retry.TransientAiException;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Embedding service with intelligent fallback handling for oversized inputs.
 *
 * <p>IMPORTANT: This service expects properly chunked input from the chunking layer.
 * If chunks are still too large for the embedding model, it will:
 * <ol>
 *   <li>Split the text at semantic boundaries</li>
 *   <li>Embed each half separately</li>
 *   <li>Average the vectors to create a single embedding</li>
 * </ol>
 *
 * <p>To avoid this fallback entirely, ensure your chunking strategy uses
 * appropriate maxChunkSize values (typically 800-1200 chars for most models).
 *
 * <h3>Asymmetric embedding (instruction prefix)</h3>
 * <p>Models like {@code mxbai-embed-large} are trained with an instruction-aware
 * protocol. Query-time embeddings should be prefixed with a task instruction so
 * that the resulting vector is closer in space to relevant passage vectors.
 * Document/chunk embeddings are stored <em>without</em> any prefix.</p>
 *
 * <ul>
 *   <li>{@link #embed(String)} — document/chunk embedding (no prefix)</li>
 *   <li>{@link #embedQuery(String)} — query embedding (with instruction prefix)</li>
 * </ul>
 */
@Slf4j
public class EmbeddingService {

    private static final Pattern TOO_LARGE = Pattern.compile("input \\((\\d+) tokens\\) is too large");

    // Safety cap for pathological inputs (e.g., malformed text, binary garbage)
    // This should rarely trigger if chunking is working correctly
    private static final int SAFETY_CAP = 3000;

    // Proactive split threshold: mxbai-embed-large caps at 512 tokens.
    // At ~4 chars/token for typical text this is ~2048 chars, but dense content
    // (code, paths, punctuation-heavy text) can tokenise at 1-2 chars/token.
    // Using 1400 chars as the pre-emptive split boundary catches dense content
    // without being overly aggressive on normal prose.
    private static final int PROACTIVE_SPLIT_THRESHOLD = 1400;

    private static final int MIN_CHARS = 200;

    /**
     * Instruction prefix for query-time embedding.
     *
     * <p>mxbai-embed-large (and many E5/GTE family models) are trained with an
     * asymmetric protocol: queries are prefixed with a task instruction while
     * documents are embedded as-is. This significantly improves retrieval
     * relevance (typically +5-15% MRR) at zero extra infrastructure cost.</p>
     *
     * <p>If you switch to a different embedding model, update or disable this
     * prefix according to the model's documentation.</p>
     */
    private static final String QUERY_INSTRUCTION_PREFIX =
            "Represent this sentence for searching relevant passages: ";

    private final EmbeddingModel embeddingModel;

    @Getter
    private final String modelName;

    public EmbeddingService(EmbeddingModel embeddingModel, String modelName) {
        this.embeddingModel = embeddingModel;
        this.modelName = modelName;
    }

    /**
     * Embeds document/chunk text <em>without</em> any instruction prefix.
     * Use this for ingestion-time embedding of document chunks.
     *
     * @param text document or chunk text
     * @return embedding vector
     */
    public List<Double> embed(String text) {
        return embedWithFallback(sanitize(text));
    }

    /**
     * Embeds a search query <em>with</em> the instruction prefix required by
     * asymmetric embedding models (e.g. mxbai-embed-large).
     *
     * <p>This should be used at query time in the semantic search path so that
     * the query vector is aligned with the document vectors stored at ingestion
     * time via {@link #embed(String)}.</p>
     *
     * @param query user's natural-language search query
     * @return embedding vector
     */
    public List<Double> embedQuery(String query) {
        String prefixed = QUERY_INSTRUCTION_PREFIX + sanitize(query);
        return embedWithFallback(prefixed);
    }

    /**
     * Embeds a list of chunks (document-side, no instruction prefix).
     */
    public List<ChunkWithEmbedding> embedChunks(List<Chunk> chunks) {
        return embedChunks(chunks, null);
    }

    /**
     * Embeds a list of chunks with optional document metadata context.
     *
     * <p>When {@code documentContext} is provided, it is prepended to each chunk's
     * text <em>only for the embedding call</em>. The stored chunk text is unchanged.
     * This improves retrieval quality by giving the embedding model richer context
     * about the document each chunk belongs to — particularly useful for disambiguating
     * chunks from different documents that contain similar language.</p>
     *
     * <p>Example enriched text sent to the embedding model:</p>
     * <pre>
     * Document: Annual_Report_2025.pdf | Path: /Company Home/Reports
     *
     * Revenue increased by 15% year-over-year...
     * </pre>
     *
     * @param chunks          list of chunks to embed
     * @param documentContext  metadata prefix (e.g. "Document: name | Path: path"), or null to skip
     * @return chunks paired with their embedding vectors
     */
    public List<ChunkWithEmbedding> embedChunks(List<Chunk> chunks, String documentContext) {
        List<ChunkWithEmbedding> results = new ArrayList<>();
        for (Chunk chunk : chunks) {
            String text = chunk.getText();
            if (text == null || text.isBlank()) {
                continue;
            }

            // Enrich text for embedding only — the chunk's stored text is not modified
            String textToEmbed = (documentContext != null && !documentContext.isBlank())
                    ? documentContext + "\n\n" + text
                    : text;

            results.add(new ChunkWithEmbedding(chunk, embed(textToEmbed)));
        }
        return results;
    }

    private List<Double> embedWithFallback(String text) {
        if (text == null || text.isBlank()) {
            return List.of();
        }

        // Safety cap for pathological inputs
        if (text.length() > SAFETY_CAP) {
            log.warn("Embedding input exceeds SAFETY_CAP ({} > {}). " +
                            "This indicates a chunking issue. Truncating and logging for investigation.",
                    text.length(), SAFETY_CAP);
            text = text.substring(0, SAFETY_CAP);
        }

        // Proactive split: avoid a round-trip to the model for inputs that are
        // very likely to exceed the 512-token context limit. Dense content
        // (code, paths, symbols) can tokenise at 1-2 chars/token, so 1400 chars
        // can exceed 512 tokens even though typical English would be fine.
        if (text.length() > PROACTIVE_SPLIT_THRESHOLD) {
            int mid = findSplitPoint(text);
            String left = text.substring(0, mid);
            String right = text.substring(mid);
            log.debug("Proactive split of {} chars into left={}, right={} to stay within token limit.",
                    text.length(), left.length(), right.length());
            List<Double> leftVec = embedWithFallback(left);
            List<Double> rightVec = embedWithFallback(right);
            if (leftVec.isEmpty()) return rightVec;
            if (rightVec.isEmpty()) return leftVec;
            if (leftVec.size() != rightVec.size()) {
                throw new IllegalStateException(
                        "Embedding dimension mismatch after proactive split: " +
                                "left=" + leftVec.size() + ", right=" + rightVec.size());
            }
            int dim = leftVec.size();
            List<Double> avg = new ArrayList<>(dim);
            for (int i = 0; i < dim; i++) {
                avg.add((leftVec.get(i) + rightVec.get(i)) / 2.0d);
            }
            return avg;
        }

        try {
            EmbeddingResponse response = embeddingModel.call(new EmbeddingRequest(List.of(text), null));
            float[] embedding = response.getResults().get(0).getOutput();
            return toDoubleList(embedding);

        } catch (TransientAiException ex) {
            if (!looksLikeTooLarge(ex)) {
                throw ex;
            }

            // If already small, try aggressive trimming
            if (text.length() <= MIN_CHARS) {
                String trimmed = trimWorstParts(text);
                if (trimmed.length() == text.length()) {
                    // Last resort: cut to half
                    int newLen = Math.max(1, text.length() / 2);
                    log.warn("Embedding request still too large at {} chars; " +
                            "last resort truncation to {} chars.", text.length(), newLen);
                    trimmed = text.substring(0, newLen);
                } else {
                    log.warn("Embedding request too large at {} chars; " +
                            "trimmed to {} chars using trimWorstParts().", text.length(), trimmed.length());
                }

                EmbeddingResponse response = embeddingModel.call(new EmbeddingRequest(List.of(trimmed), null));
                float[] embedding = response.getResults().get(0).getOutput();
                return toDoubleList(embedding);
            }

            // Split and average: preserve both halves by embedding each and averaging vectors
            int mid = findSplitPoint(text);
            String left = text.substring(0, mid);
            String right = text.substring(mid);

            log.info("Embedding request too large ({} chars). " +
                            "Splitting into two parts (left={}, right={}) and averaging vectors. " +
                            "Consider reducing maxChunkSize in chunking config.",
                    text.length(), left.length(), right.length());

            List<Double> leftVec = embedWithFallback(left);
            List<Double> rightVec = embedWithFallback(right);

            // If one side failed into empty, return the other
            if (leftVec.isEmpty()) {
                return rightVec;
            }
            if (rightVec.isEmpty()) {
                return leftVec;
            }

            if (leftVec.size() != rightVec.size()) {
                throw new IllegalStateException(
                        "Embedding dimension mismatch after split: " +
                                "left=" + leftVec.size() + ", right=" + rightVec.size());
            }

            // Average element-wise
            int dim = leftVec.size();
            List<Double> avg = new ArrayList<>(dim);
            for (int i = 0; i < dim; i++) {
                avg.add((leftVec.get(i) + rightVec.get(i)) / 2.0d);
            }
            return avg;
        }
    }

    private boolean looksLikeTooLarge(TransientAiException ex) {
        String msg = ex.getMessage();
        if (msg == null) return false;
        return TOO_LARGE.matcher(msg).find() || msg.contains("physical batch size");
    }

    private int findSplitPoint(String text) {
        int mid = text.length() / 2;

        // Prefer splitting on paragraph / sentence / whitespace near the midpoint
        int best;

        best = lastIndexBefore(text, '\n', mid, 120);
        if (best > 0) return best;

        best = lastIndexBefore(text, '.', mid, 120);
        if (best > 0) return best + 1;

        best = lastIndexBefore(text, ' ', mid, 120);
        if (best > 0) return best;

        return mid;
    }

    private int lastIndexBefore(String text, char ch, int from, int window) {
        int start = Math.max(0, from - window);
        for (int i = from; i >= start; i--) {
            if (text.charAt(i) == ch) return i;
        }
        return -1;
    }

    private String sanitize(String text) {
        // Remove nulls, collapse pathological whitespace, keep content
        String s = text.replace("\u0000", "");
        s = s.replaceAll("[ \\t\\x0B\\f\\r]+", " ");
        s = s.replaceAll("\\n{3,}", "\n\n");
        return s.trim();
    }

    private String trimWorstParts(String text) {
        // Heuristic: drop very long "words" (often PDF garbage / encoded runs)
        String[] parts = text.split(" ");
        StringBuilder sb = new StringBuilder(text.length());
        for (String p : parts) {
            if (p.length() > 80) continue;
            sb.append(p).append(' ');
        }
        return sb.toString().trim();
    }

    private List<Double> toDoubleList(float[] array) {
        List<Double> list = new ArrayList<>(array.length);
        for (float f : array) list.add((double) f);
        return list;
    }

    public record ChunkWithEmbedding(Chunk chunk, List<Double> embedding) {}
}