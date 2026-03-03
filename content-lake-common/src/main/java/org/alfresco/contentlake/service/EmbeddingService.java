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
 * Embedding service with token-aware truncation for oversized inputs.
 *
 * <p>IMPORTANT: This service expects properly chunked input from the chunking layer.
 * If chunks are still too large for the embedding model's token limit, the error
 * message reports the actual token count, which is used to compute the precise
 * truncation point — keeping as much text as possible while fitting the model's
 * context window. The stored chunk text is never modified; only the text sent
 * to the embedding model is truncated.</p>
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

    // Target token count when truncating: stay under the model's 512-token limit
    // with a small margin for tokeniser variance between the truncated and original text.
    private static final int TARGET_TOKENS = 480;

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

        try {
            EmbeddingResponse response = embeddingModel.call(new EmbeddingRequest(List.of(text), null));
            float[] embedding = response.getResults().get(0).getOutput();
            return toDoubleList(embedding);

        } catch (TransientAiException ex) {
            if (!looksLikeTooLarge(ex)) {
                throw ex;
            }

            // Parse the actual token count from the error to compute precise truncation.
            // E.g. "input (728 tokens) is too large" → 728
            int reportedTokens = extractTokenCount(ex);

            String truncated;
            if (reportedTokens > 0) {
                // Calculate the maximum chars that fit in TARGET_TOKENS using the
                // observed chars/token ratio from this specific text.
                int safeChars = (int) ((long) text.length() * TARGET_TOKENS / reportedTokens);
                safeChars = Math.max(safeChars, MIN_CHARS);
                truncated = truncateAtBoundary(text, safeChars);

                log.info("Embedding input too large ({} tokens from {} chars). " +
                                "Truncating to {} chars (~{} tokens).",
                        reportedTokens, text.length(), truncated.length(), TARGET_TOKENS);
            } else {
                // Could not parse token count — fall back to halving
                truncated = truncateAtBoundary(text, text.length() / 2);
                log.warn("Embedding input too large but could not parse token count. " +
                        "Truncating from {} to {} chars.", text.length(), truncated.length());
            }

            // Recurse: the truncated text should now fit, but if the token density
            // in the kept portion is higher than the overall average, another round
            // of truncation will handle it.
            return embedWithFallback(truncated);
        }
    }

    private int extractTokenCount(TransientAiException ex) {
        if (ex.getMessage() == null) return -1;
        Matcher m = TOO_LARGE.matcher(ex.getMessage());
        if (m.find()) {
            try {
                return Integer.parseInt(m.group(1));
            } catch (NumberFormatException e) {
                return -1;
            }
        }
        return -1;
    }

    private String truncateAtBoundary(String text, int maxChars) {
        if (text.length() <= maxChars) return text;
        int end = Math.min(maxChars, text.length());

        // Prefer a sentence or word boundary near the target length
        int best = lastIndexBefore(text, '.', end, 80);
        if (best > 0) return text.substring(0, best + 1);

        best = lastIndexBefore(text, '\n', end, 80);
        if (best > 0) return text.substring(0, best);

        best = lastIndexBefore(text, ' ', end, 40);
        if (best > 0) return text.substring(0, best);

        return text.substring(0, end);
    }

    private boolean looksLikeTooLarge(TransientAiException ex) {
        String msg = ex.getMessage();
        if (msg == null) return false;
        return TOO_LARGE.matcher(msg).find() || msg.contains("physical batch size");
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

    private List<Double> toDoubleList(float[] array) {
        List<Double> list = new ArrayList<>(array.length);
        for (float f : array) list.add((double) f);
        return list;
    }

    public record ChunkWithEmbedding(Chunk chunk, List<Double> embedding) {}
}