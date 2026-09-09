package org.hyland.contentlake.extractor;

/**
 * What representation extraction should ask a transform engine for.
 *
 * <p>Configured per ingester as {@code extraction.format}. {@link #PLAINTEXT} is the default so an
 * upgrade changes nothing: chunk boundaries, the fulltext mirrors and the resulting embeddings all
 * stay byte-identical until an operator opts in.</p>
 *
 * <p>None of these values can fail an ingest. Markdown is requested only when the engine advertises
 * it for that MIME type, and extraction always degrades to plaintext rather than skipping a
 * document.</p>
 */
public enum ExtractionFormat {

    /** Always request flattened text. Tables arrive as undelimited runs and are chunked as prose. */
    PLAINTEXT,

    /** Request markdown where the engine advertises it, silently using plaintext elsewhere. */
    AUTO,

    /**
     * Like {@link #AUTO}, but logs when a document falls back to plaintext.
     *
     * <p>Set this when markdown is the point of the deployment: the log line is the difference
     * between "the engine is not producing markdown" and "markdown is working", which is otherwise
     * invisible until retrieval numbers move for no apparent reason.</p>
     */
    MARKDOWN;

    /** Whether markdown may be requested at all. */
    public boolean allowsMarkdown() {
        return this != PLAINTEXT;
    }

    /** Whether a fallback to plaintext is worth an operator-visible log line. */
    public boolean reportsFallback() {
        return this == MARKDOWN;
    }

    /**
     * Parses a configured value, tolerating case and surrounding whitespace.
     *
     * @param value configured value, may be {@code null} or blank
     * @return the parsed value, or {@link #PLAINTEXT} when unset or unrecognised
     */
    public static ExtractionFormat parse(String value) {
        if (value == null || value.isBlank()) {
            return PLAINTEXT;
        }
        return switch (value.strip().toLowerCase()) {
            case "auto" -> AUTO;
            case "markdown", "md" -> MARKDOWN;
            default -> PLAINTEXT;
        };
    }
}
