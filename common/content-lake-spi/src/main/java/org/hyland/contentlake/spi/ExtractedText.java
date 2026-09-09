package org.hyland.contentlake.spi;

/**
 * Text an extractor produced, together with the representation it is in.
 *
 * <p>An extractor that can emit structure has no way to say so through a bare {@code String} return,
 * and the pipeline has no way to tell markdown from prose that happens to contain a pipe. Carrying
 * the format alongside the text removes the guess.</p>
 *
 * @param text   extracted text, never {@code null} on a successful extraction
 * @param format representation {@code text} is in
 */
public record ExtractedText(String text, TextFormat format) {

    public ExtractedText {
        if (format == null) {
            throw new IllegalArgumentException("format is required");
        }
    }

    /** Wraps flattened text, the representation every extractor can produce. */
    public static ExtractedText plain(String text) {
        return new ExtractedText(text, TextFormat.PLAIN);
    }

    /** Wraps markdown: ATX headings and pipe tables that chunking can segment on. */
    public static ExtractedText markdown(String text) {
        return new ExtractedText(text, TextFormat.MARKDOWN);
    }

    /**
     * Wraps {@code text}, or returns {@code null} when there is nothing to wrap.
     *
     * <p>The {@link TextExtractor} contract uses a {@code null} return for "no text can be produced",
     * so a caller adapting a {@code String}-returning extractor needs that mapping in one place
     * rather than at every call site.</p>
     */
    public static ExtractedText of(String text, TextFormat format) {
        return (text == null || text.isBlank()) ? null : new ExtractedText(text, format);
    }

    public boolean isMarkdown() {
        return format == TextFormat.MARKDOWN;
    }
}
