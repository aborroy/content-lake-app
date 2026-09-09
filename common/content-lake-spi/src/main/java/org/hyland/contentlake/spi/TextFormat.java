package org.hyland.contentlake.spi;

/**
 * Representation of the text an extractor produced.
 *
 * <p>The distinction matters to chunking, not to storage. Chunking is structure-aware: it prefers
 * heading and table boundaries over character offsets, and it detects a table by markdown separator
 * rows or pipe-delimited rows. Plaintext extraction removes exactly those signals, so a table
 * arrives as an undelimited run of text and is chunked as prose.</p>
 */
public enum TextFormat {

    /** Flattened text with no structural markup. */
    PLAIN,

    /**
     * Markdown: ATX headings and pipe-delimited tables with a separator row. Chunking splits on
     * those boundaries and keeps a table atomic.
     */
    MARKDOWN
}
