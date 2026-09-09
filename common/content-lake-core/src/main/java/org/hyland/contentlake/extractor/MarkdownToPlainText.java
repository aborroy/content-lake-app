package org.hyland.contentlake.extractor;

import org.hyland.contentlake.spi.ExtractedText;

import java.util.regex.Pattern;

/**
 * Strips markdown markup, leaving the words.
 *
 * <p>Used on the keyword side of the pipeline only. Chunks keep the markdown, because chunking
 * segments on heading and table boundaries and classifies a table by its pipe rows. The fulltext
 * mirrors get this instead: hxpr folds {@code contentLake_extractedText} into its analysed
 * {@code sys_fulltext} index, which the lexical leg of hybrid search queries and re-scores by term
 * frequency. Pipes, dash rules, backticks and link syntax are not terms anyone searches for, and
 * leaving them in dilutes the frequencies of the terms that are.</p>
 *
 * <p>Deliberately a lexical strip rather than a markdown parser: it runs on every document at ingest,
 * the output is only ever fed to an analysed index, and no structure needs to survive. Table cells
 * become space-separated words on one line, which is what the flattened extraction paths already
 * produce for a table, so keyword behaviour stays the same whichever path fed the document.</p>
 */
public final class MarkdownToPlainText {

    /** Fenced code block delimiter, with optional language: ``` or ~~~ */
    private static final Pattern CODE_FENCE = Pattern.compile("(?m)^\\s*(?:```|~~~)[^\\n]*$");

    /** ATX heading markers, e.g. "## Background" -> "Background". */
    private static final Pattern ATX_HEADING = Pattern.compile("(?m)^\\s{0,3}#{1,6}\\s+");

    /** Setext heading underline, e.g. "===" or "---" directly under a line of text. */
    private static final Pattern SETEXT_UNDERLINE = Pattern.compile("(?m)^\\s{0,3}(?:={2,}|-{2,})\\s*$");

    /** Markdown table separator row, the same shape TextSegmenter detects. */
    private static final Pattern TABLE_SEPARATOR_ROW =
            Pattern.compile("(?m)^\\s*\\|?\\s*:?-{2,}:?\\s*(?:\\|\\s*:?-{2,}:?\\s*)+\\|?\\s*$");

    /**
     * Blockquote and list markers at the start of a line.
     *
     * <p>The ordered-list marker is capped at two digits on purpose. CommonMark allows up to nine, but
     * a line opening with a four-digit year ("2024. Revenue grew") is far more common in this content
     * than a list numbered past 99, and stripping it would delete the year from the keyword index that
     * the date-sensitive queries depend on.</p>
     */
    private static final Pattern BLOCK_MARKER =
            Pattern.compile("(?m)^\\s{0,3}(?:>\\s?|[-*+]\\s+|\\d{1,2}[.)]\\s+)");

    /** Inline image or link: ![alt](url) or [text](url) -> alt / text. */
    private static final Pattern INLINE_LINK = Pattern.compile("!?\\[([^\\]]*)]\\([^)]*\\)");

    /** Reference-style link: [text][ref] -> text. */
    private static final Pattern REFERENCE_LINK = Pattern.compile("!?\\[([^\\]]*)]\\[[^\\]]*]");

    /** Autolink: <https://example.com> -> https://example.com */
    private static final Pattern AUTOLINK = Pattern.compile("<((?:https?|mailto):[^>\\s]+)>");

    /** Asterisk emphasis and inline code runs around a word: {@code **bold**}, {@code `code`}. */
    private static final Pattern EMPHASIS = Pattern.compile("(\\*{1,3}|`{1,3})(?=\\S)(.+?)(?<=\\S)\\1");

    /**
     * Underscore emphasis, only at word boundaries.
     *
     * <p>CommonMark does not allow intraword {@code _} emphasis, and neither can this: the corpus is
     * full of snake_case identifiers, and the lexical leg is the one thing that retrieves a rare
     * identifier token. Stripping the underscores out of {@code contentLake_extractedText} would
     * merge it into a token no query matches.</p>
     */
    private static final Pattern UNDERSCORE_EMPHASIS =
            Pattern.compile("(?<![A-Za-z0-9_])(_{1,3})(?=\\S)(.+?)(?<=\\S)\\1(?![A-Za-z0-9_])");

    /** Escaped punctuation, e.g. \| or \* */
    private static final Pattern ESCAPE = Pattern.compile("\\\\([\\\\`*_{}\\[\\]()#+\\-.!|])");

    /** A horizontal rule drawn with asterisks or underscores. Dash rules are handled as setext. */
    private static final Pattern HORIZONTAL_RULE =
            Pattern.compile("(?m)^\\s{0,3}(?:(?:\\*\\s*){3,}|(?:_\\s*){3,})$");

    private static final Pattern TRAILING_SPACES = Pattern.compile("(?m)[ \\t]+$");
    private static final Pattern EXCESSIVE_BLANKS = Pattern.compile("\\n{3,}");

    private MarkdownToPlainText() {}

    /**
     * Returns the text of {@code extracted}, stripped of markdown when it is markdown.
     *
     * <p>A no-op for {@link org.hyland.contentlake.spi.TextFormat#PLAIN}, so the default
     * configuration writes byte-identical values to what it wrote before.</p>
     *
     * @param extracted extraction result, may be {@code null}
     * @return plain text, or {@code null} when {@code extracted} is {@code null}
     */
    public static String toPlainText(ExtractedText extracted) {
        if (extracted == null) {
            return null;
        }
        return extracted.isMarkdown() ? strip(extracted.text()) : extracted.text();
    }

    /**
     * Strips markdown markup from {@code markdown}.
     *
     * @param markdown markdown text, may be {@code null}
     * @return the same text with markup removed
     */
    public static String strip(String markdown) {
        if (markdown == null || markdown.isBlank()) {
            return markdown;
        }

        String text = markdown.replace("\r\n", "\n").replace('\r', '\n');

        // Table rows first: the separator row goes entirely, and a data row's pipes become spaces.
        // Order matters, because a separator row is also a pipe row.
        text = TABLE_SEPARATOR_ROW.matcher(text).replaceAll("");
        text = stripTableRows(text);

        text = CODE_FENCE.matcher(text).replaceAll("");
        text = ATX_HEADING.matcher(text).replaceAll("");
        text = SETEXT_UNDERLINE.matcher(text).replaceAll("");
        text = HORIZONTAL_RULE.matcher(text).replaceAll("");
        text = BLOCK_MARKER.matcher(text).replaceAll("");

        text = INLINE_LINK.matcher(text).replaceAll("$1");
        text = REFERENCE_LINK.matcher(text).replaceAll("$1");
        text = AUTOLINK.matcher(text).replaceAll("$1");

        // Emphasis nests (***both***), so apply until it stops changing rather than once.
        for (int pass = 0; pass < 3; pass++) {
            String previous = text;
            text = EMPHASIS.matcher(text).replaceAll("$2");
            text = UNDERSCORE_EMPHASIS.matcher(text).replaceAll("$2");
            if (text.equals(previous)) {
                break;
            }
        }

        text = ESCAPE.matcher(text).replaceAll("$1");
        text = TRAILING_SPACES.matcher(text).replaceAll("");
        text = EXCESSIVE_BLANKS.matcher(text).replaceAll("\n\n");
        return text.strip();
    }

    /**
     * Turns each pipe-delimited row into its cells separated by two spaces.
     *
     * <p>Two spaces rather than one so cell boundaries stay visible to a human reading the mirror,
     * and so a numeric cell does not run into its neighbour as one token.</p>
     */
    private static String stripTableRows(String text) {
        StringBuilder out = new StringBuilder(text.length());
        for (String line : text.split("\n", -1)) {
            if (out.length() > 0) {
                out.append('\n');
            }
            out.append(countPipes(line) >= 2 ? cellsOf(line) : line);
        }
        return out.toString();
    }

    private static int countPipes(String line) {
        int pipes = 0;
        for (int i = 0; i < line.length(); i++) {
            if (line.charAt(i) == '|') {
                pipes++;
            }
        }
        return pipes;
    }

    private static String cellsOf(String row) {
        StringBuilder cells = new StringBuilder(row.length());
        for (String cell : row.split("\\|", -1)) {
            String trimmed = cell.strip();
            if (trimmed.isEmpty()) {
                continue;
            }
            if (cells.length() > 0) {
                cells.append("  ");
            }
            cells.append(trimmed);
        }
        return cells.toString();
    }
}
