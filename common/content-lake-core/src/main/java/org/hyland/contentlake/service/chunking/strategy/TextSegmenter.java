package org.hyland.contentlake.service.chunking.strategy;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Utility class for splitting text into structural units (sentences, paragraphs, sections).
 * Used by multiple chunking strategies.
 */
public final class TextSegmenter {

    /** Sentence boundary pattern — handles abbreviations, decimals, and ellipses reasonably well. */
    private static final Pattern SENTENCE_BOUNDARY = Pattern.compile(
            "(?<=[.!?])\\s+(?=[A-Z])" +          // period/excl/question followed by capital
                    "|(?<=\\n)\\s*(?=\\S)" +       // newline boundaries
                    "|(?<=;)\\s+"                  // semicolon as sentence separator
    );

    /** Paragraph boundary (two or more newlines). */
    private static final Pattern PARAGRAPH_BOUNDARY = Pattern.compile("\\n\\s*\\n");

    /** Section heading pattern (e.g., "1.", "1.1", "Chapter 3", "SECTION IV", markdown # headers). */
    private static final Pattern SECTION_HEADING = Pattern.compile(
            "(?m)^\\s*(?:" +
                    "#{1,6}\\s+" +                                    // Markdown headers
                    "|(?:chapter|section|article|part)\\s+[\\divxlc]+" + // Chapter/Section headings
                    "|\\d+(?:\\.\\d+)*\\.?\\s+[A-Z]" +               // Numbered sections (1. Intro, 1.1 Scope)
                    "|[A-Z][A-Z\\s]{3,}$" +                          // ALL CAPS HEADINGS
                    ")",
            Pattern.CASE_INSENSITIVE | Pattern.MULTILINE);

    /**
     * Markdown-style table separator row, e.g. {@code | --- | :--: |} or {@code ---|---}.
     * A run of table-like lines that includes a data row is treated as a table.
     */
    private static final Pattern TABLE_SEPARATOR_ROW = Pattern.compile(
            "^\\s*\\|?\\s*:?-{2,}:?\\s*(?:\\|\\s*:?-{2,}:?\\s*)+\\|?\\s*$");

    /** Minimum consecutive table-like lines that constitute a table block. */
    private static final int MIN_TABLE_LINES = 2;

    /** Minimum pipes for a line to be considered part of a table at all. */
    private static final int MIN_TABLE_PIPES = 2;

    /** Opening or closing fence of a code block, which table detection skips entirely. */
    private static final Pattern CODE_FENCE = Pattern.compile("^\\s*(?:`{3,}|~{3,})");

    private TextSegmenter() {}

    /**
     * Whether a line can <em>anchor</em> a table: a markdown separator row, or a delimited pipe row.
     *
     * <p>A delimited row begins and ends with a pipe. That is what distinguishes a table row from prose
     * that happens to contain pipes, and counting them does not: {@code Pass the | flag | to enable it.}
     * carries exactly two pipes, as does a genuine two-column row, so a threshold or a
     * consistent-count-across-rows rule cannot tell the two apart (#150). Treating such prose as a table
     * exempted it from noise reduction and reported it to callers as tabular.</p>
     *
     * <p>Fixed-width column tables are still not attempted, for the original reason: the pipe is the one
     * signal common text extractors emit reliably.</p>
     */
    static boolean isTableLine(String line) {
        return isSeparatorRow(line) || isDelimitedPipeRow(line);
    }

    private static boolean isSeparatorRow(String line) {
        return line != null && TABLE_SEPARATOR_ROW.matcher(line).matches();
    }

    /** A pipe row that begins and ends with a pipe, so it cannot be prose with pipes in the middle. */
    private static boolean isDelimitedPipeRow(String line) {
        if (line == null) {
            return false;
        }
        String trimmed = line.strip();
        return trimmed.length() > 1
                && trimmed.charAt(0) == '|'
                && trimmed.charAt(trimmed.length() - 1) == '|'
                && countPipes(trimmed) >= MIN_TABLE_PIPES;
    }

    /**
     * Whether a line may <em>join</em> a table run, whether or not it can anchor one.
     *
     * <p>GFM makes the outer pipes optional and some converters leave them out, so requiring delimitation
     * of every row would lose a real table whose rows read {@code Region | FTE | Joiners}. Such a row
     * joins a run that some other line anchors -- in practice the separator row -- and on its own it is
     * indistinguishable from prose, so it never starts one.</p>
     *
     * <p>A separator row joins regardless of its pipe count, which matters for the two-column case: the
     * separator of a two-column table is {@code ---|---}, carrying a single pipe. Gating on the pipe count
     * alone would break the run at exactly the line that proves it is a table, losing a table whose data
     * rows are delimited and whose separator is not.</p>
     */
    private static boolean isTableCandidateLine(String line) {
        return line != null && (isSeparatorRow(line) || countPipes(line) >= MIN_TABLE_PIPES);
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

    /**
     * Detects table blocks in {@code text}, returned as line-aligned {@code [start, end)} character
     * ranges in document order. Ranges never overlap and are sorted by start offset.
     *
     * <p>A block is a run of at least {@link #MIN_TABLE_LINES} consecutive pipe-bearing lines that
     * contains at least one line able to <em>anchor</em> it: a separator row, or a row delimited by
     * pipes. Without that requirement any two adjacent prose lines carrying two pipes each were a table,
     * and the cost was real rather than cosmetic: a table block is exempted from noise reduction and kept
     * atomic as a {@code TABLE} chunk (#150).</p>
     *
     * <p>Lines inside a fenced code block are skipped, because a document that explains markdown contains
     * rows that are delimited and a separator row that is genuine, and no rule about their shape can tell
     * them from the real thing.</p>
     */
    public static List<int[]> detectTableBlocks(String text) {
        List<int[]> blocks = new ArrayList<>();
        if (text == null || text.isEmpty()) {
            return blocks;
        }

        int runStart = -1;      // char offset of the first line in the current run
        int runLines = 0;       // number of consecutive pipe-bearing lines in the current run
        int runEnd = 0;         // char offset just past the last such line's text
        boolean anchored = false; // whether the run holds a separator or a delimited row
        int lineStart = 0;
        boolean inCodeFence = false;

        int i = 0;
        int n = text.length();
        while (i <= n) {
            boolean atEnd = (i == n);
            char c = atEnd ? '\n' : text.charAt(i);
            if (c == '\n' || atEnd) {
                String line = text.substring(lineStart, i);

                if (CODE_FENCE.matcher(line).find()) {
                    // A fence both ends any run in progress and toggles the skip.
                    if (runLines >= MIN_TABLE_LINES && anchored) {
                        blocks.add(new int[]{runStart, runEnd});
                    }
                    runStart = -1;
                    runLines = 0;
                    anchored = false;
                    inCodeFence = !inCodeFence;
                } else if (!inCodeFence && isTableCandidateLine(line)) {
                    if (runStart < 0) {
                        runStart = lineStart;
                        runLines = 0;
                        anchored = false;
                    }
                    runLines++;
                    runEnd = i;
                    anchored |= isTableLine(line);
                } else {
                    if (runLines >= MIN_TABLE_LINES && anchored) {
                        blocks.add(new int[]{runStart, runEnd});
                    }
                    runStart = -1;
                    runLines = 0;
                    anchored = false;
                }
                lineStart = i + 1;
            }
            i++;
        }
        if (runLines >= MIN_TABLE_LINES && anchored) {
            blocks.add(new int[]{runStart, runEnd});
        }
        return blocks;
    }

    /**
     * Splits text into sentences.
     */
    public static List<TextSegment> splitSentences(String text) {
        List<TextSegment> segments = new ArrayList<>();
        String[] parts = SENTENCE_BOUNDARY.split(text);
        int offset = 0;

        for (String part : parts) {
            String trimmed = part.strip();
            if (!trimmed.isEmpty()) {
                int start = text.indexOf(part, offset);
                if (start < 0) start = offset;
                segments.add(new TextSegment(trimmed, start, start + part.length()));
                offset = start + part.length();
            }
        }
        return segments;
    }

    /**
     * Splits text into paragraphs (separated by blank lines).
     */
    public static List<TextSegment> splitParagraphs(String text) {
        List<TextSegment> segments = new ArrayList<>();
        Matcher matcher = PARAGRAPH_BOUNDARY.matcher(text);
        int lastEnd = 0;

        while (matcher.find()) {
            String para = text.substring(lastEnd, matcher.start()).strip();
            if (!para.isEmpty()) {
                segments.add(new TextSegment(para, lastEnd, matcher.start()));
            }
            lastEnd = matcher.end();
        }

        // Last paragraph
        String remaining = text.substring(lastEnd).strip();
        if (!remaining.isEmpty()) {
            segments.add(new TextSegment(remaining, lastEnd, text.length()));
        }
        return segments;
    }

    /**
     * Splits text at section headings while keeping the heading with its content.
     */
    public static List<TextSegment> splitSections(String text) {
        List<TextSegment> sections = new ArrayList<>();
        Matcher matcher = SECTION_HEADING.matcher(text);
        List<Integer> headingStarts = new ArrayList<>();

        while (matcher.find()) {
            headingStarts.add(matcher.start());
        }

        if (headingStarts.isEmpty()) {
            // No sections found, return entire text as one segment
            String trimmed = text.strip();
            if (!trimmed.isEmpty()) {
                sections.add(new TextSegment(trimmed, 0, text.length()));
            }
            return sections;
        }

        // Content before first heading
        if (headingStarts.get(0) > 0) {
            String pre = text.substring(0, headingStarts.get(0)).strip();
            if (!pre.isEmpty()) {
                sections.add(new TextSegment(pre, 0, headingStarts.get(0)));
            }
        }

        // Each section: from one heading to the next
        for (int i = 0; i < headingStarts.size(); i++) {
            int start = headingStarts.get(i);
            int end = (i + 1 < headingStarts.size()) ? headingStarts.get(i + 1) : text.length();
            String section = text.substring(start, end).strip();
            if (!section.isEmpty()) {
                sections.add(new TextSegment(section, start, end));
            }
        }

        return sections;
    }

    /**
     * Table-aware top-level segmentation used by adaptive chunking.
     *
     * <p>Detected table blocks are emitted as atomic {@code table} segments and never section-split;
     * the spans between them are split into heading-delimited sections by {@link #splitSections}.
     * Every emitted segment carries a document-order {@code sectionIndex} so chunks derived from the
     * same section (or the same table) can be identified downstream.</p>
     */
    public static List<TextSegment> splitSectionsAndTables(String text) {
        List<TextSegment> result = new ArrayList<>();
        if (text == null || text.isBlank()) {
            return result;
        }

        List<int[]> tables = detectTableBlocks(text);
        if (tables.isEmpty()) {
            List<TextSegment> sections = splitSections(text);
            for (int i = 0; i < sections.size(); i++) {
                result.add(sections.get(i).withSection(i));
            }
            return result;
        }

        int cursor = 0;
        int sectionIndex = 0;
        for (int[] table : tables) {
            int tableStart = table[0];
            int tableEnd = table[1];

            // Prose span before this table: section-split it, shifting offsets back to absolute.
            if (tableStart > cursor) {
                String prose = text.substring(cursor, tableStart);
                for (TextSegment section : splitSections(prose)) {
                    result.add(new TextSegment(section.text(),
                            cursor + section.startOffset(), cursor + section.endOffset(),
                            false, sectionIndex++));
                }
            }

            String tableText = text.substring(tableStart, tableEnd).strip();
            if (!tableText.isEmpty()) {
                result.add(new TextSegment(tableText, tableStart, tableEnd, true, sectionIndex++));
            }
            cursor = tableEnd;
        }

        // Trailing prose after the last table.
        if (cursor < text.length()) {
            String prose = text.substring(cursor);
            for (TextSegment section : splitSections(prose)) {
                result.add(new TextSegment(section.text(),
                        cursor + section.startOffset(), cursor + section.endOffset(),
                        false, sectionIndex++));
            }
        }

        return result;
    }

    /**
     * Splits an oversized table into row-group segments, repeating the header (and any markdown
     * separator row) at the top of each group so every resulting chunk stays self-contained.
     *
     * <p>Returns the table unchanged (as a single-element list) when it already fits {@code maxSize}
     * or has too few rows to split.</p>
     */
    public static List<TextSegment> splitTableByRowGroups(TextSegment table, int maxSize) {
        String text = table.text();
        if (text.length() <= maxSize) {
            return List.of(table);
        }

        List<String> lines = text.lines().toList();
        if (lines.size() <= 1) {
            return List.of(table);
        }

        String header = lines.get(0);
        int firstDataRow = 1;
        String headerBlock = header;
        if (lines.size() > 1 && TABLE_SEPARATOR_ROW.matcher(lines.get(1)).matches()) {
            headerBlock = header + "\n" + lines.get(1);
            firstDataRow = 2;
        }

        List<TextSegment> groups = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean hasRow = false;
        for (int i = firstDataRow; i < lines.size(); i++) {
            String row = lines.get(i);
            int projected = headerBlock.length() + 1 + current.length() + row.length() + 1;
            if (hasRow && projected > maxSize) {
                groups.add(rowGroupSegment(table, headerBlock, current.toString()));
                current.setLength(0);
                hasRow = false;
            }
            if (!current.isEmpty()) {
                current.append('\n');
            }
            current.append(row);
            hasRow = true;
        }
        if (hasRow) {
            groups.add(rowGroupSegment(table, headerBlock, current.toString()));
        }

        // Degenerate case (e.g. a single header+separator with no data rows): keep the table as-is.
        return groups.isEmpty() ? List.of(table) : groups;
    }

    private static TextSegment rowGroupSegment(TextSegment table, String headerBlock, String rows) {
        String groupText = headerBlock + "\n" + rows;
        return new TextSegment(groupText, table.startOffset(), table.endOffset(), true,
                table.sectionIndex());
    }

    /**
     * Groups consecutive segments into chunks that respect size constraints.
     * Tries to keep chunks between minSize and maxSize characters.
     */
    public static List<TextSegment> groupSegments(List<TextSegment> segments, int minSize, int maxSize) {
        List<TextSegment> grouped = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        int currentStart = -1;
        int currentEnd = 0;

        for (TextSegment segment : segments) {
            // Would adding this segment exceed max?
            if (current.length() + segment.text().length() + 1 > maxSize && current.length() >= minSize) {
                // Flush current group
                grouped.add(new TextSegment(current.toString().strip(), currentStart, currentEnd));
                current.setLength(0);
                currentStart = -1;
            }

            if (currentStart < 0) {
                currentStart = segment.startOffset();
            }

            if (!current.isEmpty()) {
                current.append('\n');
            }
            current.append(segment.text());
            currentEnd = segment.endOffset();
        }

        // Flush remaining
        if (!current.isEmpty()) {
            grouped.add(new TextSegment(current.toString().strip(), currentStart, currentEnd));
        }

        return grouped;
    }

    /**
     * A segment of text with its position in the original document.
     *
     * @param table        whether this segment is a detected table (kept atomic during chunking)
     * @param sectionIndex document-order index of the source section this segment belongs to
     */
    public record TextSegment(String text, int startOffset, int endOffset, boolean table,
                              int sectionIndex) {

        /** Convenience constructor for prose segments in the first section. */
        public TextSegment(String text, int startOffset, int endOffset) {
            this(text, startOffset, endOffset, false, 0);
        }

        public int length() {
            return text.length();
        }

        /** Returns a copy of this segment reassigned to {@code sectionIndex}. */
        public TextSegment withSection(int sectionIndex) {
            return new TextSegment(text, startOffset, endOffset, table, sectionIndex);
        }
    }
}
