package org.alfresco.contentlake.service.chunking.strategy;

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

    private TextSegmenter() {}

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
     */
    public record TextSegment(String text, int startOffset, int endOffset) {
        public int length() {
            return text.length();
        }
    }
}
