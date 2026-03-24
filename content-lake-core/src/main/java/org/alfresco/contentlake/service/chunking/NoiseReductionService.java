package org.alfresco.contentlake.service.chunking;

import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Advanced text cleaning pipeline that removes noise from extracted document text.
 *
 * <p>Handles headers/footers, page numbers, watermarks, formatting artifacts,
 * and repetitive boilerplate that degrade embedding quality.</p>
 */
@Slf4j
public class NoiseReductionService {

    // Page number patterns (standalone lines like "Page 3", "- 12 -", "3/15", etc.)
    private static final Pattern PAGE_NUMBER = Pattern.compile(
            "(?m)^\\s*(?:" +
                    "(?:page|p\\.?)\\s*\\d+(?:\\s*(?:of|/)\\s*\\d+)?" +  // Page 3, p.3, Page 3 of 15
                    "|\\d+\\s*(?:of|/)\\s*\\d+" +                         // 3 of 15, 3/15
                    "|-\\s*\\d+\\s*-" +                                   // - 12 -
                    "|\\d{1,4}" +                                         // standalone number (1-4 digits)
                    ")\\s*$",
            Pattern.CASE_INSENSITIVE);

    // Common header/footer patterns
    private static final Pattern HEADER_FOOTER = Pattern.compile(
            "(?m)^\\s*(?:" +
                    "(?:confidential|draft|internal use only|do not distribute|privileged)" +
                    "|(?:copyright|©)\\s*(?:\\d{4}|\\(c\\)).*" +
                    "|(?:all rights reserved).*" +
                    "|(?:printed on|generated on|last (?:updated|modified))\\s+.*" +
                    ")\\s*$",
            Pattern.CASE_INSENSITIVE);

    // Formatting artifacts from PDF extraction
    private static final Pattern PDF_ARTIFACTS = Pattern.compile(
            "(?:" +
                    "\\u0000" +                          // null bytes
                    "|\\cL" +                            // form feed
                    "|\\x{FEFF}" +                       // BOM
                    "|\\x{00AD}" +                       // soft hyphen
                    "|[\\x{200B}-\\x{200F}]" +           // zero-width spaces and direction marks
                    "|[\\x{2028}\\x{2029}]" +            // line/paragraph separators
                    ")");

    // Long runs of repeated characters (often PDF garbage)
    private static final Pattern REPEATED_CHARS = Pattern.compile("(.)\\1{10,}");

    // Excessive dots/dashes (table of contents leaders, horizontal rules)
    private static final Pattern DOT_LEADERS = Pattern.compile("[.·…]{5,}|[-_=]{5,}");

    // Multiple consecutive blank lines
    private static final Pattern EXCESSIVE_BLANKS = Pattern.compile("\\n{4,}");

    // Whitespace normalization
    private static final Pattern HORIZONTAL_WHITESPACE = Pattern.compile("[ \\t\\x0B\\f\\r]+");

    private final boolean aggressive;

    /**
     * @param aggressive when true, applies more aggressive cleaning (removes more borderline content)
     */
    public NoiseReductionService(boolean aggressive) {
        this.aggressive = aggressive;
    }

    /**
     * Cleans extracted text by removing noise patterns.
     *
     * @param text raw extracted text
     * @return cleaned text ready for chunking
     */
    public String clean(String text) {
        if (text == null || text.isBlank()) {
            return "";
        }

        String result = text;

        // Phase 1: Remove binary/encoding artifacts
        result = PDF_ARTIFACTS.matcher(result).replaceAll("");
        result = REPEATED_CHARS.matcher(result).replaceAll("");

        // Phase 2: Normalize whitespace (but preserve paragraph structure)
        result = HORIZONTAL_WHITESPACE.matcher(result).replaceAll(" ");
        result = DOT_LEADERS.matcher(result).replaceAll(" ");

        // Phase 3: Remove structural noise (page numbers, headers/footers)
        result = PAGE_NUMBER.matcher(result).replaceAll("");
        result = HEADER_FOOTER.matcher(result).replaceAll("");

        // Phase 4: Remove repetitive boilerplate (lines that repeat across the document)
        if (aggressive) {
            result = removeRepetitiveLines(result);
        }

        // Phase 5: Final cleanup
        result = EXCESSIVE_BLANKS.matcher(result).replaceAll("\n\n");
        result = result.trim();

        if (log.isDebugEnabled()) {
            int removed = text.length() - result.length();
            if (removed > 0) {
                log.debug("Noise reduction removed {} chars ({}% of original)",
                        removed, String.format("%.1f", 100.0 * removed / text.length()));
            }
        }

        return result;
    }

    /**
     * Removes lines that appear more than a threshold number of times,
     * which typically indicates headers, footers, or watermarks.
     */
    private String removeRepetitiveLines(String text) {
        String[] lines = text.split("\\n");
        if (lines.length < 10) {
            return text;
        }

        // Count occurrences of each non-trivial line
        var lineCounts = Arrays.stream(lines)
                .map(String::strip)
                .filter(l -> l.length() > 3 && l.length() < 100)
                .collect(Collectors.groupingBy(l -> l, Collectors.counting()));

        // Threshold: if a line appears in >15% of all lines, it's likely boilerplate
        long threshold = Math.max(3, lines.length / 7);

        var boilerplate = lineCounts.entrySet().stream()
                .filter(e -> e.getValue() >= threshold)
                .map(java.util.Map.Entry::getKey)
                .collect(Collectors.toSet());

        if (boilerplate.isEmpty()) {
            return text;
        }

        log.debug("Identified {} repetitive boilerplate patterns", boilerplate.size());

        StringBuilder sb = new StringBuilder(text.length());
        for (String line : lines) {
            if (!boilerplate.contains(line.strip())) {
                sb.append(line).append('\n');
            }
        }
        return sb.toString();
    }
}
