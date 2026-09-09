package org.hyland.contentlake.extractor;

import org.hyland.contentlake.spi.ExtractedText;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class MarkdownToPlainTextTest {

    @Test
    void plainFormatIsUntouched() {
        String text = "A | line | with pipes and ## hashes stays exactly as it is.";

        assertThat(MarkdownToPlainText.toPlainText(ExtractedText.plain(text))).isEqualTo(text);
    }

    @Test
    void nullExtractionYieldsNull() {
        assertThat(MarkdownToPlainText.toPlainText(null)).isNull();
        assertThat(MarkdownToPlainText.strip(null)).isNull();
    }

    @Test
    void blankInputIsReturnedUnchanged() {
        assertThat(MarkdownToPlainText.strip("   ")).isEqualTo("   ");
    }

    @Test
    void atxHeadingMarkersAreRemovedButHeadingTextSurvives() {
        String markdown = """
                # Workforce Report
                Body text.

                ### Basis of preparation
                More body text.
                """;

        String plain = MarkdownToPlainText.strip(markdown);

        assertThat(plain).contains("Workforce Report").contains("Basis of preparation");
        assertThat(plain).doesNotContain("#");
    }

    @Test
    void tableBecomesSpaceSeparatedCellsAndTheSeparatorRowGoes() {
        String markdown = """
                | Region | Closing FTE | Net change |
                | --- | --- | --- |
                | Iberia | 227 | 59 |
                """;

        String plain = MarkdownToPlainText.strip(markdown);

        assertThat(plain).doesNotContain("|").doesNotContain("---");
        assertThat(plain).contains("Region  Closing FTE  Net change");
        assertThat(plain).contains("Iberia  227  59");
    }

    @Test
    void tableCellValuesDoNotRunTogetherIntoOneToken() {
        String plain = MarkdownToPlainText.strip("| 1,840 | 310 |\n| 268 | 1,882 |");

        // Two spaces, so an analyser sees separate numeric tokens rather than "1,840310".
        assertThat(plain).contains("1,840  310");
        assertThat(plain).doesNotContain("1,840310");
    }

    /**
     * The lexical leg is what retrieves a rare identifier, and CommonMark forbids intraword
     * underscore emphasis anyway. Merging the parts of a snake_case token would make it unmatchable.
     */
    @Test
    void snakeCaseIdentifiersSurviveIntact() {
        String markdown = "Query contentLake_extractedText, not sys_fulltextBinary, for CHG-105402.";

        String plain = MarkdownToPlainText.strip(markdown);

        assertThat(plain)
                .contains("contentLake_extractedText")
                .contains("sys_fulltextBinary")
                .contains("CHG-105402");
    }

    @Test
    void emphasisMarkersAreRemovedAroundWords() {
        String plain = MarkdownToPlainText.strip("**Bold** and _italic_ and `code` and ***both***.");

        assertThat(plain).isEqualTo("Bold and italic and code and both.");
    }

    @Test
    void linkSyntaxLeavesTheLabelAndTheBareUrl() {
        String markdown = """
                See [the runbook](https://example.com/runbook) and ![a chart](chart.png).
                Reference [style][ref] too, plus <https://example.com/direct>.
                """;

        String plain = MarkdownToPlainText.strip(markdown);

        assertThat(plain).contains("the runbook").doesNotContain("https://example.com/runbook");
        assertThat(plain).contains("a chart").doesNotContain("chart.png");
        assertThat(plain).contains("style").doesNotContain("[ref]");
        assertThat(plain).contains("https://example.com/direct").doesNotContain("<https");
    }

    @Test
    void listAndBlockquoteMarkersAreRemoved() {
        String markdown = """
                - first item
                * second item
                1. numbered item
                > quoted line
                """;

        String plain = MarkdownToPlainText.strip(markdown);

        assertThat(plain).contains("first item").contains("second item")
                .contains("numbered item").contains("quoted line");
        assertThat(plain.lines()).noneMatch(line -> line.startsWith("-") || line.startsWith(">"));
    }

    /**
     * A leading four-digit year must survive. The golden set has a date-sensitive category, and the
     * lexical leg is what retrieves a bare year token.
     */
    @Test
    void aLeadingYearIsNotMistakenForAnOrderedListMarker() {
        String plain = MarkdownToPlainText.strip("2024. Revenue grew by twelve percent.\n1. First item");

        assertThat(plain).contains("2024.").contains("Revenue grew");
        assertThat(plain).contains("First item").doesNotContain("1. First");
    }

    @Test
    void codeFencesAreRemovedButCodeContentStays() {
        String markdown = """
                Run this:

                ```bash
                make up-alfresco
                ```
                """;

        String plain = MarkdownToPlainText.strip(markdown);

        assertThat(plain).contains("make up-alfresco");
        assertThat(plain).doesNotContain("```").doesNotContain("bash\n");
    }

    @Test
    void escapedPunctuationIsUnescaped() {
        assertThat(MarkdownToPlainText.strip("A literal \\| pipe and a \\* star."))
                .isEqualTo("A literal | pipe and a * star.");
    }

    @Test
    void markdownFormatIsStrippedThroughTheExtractedTextEntryPoint() {
        ExtractedText extracted = ExtractedText.markdown("## Heading\n\n| a | b |\n| --- | --- |\n| 1 | 2 |");

        String plain = MarkdownToPlainText.toPlainText(extracted);

        assertThat(plain).isEqualTo("Heading\n\na  b\n\n1  2");
    }

    @Test
    void horizontalRulesAreRemoved() {
        String plain = MarkdownToPlainText.strip("Before\n\n***\n\nAfter\n\n___\n\nEnd");

        assertThat(plain).contains("Before").contains("After").contains("End");
        assertThat(plain).doesNotContain("***").doesNotContain("___");
    }

    @Test
    void windowsLineEndingsAreNormalised() {
        assertThat(MarkdownToPlainText.strip("# One\r\n\r\nTwo\r\n")).isEqualTo("One\n\nTwo");
    }
}
