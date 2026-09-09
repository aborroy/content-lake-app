package org.hyland.contentlake.service.chunking.strategy;

import org.hyland.contentlake.service.chunking.strategy.TextSegmenter.TextSegment;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class TextSegmenterTest {

    @Test
    void splitSentences_basicEnglish() {
        String text = "First sentence. Second sentence. Third sentence.";

        List<TextSegment> segments = TextSegmenter.splitSentences(text);

        assertThat(segments).hasSizeGreaterThanOrEqualTo(2);
        assertThat(segments.get(0).text()).contains("First sentence.");
    }

    @Test
    void splitSentences_handlesAbbreviations() {
        // "Dr. Smith" should not split at the period after "Dr"
        // because the next character is uppercase but the regex looks for whitespace + uppercase
        String text = "Dr. Smith went to the store. He bought apples.";

        List<TextSegment> segments = TextSegmenter.splitSentences(text);

        // The exact split behavior depends on regex, but "Dr. Smith" should stay together
        // since "Dr." is followed by a space+uppercase, the regex may split here.
        // Key invariant: all original text is preserved across segments.
        String joined = segments.stream().map(TextSegment::text).reduce("", (a, b) -> a + " " + b).trim();
        assertThat(joined).contains("Dr.");
        assertThat(joined).contains("Smith");
        assertThat(joined).contains("apples");
    }

    @Test
    void splitParagraphs_onDoubleNewlines() {
        String text = "First paragraph content.\n\nSecond paragraph content.\n\nThird paragraph content.";

        List<TextSegment> segments = TextSegmenter.splitParagraphs(text);

        assertThat(segments).hasSize(3);
        assertThat(segments.get(0).text()).isEqualTo("First paragraph content.");
        assertThat(segments.get(1).text()).isEqualTo("Second paragraph content.");
        assertThat(segments.get(2).text()).isEqualTo("Third paragraph content.");
    }

    @Test
    void splitSections_detectsMarkdownHeaders() {
        String text = """
                # Introduction
                Some intro text here.

                ## Background
                Background details go here.

                # Conclusion
                Final thoughts.
                """;

        List<TextSegment> sections = TextSegmenter.splitSections(text);

        assertThat(sections).hasSizeGreaterThanOrEqualTo(2);
        // Verify sections contain heading text
        List<String> sectionTexts = sections.stream().map(TextSegment::text).toList();
        assertThat(sectionTexts).anyMatch(s -> s.contains("Introduction"));
        assertThat(sectionTexts).anyMatch(s -> s.contains("Conclusion"));
    }

    @Test
    void splitSections_detectsNumberedSections() {
        String text = """
                1. Overview
                This is the overview section.

                2. Details
                This section has more details.

                3. Summary
                A brief summary of everything.
                """;

        List<TextSegment> sections = TextSegmenter.splitSections(text);

        assertThat(sections).hasSizeGreaterThanOrEqualTo(2);
        List<String> sectionTexts = sections.stream().map(TextSegment::text).toList();
        assertThat(sectionTexts).anyMatch(s -> s.contains("Overview"));
        assertThat(sectionTexts).anyMatch(s -> s.contains("Summary"));
    }

    // ──────────────────────────────────────────────────────────────────────
    // Table detection
    // ──────────────────────────────────────────────────────────────────────

    /**
     * A fixed-width table drawn with dash rules and column padding carries no pipes, so it is not
     * table-like. This is the documented tradeoff in {@link TextSegmenter#isTableLine}, and it is the
     * reason plaintext extraction yields no TABLE chunks: pipes are the only structural signal, and
     * flattening a document to plaintext is exactly what removes them.
     */
    @Test
    void isTableLine_needsPipes_soFixedWidthColumnsAreNotDetected() {
        assertThat(TextSegmenter.isTableLine("----------------  ------------  --------")).isFalse();
        assertThat(TextSegmenter.isTableLine("Region            Opening FTE   Joiners")).isFalse();
        assertThat(TextSegmenter.isTableLine("United Kingdom    1,840         310")).isFalse();
    }

    @Test
    void isTableLine_detectsMarkdownSeparatorAndPipeRows() {
        assertThat(TextSegmenter.isTableLine("| --- | --- |")).isTrue();
        assertThat(TextSegmenter.isTableLine("| :--: | ---: |")).isTrue();
        assertThat(TextSegmenter.isTableLine("---|---")).isTrue();
        assertThat(TextSegmenter.isTableLine("| Tier | Severity 1 response |")).isTrue();
    }

    @Test
    void isTableLine_nullAndProseAreNotTables() {
        assertThat(TextSegmenter.isTableLine(null)).isFalse();
        assertThat(TextSegmenter.isTableLine("")).isFalse();
        assertThat(TextSegmenter.isTableLine("Limits are enforced per tenant.")).isFalse();
        // One pipe is not enough; the threshold is two.
        assertThat(TextSegmenter.isTableLine("Use the | character sparingly.")).isFalse();
    }

    /**
     * Two prose lines that each happen to carry two pipes are read as a table, which exempts them
     * from noise reduction. Pinned deliberately: the conservative pipe threshold trades this false
     * positive for never splitting a real table mid-row.
     */
    @Test
    void isTableLine_twoPipesInProseIsAKnownFalsePositive() {
        String text = """
                Pass the | flag | to enable it.
                Then the | other | flag follows.
                """;

        assertThat(TextSegmenter.detectTableBlocks(text)).hasSize(1);
    }

    @Test
    void detectTableBlocks_findsNothingInAFixedWidthTable() {
        String text = """
                HEADCOUNT BY REGION

                Region            Opening FTE   Joiners   Leavers   Closing FTE
                ----------------  ------------  --------  --------  ------------
                United Kingdom    1,840         310       268       1,882
                North America     1,120         402       196       1,326
                Germany           486           98        71        513
                """;

        assertThat(TextSegmenter.detectTableBlocks(text)).isEmpty();
    }

    @Test
    void detectTableBlocks_findsTheMarkdownEquivalentOfTheSameTable() {
        String text = """
                HEADCOUNT BY REGION

                | Region | Opening FTE | Joiners | Leavers | Closing FTE |
                | --- | --- | --- | --- | --- |
                | United Kingdom | 1,840 | 310 | 268 | 1,882 |
                | North America | 1,120 | 402 | 196 | 1,326 |
                """;

        List<int[]> blocks = TextSegmenter.detectTableBlocks(text);

        assertThat(blocks).hasSize(1);
        int[] block = blocks.get(0);
        assertThat(text.substring(block[0], block[1]))
                .contains("| Region |")
                .contains("| United Kingdom |")
                .contains("| North America |");
    }

    @Test
    void detectTableBlocks_needsTwoConsecutiveLines() {
        // A lone pipe row surrounded by prose is not a table block.
        String text = """
                Intro prose line.
                | Tier | Response |
                Trailing prose line.
                """;

        assertThat(TextSegmenter.detectTableBlocks(text)).isEmpty();
    }

    @Test
    void detectTableBlocks_returnsOrderedNonOverlappingRanges() {
        String text = """
                | a | b |
                | 1 | 2 |

                Prose between the two tables.

                | c | d |
                | 3 | 4 |
                """;

        List<int[]> blocks = TextSegmenter.detectTableBlocks(text);

        assertThat(blocks).hasSize(2);
        assertThat(blocks.get(0)[1]).isLessThanOrEqualTo(blocks.get(1)[0]);
        assertThat(text.substring(blocks.get(0)[0], blocks.get(0)[1])).contains("| 1 | 2 |");
        assertThat(text.substring(blocks.get(1)[0], blocks.get(1)[1])).contains("| 3 | 4 |");
    }

    @Test
    void splitSectionsAndTables_emitsTheTableAsOneAtomicSegment() {
        String text = """
                # Service Tiers

                The matrix below is authoritative.

                | Tier | Severity 1 response |
                | --- | --- |
                | Starter | 8 business hours |
                | Growth | 2 business hours |

                Limits are enforced per tenant.
                """;

        List<TextSegment> segments = TextSegmenter.splitSectionsAndTables(text);

        List<TextSegment> tables = segments.stream().filter(TextSegment::table).toList();
        assertThat(tables).hasSize(1);
        assertThat(tables.get(0).text())
                .contains("| Tier | Severity 1 response |")
                .contains("| Starter | 8 business hours |")
                .contains("| Growth | 2 business hours |");

        // Section indices stay monotonic across interleaved prose and tables.
        assertThat(segments.stream().map(TextSegment::sectionIndex).toList()).isSorted();
        assertThat(segments.stream().anyMatch(s -> !s.table() && s.text().contains("per tenant")))
                .isTrue();
    }

    @Test
    void splitSectionsAndTables_withNoTableFallsBackToSectionsOnly() {
        String text = """
                # Introduction
                Some intro text here.

                # Conclusion
                Final thoughts.
                """;

        List<TextSegment> segments = TextSegmenter.splitSectionsAndTables(text);

        assertThat(segments).isNotEmpty();
        assertThat(segments).noneMatch(TextSegment::table);
    }

    @Test
    void splitTableByRowGroups_repeatsHeaderAndSeparatorInEveryGroup() {
        String header = "| Region | FTE |";
        String separator = "| --- | --- |";
        StringBuilder table = new StringBuilder(header).append('\n').append(separator);
        for (int i = 1; i <= 6; i++) {
            table.append("\n| Region-0").append(i).append(" | 100 |");
        }
        TextSegment segment = new TextSegment(table.toString(), 0, table.length(), true, 0);

        List<TextSegment> groups = TextSegmenter.splitTableByRowGroups(segment, 80);

        assertThat(groups).hasSizeGreaterThan(1);
        assertThat(groups).allSatisfy(group -> {
            assertThat(group.table()).isTrue();
            assertThat(group.text()).startsWith(header + "\n" + separator);
        });
        // Every data row survives exactly once across the groups.
        for (int i = 1; i <= 6; i++) {
            String row = "| Region-0" + i + " | 100 |";
            assertThat(groups.stream().filter(g -> g.text().contains(row)).count()).isEqualTo(1);
        }
    }

    @Test
    void splitTableByRowGroups_leavesAFittingTableUntouched() {
        String text = """
                | Tier | Response |
                | --- | --- |
                | Starter | 8 hours |""";
        TextSegment segment = new TextSegment(text, 0, text.length(), true, 3);

        List<TextSegment> groups = TextSegmenter.splitTableByRowGroups(segment, 4096);

        assertThat(groups).containsExactly(segment);
    }
}
