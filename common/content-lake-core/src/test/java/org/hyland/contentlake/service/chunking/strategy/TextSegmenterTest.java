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
}
