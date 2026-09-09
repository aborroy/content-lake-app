package org.hyland.contentlake.extractor;

import org.hyland.contentlake.spi.ExtractedText;
import org.hyland.contentlake.spi.TextExtractor;
import org.hyland.contentlake.spi.TextFormat;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.core.io.Resource;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ChainingTextExtractorTest {

    private static final String PDF = "application/pdf";
    private static final Resource CONTENT =
            new ByteArrayResource("bytes".getBytes(StandardCharsets.UTF_8));

    @Test
    void usesTheFirstExtractorThatProducesText() {
        Recording first = new Recording("first", ExtractedText.markdown("# structured"));
        Recording second = new Recording("second", ExtractedText.plain("flattened"));

        ExtractedText extracted = new ChainingTextExtractor(first, second).extract(CONTENT, PDF);

        assertThat(extracted.text()).isEqualTo("# structured");
        assertThat(extracted.format()).isEqualTo(TextFormat.MARKDOWN);
        assertThat(first.calls).isEqualTo(1);
        assertThat(second.calls).isZero();
    }

    @Test
    void fallsThroughWhenAnExtractorReturnsNull() {
        Recording engine = new Recording("engine", null);
        Recording tika = new Recording("tika", ExtractedText.plain("flattened"));

        ExtractedText extracted = new ChainingTextExtractor(engine, tika).extract(CONTENT, PDF);

        assertThat(extracted.text()).isEqualTo("flattened");
        assertThat(engine.calls).isEqualTo(1);
        assertThat(tika.calls).isEqualTo(1);
    }

    @Test
    void fallsThroughWhenAnExtractorReturnsBlankText() {
        Recording engine = new Recording("engine", ExtractedText.plain("   "));
        Recording tika = new Recording("tika", ExtractedText.plain("flattened"));

        assertThat(new ChainingTextExtractor(engine, tika).extract(CONTENT, PDF).text())
                .isEqualTo("flattened");
    }

    /** An engine that throws must not fail the ingest; that is the whole point of the chain. */
    @Test
    void fallsThroughWhenAnExtractorThrows() {
        TextExtractor exploding = new Recording("exploding", null) {
            @Override
            public ExtractedText extract(Resource content, String mimeType) {
                throw new IllegalStateException("connection reset");
            }
        };
        Recording tika = new Recording("tika", ExtractedText.plain("flattened"));

        assertThat(new ChainingTextExtractor(exploding, tika).extract(CONTENT, PDF).text())
                .isEqualTo("flattened");
    }

    @Test
    void returnsNullWhenEveryExtractorFails() {
        Recording first = new Recording("first", null);
        Recording second = new Recording("second", null);

        assertThat(new ChainingTextExtractor(first, second).extract(CONTENT, PDF)).isNull();
    }

    @Test
    void skipsExtractorsThatDoNotSupportTheMimeType() {
        Recording unsupported = new Recording("unsupported", ExtractedText.plain("never used"), false, false);
        Recording tika = new Recording("tika", ExtractedText.plain("flattened"));

        assertThat(new ChainingTextExtractor(unsupported, tika).extract(CONTENT, PDF).text())
                .isEqualTo("flattened");
        assertThat(unsupported.calls).isZero();
    }

    @Test
    void supportsIsTrueWhenAnyDelegateSupportsTheType() {
        Recording no = new Recording("no", null, false, false);
        Recording yes = new Recording("yes", ExtractedText.plain("x"));

        assertThat(new ChainingTextExtractor(no, yes).supports(PDF)).isTrue();
        assertThat(new ChainingTextExtractor(no, no).supports(PDF)).isFalse();
    }

    /**
     * The pipeline decides between the node-id and temp-file styles before any extractor runs, so the
     * answer has to describe the delegate the chain will reach first, not any delegate.
     */
    @Test
    void supportsSourceReferenceFollowsTheFirstClaimingDelegate() {
        Recording engineNeedsResource = new Recording("engine", ExtractedText.plain("x"), true, false);
        Recording nuxeoUsesNodeId = new Recording("nuxeo", ExtractedText.plain("x"), true, true);

        assertThat(new ChainingTextExtractor(engineNeedsResource, nuxeoUsesNodeId)
                .supportsSourceReference(PDF)).isFalse();
        assertThat(new ChainingTextExtractor(nuxeoUsesNodeId, engineNeedsResource)
                .supportsSourceReference(PDF)).isTrue();
    }

    @Test
    void preferredFormatFollowsTheFirstClaimingDelegate() {
        Recording markdown = new Recording("markdown", ExtractedText.markdown("# x"));
        Recording plain = new Recording("plain", ExtractedText.plain("x"));

        assertThat(new ChainingTextExtractor(markdown, plain).preferredFormat(PDF))
                .isEqualTo(TextFormat.MARKDOWN);
        assertThat(new ChainingTextExtractor(plain, markdown).preferredFormat(PDF))
                .isEqualTo(TextFormat.PLAIN);
    }

    @Test
    void sourceReferenceExtractionOnlyReachesDelegatesThatSupportIt() {
        Recording resourceOnly = new Recording("resourceOnly", ExtractedText.plain("never"), true, false);
        Recording nodeIdCapable = new Recording("nodeIdCapable", ExtractedText.plain("by node id"), true, true);

        ExtractedText extracted =
                new ChainingTextExtractor(resourceOnly, nodeIdCapable).extract("node-1", PDF);

        assertThat(extracted.text()).isEqualTo("by node id");
        assertThat(resourceOnly.calls).isZero();
    }

    @Test
    void extractTextUnwrapsBothOverloads() {
        Recording only = new Recording("only", ExtractedText.plain("text"), true, true);
        ChainingTextExtractor chain = new ChainingTextExtractor(only);

        assertThat(chain.extractText(CONTENT, PDF)).isEqualTo("text");
        assertThat(chain.extractText("node-1", PDF)).isEqualTo("text");
    }

    @Test
    void aDelegateThrowingFromSupportsIsSkippedRatherThanPropagated() {
        TextExtractor exploding = new Recording("exploding", null) {
            @Override
            public boolean supports(String mimeType) {
                throw new IllegalStateException("engine config unreachable");
            }
        };
        Recording tika = new Recording("tika", ExtractedText.plain("flattened"));
        ChainingTextExtractor chain = new ChainingTextExtractor(exploding, tika);

        assertThat(chain.supports(PDF)).isTrue();
        assertThat(chain.extract(CONTENT, PDF).text()).isEqualTo("flattened");
    }

    @Test
    void rejectsAnEmptyOrNullChain() {
        assertThatThrownBy(() -> new ChainingTextExtractor(List.of()))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new ChainingTextExtractor((List<TextExtractor>) null))
                .isInstanceOf(IllegalArgumentException.class);

        List<TextExtractor> withNull = new ArrayList<>();
        withNull.add(null);
        assertThatThrownBy(() -> new ChainingTextExtractor(withNull))
                .isInstanceOf(IllegalArgumentException.class);
    }

    // ──────────────────────────────────────────────────────────────────────

    /** A delegate that records whether it was called and returns a fixed result. */
    private static class Recording implements TextExtractor {
        private final String name;
        private final ExtractedText result;
        private final boolean supports;
        private final boolean sourceReference;
        private int calls;

        Recording(String name, ExtractedText result) {
            this(name, result, true, false);
        }

        Recording(String name, ExtractedText result, boolean supports, boolean sourceReference) {
            this.name = name;
            this.result = result;
            this.supports = supports;
            this.sourceReference = sourceReference;
        }

        @Override
        public boolean supports(String mimeType) {
            return supports;
        }

        @Override
        public boolean supportsSourceReference(String mimeType) {
            return sourceReference;
        }

        @Override
        public TextFormat preferredFormat(String mimeType) {
            return result == null ? TextFormat.PLAIN : result.format();
        }

        @Override
        public String extractText(Resource content, String mimeType) {
            ExtractedText extracted = extract(content, mimeType);
            return extracted == null ? null : extracted.text();
        }

        @Override
        public ExtractedText extract(Resource content, String mimeType) {
            calls++;
            return result;
        }

        @Override
        public ExtractedText extract(String nodeId, String mimeType) {
            calls++;
            return result;
        }

        @Override
        public String toString() {
            return name;
        }
    }
}
