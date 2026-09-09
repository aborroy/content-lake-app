package org.hyland.contentlake.extractor;

import org.hyland.contentlake.spi.TextExtractor;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ExtractionChainTest {

    @Test
    void noEnginesConfiguredLeavesJustTheFallbacks() {
        assertThat(ExtractionChain.engines(null, 1000L, ExtractionFormat.AUTO)).isEmpty();
        assertThat(ExtractionChain.engines("", 1000L, ExtractionFormat.AUTO)).isEmpty();
        assertThat(ExtractionChain.engines("   ", 1000L, ExtractionFormat.AUTO)).isEmpty();
    }

    @Test
    void urlsAreParsedInOrder() {
        assertThat(ExtractionChain.urls("http://a:8090,http://b:8090"))
                .containsExactly("http://a:8090", "http://b:8090");
    }

    @Test
    void whitespaceAndBlankEntriesAreTolerated() {
        assertThat(ExtractionChain.urls("  http://a:8090 ,, http://b:8090  ,  "))
                .containsExactly("http://a:8090", "http://b:8090");
        assertThat(ExtractionChain.urls("http://a:8090 http://b:8090"))
                .containsExactly("http://a:8090", "http://b:8090");
    }

    /** A URL that is also the source's own transform service must not produce two identical hops. */
    @Test
    void duplicatesCollapseAndOrderIsPreserved() {
        assertThat(ExtractionChain.urls("http://a:8090,http://b:8090,http://a:8090"))
                .containsExactly("http://a:8090", "http://b:8090");
    }

    @Test
    void aTrailingSlashIsNotADifferentEngine() {
        assertThat(ExtractionChain.urls("http://a:8090/,http://a:8090"))
                .containsExactly("http://a:8090");
    }

    @Test
    void oneExtractorIsBuiltPerDistinctUrl() {
        List<TextExtractor> engines =
                ExtractionChain.engines("http://a:8090,http://b:8090", 1000L, ExtractionFormat.AUTO);

        assertThat(engines).hasSize(2);
        assertThat(engines).allMatch(e -> e instanceof TransformEngineTextExtractor);
    }

    @Test
    void theChainPutsEnginesBeforeTheFallbacks() {
        TextExtractor tika = new TikaTextExtractor();

        TextExtractor chain = ExtractionChain.of(
                "http://a:8090,http://b:8090", 1000L, ExtractionFormat.AUTO, tika);

        assertThat(chain).isInstanceOf(ChainingTextExtractor.class);
        // text/plain reaches the fallback: the engines are unreachable here, so a null-returning
        // engine must not stop Tika from handling it.
        assertThat(chain.supports("text/plain")).isTrue();
    }

    @Test
    void withNoEnginesTheChainIsJustTheFallbacks() {
        TextExtractor chain = ExtractionChain.of("", 1000L, ExtractionFormat.PLAINTEXT,
                new TikaTextExtractor());

        assertThat(chain).isInstanceOf(ChainingTextExtractor.class);
        assertThat(chain.supports("text/plain")).isTrue();
    }

    @Test
    void atLeastOneFallbackIsRequired() {
        assertThatThrownBy(() -> ExtractionChain.of("http://a:8090", 1000L, ExtractionFormat.AUTO))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> ExtractionChain.of("http://a:8090", 1000L, ExtractionFormat.AUTO,
                (TextExtractor[]) null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    // ── backend selection, so a non transform-core service can be plugged in ──

    @Test
    void anUnprefixedEntryDefaultsToTheTransformCoreBackend() {
        assertThat(ExtractionChain.entries("http://a:8090"))
                .containsExactly(new ExtractionChain.Entry("transform", "http://a:8090"));
    }

    @Test
    void aUrlSchemeIsNotMistakenForABackendKind() {
        assertThat(ExtractionChain.entries("https://a.example.com/x"))
                .containsExactly(new ExtractionChain.Entry("transform", "https://a.example.com/x"));
    }

    @Test
    void aKindPrefixSelectsTheBackend() {
        assertThat(ExtractionChain.entries("transform:http://a:8090,docfilters:http://b:8080"))
                .containsExactly(
                        new ExtractionChain.Entry("transform", "http://a:8090"),
                        new ExtractionChain.Entry("docfilters", "http://b:8080"));
    }

    @Test
    void kindsAreCaseInsensitive() {
        assertThat(ExtractionChain.entries("DocFilters:http://b:8080"))
                .containsExactly(new ExtractionChain.Entry("docfilters", "http://b:8080"));
    }

    /**
     * An unknown kind is skipped with a warning rather than failing the context: one bad entry must not
     * stop the other services, nor stop the ingester from starting.
     */
    @Test
    void anUnknownKindIsSkippedRatherThanFailing() {
        List<TextExtractor> engines = ExtractionChain.engines(
                "nosuchbackend:http://a:8090,http://b:8090", 1000L, ExtractionFormat.AUTO);

        assertThat(engines).hasSize(1);
    }

    @Test
    void aCustomBackendIsUsedForItsOwnKind() {
        ExtractionBackend docfilters = new ExtractionBackend() {
            @Override public String kind() { return "docfilters"; }
            @Override public TextExtractor create(String url, long timeoutMs, ExtractionFormat format) {
                return new TikaTextExtractor();   // stand-in for a real client
            }
        };

        List<TextExtractor> engines = ExtractionChain.engines(
                "docfilters:http://b:8080,http://a:8090", 1000L, ExtractionFormat.AUTO,
                List.of(docfilters));

        assertThat(engines).hasSize(2);
        assertThat(engines.get(0)).isInstanceOf(TikaTextExtractor.class);
        assertThat(engines.get(1)).isInstanceOf(TransformEngineTextExtractor.class);
    }

    @Test
    void aCustomBackendCanOverrideABuiltInKind() {
        ExtractionBackend replacement = new ExtractionBackend() {
            @Override public String kind() { return "transform"; }
            @Override public TextExtractor create(String url, long timeoutMs, ExtractionFormat format) {
                return new TikaTextExtractor();
            }
        };

        List<TextExtractor> engines = ExtractionChain.engines(
                "http://a:8090", 1000L, ExtractionFormat.AUTO, List.of(replacement));

        assertThat(engines).singleElement().isInstanceOf(TikaTextExtractor.class);
    }
}
