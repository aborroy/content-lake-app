package org.hyland.contentlake.service;

import org.hyland.contentlake.service.chunking.strategy.ChunkingStrategy.ChunkingConfig;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ContentFingerprintTest {

    private static final ChunkingConfig CONFIG = new ChunkingConfig(200, 1024, 256, 0.75);
    private static final String TYPE = "ai-mxbai-embed-large";
    private static final String TEXT = "Change CHG-105402 was reverted after the rollback failed.";

    private static String fingerprint(String text, String embeddingType,
                                      ChunkingConfig config, boolean keywordContext) {
        return ContentFingerprint.of(text, embeddingType, config, keywordContext);
    }

    @Test
    void isStableForIdenticalInput() {
        assertThat(fingerprint(TEXT, TYPE, CONFIG, false))
                .isEqualTo(fingerprint(TEXT, TYPE, CONFIG, false));
    }

    @Test
    void carriesItsAlgorithmAsAPrefix() {
        assertThat(fingerprint(TEXT, TYPE, CONFIG, false))
                .startsWith("sha256:")
                .hasSize("sha256:".length() + 64);
    }

    @Test
    void changesWhenTheTextChanges() {
        assertThat(fingerprint(TEXT + " ", TYPE, CONFIG, false))
                .isNotEqualTo(fingerprint(TEXT, TYPE, CONFIG, false));
    }

    /**
     * The defect a text-only hash would introduce: switching the embedding model would leave every
     * document matching its stored fingerprint, so nothing would be re-embedded and the corpus would
     * keep serving the retired model's vectors.
     */
    @Test
    void changesWhenTheEmbeddingTypeChanges() {
        assertThat(fingerprint(TEXT, "ai-nomic-embed-text", CONFIG, false))
                .isNotEqualTo(fingerprint(TEXT, TYPE, CONFIG, false));
    }

    @Test
    void changesWhenAnyChunkingParameterChanges() {
        String base = fingerprint(TEXT, TYPE, CONFIG, false);

        assertThat(fingerprint(TEXT, TYPE, new ChunkingConfig(100, 1024, 256, 0.75), false)).isNotEqualTo(base);
        assertThat(fingerprint(TEXT, TYPE, new ChunkingConfig(200, 512, 256, 0.75), false)).isNotEqualTo(base);
        assertThat(fingerprint(TEXT, TYPE, new ChunkingConfig(200, 1024, 150, 0.75), false)).isNotEqualTo(base);
        assertThat(fingerprint(TEXT, TYPE, new ChunkingConfig(200, 1024, 256, 0.80), false)).isNotEqualTo(base);
    }

    @Test
    void changesWhenKeywordContextEnrichmentIsToggled() {
        assertThat(fingerprint(TEXT, TYPE, CONFIG, true))
                .isNotEqualTo(fingerprint(TEXT, TYPE, CONFIG, false));
    }

    /**
     * The fields are separated, so moving a boundary between two of them has to change the result.
     * Without a separator, type {@code "a"} with text {@code "bc"} and type {@code "ab"} with text
     * {@code "c"} would collide.
     */
    @Test
    void doesNotCollideWhenAFieldBoundaryMoves() {
        assertThat(fingerprint("bc", "a", CONFIG, false))
                .isNotEqualTo(fingerprint("c", "ab", CONFIG, false));
    }

    @Test
    void toleratesAMissingEmbeddingTypeOrChunkingConfig() {
        assertThat(fingerprint(TEXT, null, null, false)).startsWith("sha256:");
    }

    @Test
    void rejectsNullText() {
        assertThatThrownBy(() -> fingerprint(null, TYPE, CONFIG, false))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("null extracted text");
    }

    @Test
    void aStoredFingerprintMatchesOnlyItself() {
        String computed = fingerprint(TEXT, TYPE, CONFIG, false);

        assertThat(ContentFingerprint.matches(computed, computed)).isTrue();
        assertThat(ContentFingerprint.matches("sha256:other", computed)).isFalse();
    }

    /** A document with no stored fingerprint has to be reprocessed to acquire one. */
    @Test
    void anAbsentStoredFingerprintNeverMatches() {
        String computed = fingerprint(TEXT, TYPE, CONFIG, false);

        assertThat(ContentFingerprint.matches(null, computed)).isFalse();
        assertThat(ContentFingerprint.matches("   ", computed)).isFalse();
    }
}
