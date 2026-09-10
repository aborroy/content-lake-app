package org.hyland.contentlake.client;

import org.hyland.contentlake.hxpr.api.model.Embedding;
import org.hyland.contentlake.hxpr.api.model.VectorSearchResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Discovering which embedding types the corpus actually holds (#121).
 *
 * <p>Read from {@code sysembed_type} on the embedding rows, because that is the field a type-restricted
 * query matches on. The child document's {@code sys_name} is not a substitute: it is always the sanitized
 * derivation, so a name-based scan cannot see a row that recorded the raw model name, and would hand back
 * a type matching none of those rows.</p>
 */
@ExtendWith(MockitoExtension.class)
class EmbeddingTypeCatalogTest {

    private static final String CONFIGURED = "ai-mxbai-embed-large";
    private static final Duration TTL = Duration.ofSeconds(300);
    private static final List<Double> PROBE = List.of(0.1d, 0.2d, 0.3d);

    @Mock
    private HxprService hxprService;

    /** Advanced by the tests, so TTL expiry is exercised without waiting. */
    private MutableClock clock;

    @BeforeEach
    void setUp() {
        clock = new MutableClock(Instant.parse("2026-09-09T10:00:00Z"));
    }

    private EmbeddingTypeCatalog catalog(boolean discoveryEnabled) {
        return new EmbeddingTypeCatalog(hxprService, () -> PROBE, CONFIGURED, discoveryEnabled, TTL, clock);
    }

    /** One short page of rows, so the scan stops after a single call. */
    private void indexHolds(String... rowTypes) {
        when(hxprService.vectorSearch(any(), isNull(), isNull(), isNull(), anyInt(), anyInt()))
                .thenReturn(rows((long) rowTypes.length, rowTypes));
    }

    private static VectorSearchResult rows(Long totalCount, String... rowTypes) {
        List<Embedding> embeddings = new ArrayList<>();
        for (String type : rowTypes) {
            Embedding row = new Embedding();
            row.setSysembedType(type);
            embeddings.add(row);
        }
        VectorSearchResult result = new VectorSearchResult();
        result.setEmbeddings(embeddings);
        result.setTotalCount(totalCount);
        return result;
    }

    @Test
    void readsTheTypesFromTheEmbeddingRows() {
        indexHolds("ai-mxbai-embed-large", "ai-nomic-embed-text", "ai-mxbai-embed-large");

        assertThat(catalog(true).activeTypes())
                .containsExactly("ai-mxbai-embed-large", "ai-nomic-embed-text");
    }

    /**
     * The configured type leads, because it is the one the current model writes and therefore the one
     * whose query vector the caller already has in hand.
     */
    @Test
    void theConfiguredTypeIsListedFirst() {
        indexHolds("ai-nomic-embed-text", "ai-mxbai-embed-large");

        assertThat(catalog(true).activeTypes()).startsWith(CONFIGURED);
    }

    /**
     * The case a name-based scan cannot see. Rows written before the type and the child name were
     * reconciled carry the raw configured model, while the child is still named
     * {@code _e_ai-mxbai-embed-large}. Only the row's own type is queryable, so only the row's own type
     * may be reported.
     */
    @Test
    void findsATypeRecordedInALegacyForm() {
        indexHolds("ai/mxbai-embed-large");

        assertThat(catalog(true).activeTypes()).containsExactly("ai/mxbai-embed-large");
    }

    /**
     * A corpus caught mid-migration holds both forms, and both have to be reported: querying only the
     * derived one would drop every document still on the legacy rows, which is the failure this class
     * exists to prevent.
     */
    @Test
    void reportsBothFormsOfTheSameModelDuringAMigration() {
        indexHolds("ai/mxbai-embed-large", "ai-mxbai-embed-large");

        assertThat(catalog(true).activeTypes())
                .containsExactly("ai-mxbai-embed-large", "ai/mxbai-embed-large");
    }

    @Test
    void ignoresRowsCarryingNoType() {
        indexHolds("ai-mxbai-embed-large", null, "");

        assertThat(catalog(true).activeTypes()).containsExactly(CONFIGURED);
    }

    /** A full page means there may be more, so the scan asks for the next one. */
    @Test
    void pagesUntilAShortPageArrives() {
        String[] full = new String[200];
        Arrays.fill(full, CONFIGURED);
        when(hxprService.vectorSearch(any(), isNull(), isNull(), isNull(), anyInt(), eq(0)))
                .thenReturn(rows(250L, full));
        when(hxprService.vectorSearch(any(), isNull(), isNull(), isNull(), anyInt(), eq(200)))
                .thenReturn(rows(250L, "ai-nomic-embed-text"));

        assertThat(catalog(true).activeTypes())
                .containsExactly(CONFIGURED, "ai-nomic-embed-text");
    }

    @Test
    void servesFromCacheInsideTheTtl() {
        indexHolds("ai-mxbai-embed-large", "ai-nomic-embed-text");
        EmbeddingTypeCatalog catalog = catalog(true);

        catalog.activeTypes();
        clock.advance(Duration.ofSeconds(299));
        catalog.activeTypes();

        verify(hxprService, times(1)).vectorSearch(any(), isNull(), isNull(), isNull(), anyInt(), eq(0));
    }

    @Test
    void reReadsAfterTheTtlExpires() {
        indexHolds("ai-mxbai-embed-large", "ai-nomic-embed-text");
        EmbeddingTypeCatalog catalog = catalog(true);

        catalog.activeTypes();
        clock.advance(Duration.ofSeconds(301));
        catalog.activeTypes();

        verify(hxprService, times(2)).vectorSearch(any(), isNull(), isNull(), isNull(), anyInt(), eq(0));
    }

    @Test
    void invalidateForcesAReRead() {
        indexHolds("ai-mxbai-embed-large", "ai-nomic-embed-text");
        EmbeddingTypeCatalog catalog = catalog(true);

        catalog.activeTypes();
        catalog.invalidate();
        catalog.activeTypes();

        verify(hxprService, times(2)).vectorSearch(any(), isNull(), isNull(), isNull(), anyInt(), eq(0));
    }

    /** The probe is embedded once, not once per scan. */
    @Test
    void embedsTheProbeOnlyOnce() {
        indexHolds("ai-mxbai-embed-large", "ai-nomic-embed-text");
        int[] calls = {0};
        EmbeddingTypeCatalog catalog = new EmbeddingTypeCatalog(
                hxprService, () -> { calls[0]++; return PROBE; }, CONFIGURED, true, TTL, clock);

        catalog.activeTypes();
        catalog.invalidate();
        catalog.activeTypes();

        assertThat(calls[0]).isEqualTo(1);
    }

    @Test
    void withDiscoveryDisabledItNeverQueriesTheIndex() {
        EmbeddingTypeCatalog catalog = catalog(false);

        assertThat(catalog.activeTypes()).containsExactly(CONFIGURED);
        verify(hxprService, never())
                .vectorSearch(any(), any(), any(), any(), anyInt(), anyInt());
    }

    /** A search must keep working on an index the catalogue cannot read. */
    @Test
    void aFailedScanFallsBackToTheConfiguredType() {
        when(hxprService.vectorSearch(any(), isNull(), isNull(), isNull(), anyInt(), anyInt()))
                .thenThrow(new RuntimeException("hxpr said 500"));

        assertThat(catalog(true).activeTypes()).containsExactly(CONFIGURED);
    }

    /** No probe vector means no scan, and the configured type rather than an exception. */
    @Test
    void aFailedProbeEmbeddingFallsBackToTheConfiguredType() {
        EmbeddingTypeCatalog catalog = new EmbeddingTypeCatalog(
                hxprService,
                () -> { throw new IllegalStateException("model runner down"); },
                CONFIGURED, true, TTL, clock);

        assertThat(catalog.activeTypes()).containsExactly(CONFIGURED);
        verify(hxprService, never())
                .vectorSearch(any(), any(), any(), any(), anyInt(), anyInt());
    }

    /**
     * An empty result is not cached: on a corpus still being ingested it would otherwise be held for
     * the whole TTL, leaving the first documents written unqueryable by type.
     */
    @Test
    void anEmptyIndexFallsBackAndIsNotCached() {
        when(hxprService.vectorSearch(any(), isNull(), isNull(), isNull(), anyInt(), anyInt()))
                .thenReturn(rows(0L));
        EmbeddingTypeCatalog catalog = catalog(true);

        assertThat(catalog.activeTypes()).containsExactly(CONFIGURED);
        catalog.activeTypes();

        verify(hxprService, times(2)).vectorSearch(any(), isNull(), isNull(), isNull(), anyInt(), eq(0));
    }

    /** A clock a test can move, so TTL behaviour is asserted rather than waited for. */
    private static final class MutableClock extends Clock {

        private Instant now;

        private MutableClock(Instant now) {
            this.now = now;
        }

        void advance(Duration amount) {
            now = now.plus(amount);
        }

        @Override
        public Instant instant() {
            return now;
        }

        @Override
        public java.time.ZoneId getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(java.time.ZoneId zone) {
            return this;
        }
    }
}
