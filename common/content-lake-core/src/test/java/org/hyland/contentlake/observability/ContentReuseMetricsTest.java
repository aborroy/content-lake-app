package org.hyland.contentlake.observability;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.hyland.contentlake.service.NodeSyncService;
import org.hyland.contentlake.spi.ContentSourceClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.NoUniqueBeanDefinitionException;
import org.springframework.beans.factory.ObjectProvider;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * The content-reuse saving has to be readable without grepping a log (#131), and reading it must not
 * depend on the sync path doing anything extra.
 */
class ContentReuseMetricsTest {

    private final MeterRegistry registry = new SimpleMeterRegistry();
    private final NodeSyncService syncService = mock(NodeSyncService.class);

    @BeforeEach
    void setUp() {
        when(syncService.getContentReuseStats())
                .thenReturn(new NodeSyncService.ContentReuseStats(26, 41));
    }

    private void bind(MeterRegistry meterRegistry, NodeSyncService service, ContentSourceClient client) {
        new ContentReuseMetrics(provider(meterRegistry), provider(service), provider(client))
                .afterSingletonsInstantiated();
    }

    @Test
    void publishesBothCountersWithTheSourceTag() {
        bind(registry, syncService, source("nuxeo"));

        assertThat(registry.get(ContentReuseMetrics.SHORT_CIRCUITS)
                .tag(ContentReuseMetrics.SOURCE_TAG, "nuxeo")
                .functionCounter().count()).isEqualTo(26d);
        assertThat(registry.get(ContentReuseMetrics.REPROCESSES)
                .tag(ContentReuseMetrics.SOURCE_TAG, "nuxeo")
                .functionCounter().count()).isEqualTo(41d);
    }

    /**
     * The counters read the service's own state, so a sync that happens after binding is reflected without
     * the sync path touching a meter. That is also what makes the numbers survive for the process's life.
     */
    @Test
    void theCountersTrackLaterSyncs() {
        bind(registry, syncService, source("alfresco"));
        when(syncService.getContentReuseStats())
                .thenReturn(new NodeSyncService.ContentReuseStats(30, 45));

        assertThat(registry.get(ContentReuseMetrics.SHORT_CIRCUITS).functionCounter().count())
                .isEqualTo(30d);
        assertThat(registry.get(ContentReuseMetrics.REPROCESSES).functionCounter().count())
                .isEqualTo(45d);
    }

    @Test
    void bothCountersDescribeThemselvesInDocuments() {
        bind(registry, syncService, source("filesystem"));

        assertThat(registry.get(ContentReuseMetrics.SHORT_CIRCUITS).functionCounter().getId().getBaseUnit())
                .isEqualTo("documents");
        assertThat(registry.get(ContentReuseMetrics.SHORT_CIRCUITS).functionCounter().getId().getDescription())
                .isNotBlank();
    }

    /** The RAG service has no sync service, so there is nothing to publish and nothing to fail. */
    @Test
    void anApplicationWithoutASyncServiceRegistersNothing() {
        bind(registry, null, source("nuxeo"));

        assertThat(registry.getMeters()).isEmpty();
    }

    @Test
    void withoutAMeterRegistryNothingIsRegisteredAndStartupIsUnaffected() {
        assertThatCode(() -> bind(null, syncService, source("nuxeo"))).doesNotThrowAnyException();
    }

    @Test
    void anUnresolvableSourceStillPublishesTheCounters() {
        bind(registry, syncService, null);

        assertThat(registry.get(ContentReuseMetrics.SHORT_CIRCUITS)
                .tag(ContentReuseMetrics.SOURCE_TAG, "unknown")
                .functionCounter().count()).isEqualTo(26d);
    }

    /** Two source clients make the tag ambiguous; the metric matters more than the tag. */
    @Test
    void anAmbiguousSourceLookupDoesNotPreventPublishing() {
        ObjectProvider<ContentSourceClient> ambiguous = new StubProvider<>(null) {
            @Override
            public ContentSourceClient getIfAvailable() {
                throw new NoUniqueBeanDefinitionException(ContentSourceClient.class, 2, "two connectors");
            }
        };

        new ContentReuseMetrics(provider(registry), provider(syncService), ambiguous)
                .afterSingletonsInstantiated();

        assertThat(registry.get(ContentReuseMetrics.SHORT_CIRCUITS)
                .tag(ContentReuseMetrics.SOURCE_TAG, "unknown")
                .functionCounter().count()).isEqualTo(26d);
    }

    private static ContentSourceClient source(String sourceType) {
        ContentSourceClient client = mock(ContentSourceClient.class);
        when(client.getSourceType()).thenReturn(sourceType);
        return client;
    }

    private static <T> ObjectProvider<T> provider(T value) {
        return new StubProvider<>(value);
    }

    /** Only the {@code getIfAvailable} half of {@link ObjectProvider}, which is all the binder uses. */
    private static class StubProvider<T> implements ObjectProvider<T> {

        private final T value;

        StubProvider(T value) {
            this.value = value;
        }

        @Override
        public T getIfAvailable() {
            return value;
        }

        @Override
        public T getObject() {
            if (value == null) {
                throw new UnsupportedOperationException();
            }
            return value;
        }

        @Override
        public T getObject(Object... args) {
            return getObject();
        }

        @Override
        public T getIfUnique() {
            return value;
        }
    }
}
