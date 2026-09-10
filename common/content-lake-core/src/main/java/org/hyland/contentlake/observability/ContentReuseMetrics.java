package org.hyland.contentlake.observability;

import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.hyland.contentlake.service.NodeSyncService;
import org.hyland.contentlake.spi.ContentSourceClient;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.stereotype.Component;

/**
 * Publishes the content-reuse counters as metrics, so the re-embedding a sync avoided is visible without
 * reading logs (#131).
 *
 * <p>Embedding is the pipeline's bottleneck, so "how much re-embedding did we avoid" is the number that
 * justifies the content fingerprint (#120). Until this, the only way to see it was a log line emitted per
 * short circuit, which also meant there was no signal if the short circuit silently stopped firing after
 * an unrelated change to the fingerprint inputs.</p>
 *
 * <ul>
 *   <li>{@code contentlake.ingest.content.shortcircuits} -- documents whose chunking and embedding were
 *       skipped because the content had not changed</li>
 *   <li>{@code contentlake.ingest.content.reprocesses} -- documents that were chunked and embedded</li>
 * </ul>
 *
 * <p>Both are {@link FunctionCounter}s reading {@link NodeSyncService#getContentReuseStats()} rather than
 * counters incremented alongside it. That keeps one source of truth, adds nothing to the sync path, and
 * reports whatever the service has counted for as long as the process lives. They are per-process and
 * start at zero on restart, as any counter does; a scrape-based backend handles the reset.</p>
 *
 * <p>Registered in <em>every</em> ingester, because core is component-scanned by all of them. That matters:
 * the short circuit fires far more on the live path than the batch one, since {@code isStale} skips
 * content entirely for an unchanged {@code source_modifiedAt} before the fingerprint is even reached, so a
 * metric covering only batch runs would read as zero and look broken. An application with no
 * {@link NodeSyncService} -- the RAG service -- registers nothing.</p>
 */
@Slf4j
@Component
@ConditionalOnClass(MeterRegistry.class)
public class ContentReuseMetrics implements SmartInitializingSingleton {

    static final String SHORT_CIRCUITS = "contentlake.ingest.content.shortcircuits";
    static final String REPROCESSES = "contentlake.ingest.content.reprocesses";

    /** Tag carrying the source type, so one dashboard can separate Alfresco from Nuxeo. */
    static final String SOURCE_TAG = "source";

    private static final String UNKNOWN_SOURCE = "unknown";
    private static final String UNIT = "documents";

    private final ObjectProvider<MeterRegistry> registries;
    private final ObjectProvider<NodeSyncService> syncServices;
    private final ObjectProvider<ContentSourceClient> sourceClients;

    public ContentReuseMetrics(ObjectProvider<MeterRegistry> registries,
                               ObjectProvider<NodeSyncService> syncServices,
                               ObjectProvider<ContentSourceClient> sourceClients) {
        this.registries = registries;
        this.syncServices = syncServices;
        this.sourceClients = sourceClients;
    }

    /**
     * Binds after the singletons exist, so asking for the sync service cannot force it into being early.
     */
    @Override
    public void afterSingletonsInstantiated() {
        MeterRegistry registry = registries.getIfAvailable();
        NodeSyncService syncService = syncServices.getIfAvailable();
        if (registry == null || syncService == null) {
            log.debug("Content-reuse metrics not registered: {}",
                    registry == null ? "no meter registry" : "no sync service in this application");
            return;
        }

        String source = sourceType();

        FunctionCounter.builder(SHORT_CIRCUITS, syncService,
                        service -> service.getContentReuseStats().shortCircuits())
                .description("Documents whose chunking and embedding were skipped because the content "
                        + "fingerprint was unchanged")
                .baseUnit(UNIT)
                .tag(SOURCE_TAG, source)
                .register(registry);

        FunctionCounter.builder(REPROCESSES, syncService,
                        service -> service.getContentReuseStats().reprocesses())
                .description("Documents that were chunked and embedded")
                .baseUnit(UNIT)
                .tag(SOURCE_TAG, source)
                .register(registry);

        log.info("Content-reuse metrics registered for source '{}': {} and {}",
                source, SHORT_CIRCUITS, REPROCESSES);
    }

    /**
     * The tag value, or {@code unknown}. Everything here is best effort: an application with two source
     * clients makes the lookup ambiguous, and a tag is not worth failing a startup over.
     */
    private String sourceType() {
        try {
            ContentSourceClient client = sourceClients.getIfAvailable();
            String sourceType = client == null ? null : client.getSourceType();
            return sourceType == null || sourceType.isBlank() ? UNKNOWN_SOURCE : sourceType;
        } catch (Exception e) {
            log.debug("Could not resolve the source type for the content-reuse metrics: {}", e.getMessage());
            return UNKNOWN_SOURCE;
        }
    }
}
