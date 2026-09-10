package org.hyland.contentlake.client;

import lombok.extern.slf4j.Slf4j;
import org.hyland.contentlake.hxpr.api.model.Embedding;
import org.hyland.contentlake.hxpr.api.model.VectorSearchResult;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.function.Supplier;

/**
 * The embedding types actually present in the index.
 *
 * <p>Discovered rather than derived from configuration, because a corpus can hold vectors written by a
 * model that is no longer the configured one, and those are exactly the types a multi-type query must
 * not miss.</p>
 *
 * <p>Types are read from {@code sysembed_type} on the embedding rows themselves, which is the field a
 * type-restricted {@link org.hyland.contentlake.hxpr.api.model.VectorQuery} matches on. Reading anything
 * else is a correctness trap: the {@code SysEmbeddings} child document's {@code sys_name} is
 * {@code _e_} plus the sanitized derivation, so a name-based scan can only ever return derived-form
 * types, while a corpus written before the two were reconciled carries the raw model name
 * ({@code ai/mxbai-embed-large}) in the row. Discovering {@code ai-mxbai-embed-large} from the name and
 * then querying it would match none of those rows, and the legacy half of the corpus would leave
 * retrieval silently at the moment multi-type querying activated. Reading the rows makes every
 * discovered type one that provably matches rows, because a row carrying it is how it was found.</p>
 *
 * <p>There is no aggregation endpoint over the embeddings index ({@code termsAggregation} covers the
 * document index only), so the scan is a paged wildcard vector query. Two measured properties of that
 * endpoint shape it. Rows come back nearest-first to an arbitrary probe vector and the ordering is not
 * perfectly stable between calls, so consecutive pages can overlap slightly; harmless here, because the
 * scan accumulates a set of types rather than a list of rows. And {@code totalCount} saturates at
 * {@code limit + offset} rather than reporting the index total, so it cannot say how much is left: the
 * only end-of-scan signal is a page coming back shorter than requested. A scan that instead stops at
 * {@link #MAX_ROWS} has sampled, not enumerated, and says so in the log -- a type holding a small share
 * of a large corpus can be missed, and a type nobody discovers is a type nobody queries.</p>
 *
 * <p>The result is cached for a TTL because it is a property of the whole corpus rather than of a
 * request, and it changes only when a model is introduced or retired.</p>
 */
@Slf4j
public class EmbeddingTypeCatalog {

    /** Rows per page of the scan. */
    private static final int PAGE_SIZE = 200;

    /** Ceiling on the scan, so discovery costs a bounded number of calls on any corpus size. */
    private static final int MAX_ROWS = 10_000;

    private final HxprService hxprService;
    private final Supplier<List<Double>> probeVectorSupplier;
    private final String configuredType;
    private final boolean discoveryEnabled;
    private final Duration ttl;
    private final Clock clock;

    private volatile List<String> cached;
    private volatile Instant cachedAt;

    /** Lazily obtained and then reused; the probe never changes for a given model. */
    private volatile List<Double> probeVector;

    /**
     * @param probeVectorSupplier supplies a vector of the index's dimensionality. Its direction is
     *                            immaterial: the scan reads each row's type and discards the scores.
     *                            Taken as a supplier rather than an {@code EmbeddingService} so this
     *                            package does not depend on the service layer.
     */
    public EmbeddingTypeCatalog(HxprService hxprService,
                                Supplier<List<Double>> probeVectorSupplier,
                                String configuredType,
                                boolean discoveryEnabled,
                                Duration ttl,
                                Clock clock) {
        this.hxprService = hxprService;
        this.probeVectorSupplier = probeVectorSupplier;
        this.configuredType = configuredType;
        this.discoveryEnabled = discoveryEnabled;
        this.ttl = ttl;
        this.clock = clock;
    }

    /**
     * The embedding types to query, most recently discovered first read and then from cache.
     *
     * <p>Never empty: a corpus with no embedding rows at all, a disabled discovery, or a failed scan all
     * fall back to the configured type. Falling back rather than returning nothing is what keeps a search
     * working on an index the catalogue cannot read.</p>
     */
    public List<String> activeTypes() {
        if (!discoveryEnabled) {
            return configuredOnly();
        }

        List<String> current = cached;
        if (current != null && !isExpired()) {
            return current;
        }

        List<String> discovered = discover();
        if (discovered.isEmpty()) {
            // Not cached: an empty result on a corpus that is still being ingested must not be held
            // for the whole TTL, or the first documents written would be unqueryable by type.
            return configuredOnly();
        }

        cached = discovered;
        cachedAt = clock.instant();
        return discovered;
    }

    /** Discards the cached types, so the next call re-reads the index. */
    public void invalidate() {
        cached = null;
        cachedAt = null;
    }

    private boolean isExpired() {
        Instant at = cachedAt;
        return at == null || Duration.between(at, clock.instant()).compareTo(ttl) >= 0;
    }

    private List<String> configuredOnly() {
        return (configuredType == null || configuredType.isBlank()) ? List.of() : List.of(configuredType);
    }

    private List<String> discover() {
        List<Double> probe = probeVector();
        if (probe == null || probe.isEmpty()) {
            log.warn("Cannot discover embedding types without a probe vector, falling back to the "
                    + "configured type '{}'", configuredType);
            return List.of();
        }

        LinkedHashSet<String> types = new LinkedHashSet<>();
        int examined = 0;
        boolean reachedEnd = false;
        try {
            while (examined < MAX_ROWS) {
                int limit = Math.min(PAGE_SIZE, MAX_ROWS - examined);
                // Null embeddingType is the '*' wildcard, which is the whole point: the scan must see
                // rows of every type, including one written by a model nothing is configured for.
                VectorSearchResult result = hxprService.vectorSearch(probe, null, null, null, limit, examined);
                List<Embedding> rows = result == null ? null : result.getEmbeddings();
                if (rows == null || rows.isEmpty()) {
                    reachedEnd = true;
                    break;
                }
                for (Embedding row : rows) {
                    String type = row.getSysembedType();
                    if (type != null && !type.isBlank()) {
                        types.add(type);
                    }
                }
                examined += rows.size();
                if (rows.size() < limit) {
                    reachedEnd = true;
                    break;
                }
            }
        } catch (Exception e) {
            log.warn("Failed to discover embedding types, falling back to the configured type '{}': {}",
                    configuredType, e.getMessage());
            return List.of();
        }

        if (types.isEmpty()) {
            // Distinguished from a failure on purpose: an empty corpus is normal before the first
            // ingest, whereas rows that carry no type at all means something wrote them wrongly.
            if (examined == 0) {
                log.debug("No embedding rows in the corpus yet; querying the configured type '{}'",
                        configuredType);
            } else {
                log.warn("Scanned {} embedding rows and none carried a sysembed_type, falling back to "
                        + "the configured type '{}'", examined, configuredType);
            }
            return List.of();
        }

        // The configured type leads: it is the one the current model writes, so it is the one a
        // single-type corpus is querying and the one whose query vector is already in hand.
        List<String> ordered = new ArrayList<>();
        if (configuredType != null && !configuredType.isBlank() && types.remove(configuredType)) {
            ordered.add(configuredType);
        }
        ordered.addAll(types);

        if (ordered.size() > 1) {
            log.info("Corpus holds {} embedding types: {} (read from {} embedding rows, {})",
                    ordered.size(), ordered, examined, reachedEnd ? "the whole index" : "sampled");
        } else {
            log.debug("Corpus holds one embedding type: {} (read from {} embedding rows, {})",
                    ordered, examined, reachedEnd ? "the whole index" : "sampled");
        }
        if (!reachedEnd) {
            // Worth saying out loud: the scan stopped at its ceiling, and it is nearest-first over an
            // arbitrary probe, so a type holding a small share of the corpus can be absent from the
            // sample. A type nobody discovers is a type nobody queries.
            log.info("Embedding-type discovery stopped at its {}-row ceiling, so a rare type may be "
                            + "missing from {}. Pin the types with rag.embedding.additional-models if "
                            + "that matters here.",
                    MAX_ROWS, ordered);
        }
        return List.copyOf(ordered);
    }

    private List<Double> probeVector() {
        List<Double> current = probeVector;
        if (current == null) {
            synchronized (this) {
                current = probeVector;
                if (current == null) {
                    try {
                        current = probeVectorSupplier.get();
                    } catch (Exception e) {
                        log.warn("Probe embedding for type discovery failed: {}", e.getMessage());
                        return null;
                    }
                    probeVector = current;
                }
            }
        }
        return current;
    }
}
