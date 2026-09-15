package org.hyland.contentlake.client;

import lombok.extern.slf4j.Slf4j;
import org.hyland.contentlake.hxpr.api.model.Embedding;
import org.hyland.contentlake.hxpr.api.model.VectorSearchResult;
import org.hyland.contentlake.model.HxprTermsAggregationResult;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
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
 * only end-of-scan signal is a page coming back shorter than requested. A scan that instead stops at its
 * configured ceiling ({@code rag.embedding.type-discovery.max-scan-rows}, {@link #DEFAULT_MAX_ROWS} by
 * default, non-positive for no ceiling) has sampled, not enumerated, and says so at WARN -- a type
 * holding a small share of a large corpus can be missed, and a type nobody discovers is a type nobody
 * queries.</p>
 *
 * <p>The sampling gap is then narrowed from the other side (#130). {@code SysEmbeddings} child names
 * <em>are</em> enumerable, in one terms aggregation over {@code sys_name}, so the set is widened with any
 * type a child name implies and the rows did not reach. That is a widening and never a substitute,
 * because of the same derivation trap above: each candidate is confirmed with one type-restricted probe
 * and dropped unless rows actually carry it, so every type queried is still one that provably matches
 * rows. What remains uncovered is a type whose rows exist while its child document does not, which is
 * what pinning {@code rag.embedding.additional-models} is for.</p>
 *
 * <p>The result is cached for a TTL because it is a property of the whole corpus rather than of a
 * request, and it changes only when a model is introduced or retired.</p>
 */
@Slf4j
public class EmbeddingTypeCatalog {

    /** Rows per page of the scan. */
    private static final int PAGE_SIZE = 200;

    /** Default ceiling on the scan, so discovery costs a bounded number of calls on any corpus size. */
    static final int DEFAULT_MAX_ROWS = 10_000;

    /** Name prefix of every {@code SysEmbeddings} child; what follows it is the sanitized type. */
    private static final String EMBEDDING_CHILD_PREFIX = "_e_";

    /** Child names are one bucket each, and a corpus holds one per embedding type ever written. */
    private static final int CHILD_NAME_BUCKETS = 100;

    /** Aggregating on the child documents' own name is what makes the candidate set enumerable. */
    private static final String CHILD_NAME_PROPERTY = "sys_name";
    private static final String CHILD_NAME_QUERY = "SELECT * FROM SysEmbeddings";

    private final HxprService hxprService;
    private final Supplier<List<Double>> probeVectorSupplier;
    private final String configuredType;
    private final boolean discoveryEnabled;
    private final Duration ttl;
    private final Clock clock;
    private final int maxRows;
    private final boolean deriveFromChildNames;

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
        this(hxprService, probeVectorSupplier, configuredType, discoveryEnabled, ttl, clock,
                DEFAULT_MAX_ROWS, true);
    }

    /**
     * @param maxRows              ceiling on the rows the scan reads; zero or less reads the whole index
     * @param deriveFromChildNames whether to widen the set with types derived from the
     *                             {@code SysEmbeddings} child names, each verified against the rows
     */
    public EmbeddingTypeCatalog(HxprService hxprService,
                                Supplier<List<Double>> probeVectorSupplier,
                                String configuredType,
                                boolean discoveryEnabled,
                                Duration ttl,
                                Clock clock,
                                int maxRows,
                                boolean deriveFromChildNames) {
        this.hxprService = hxprService;
        this.probeVectorSupplier = probeVectorSupplier;
        this.configuredType = configuredType;
        this.discoveryEnabled = discoveryEnabled;
        this.ttl = ttl;
        this.clock = clock;
        this.maxRows = maxRows;
        this.deriveFromChildNames = deriveFromChildNames;
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
        int ceiling = maxRows > 0 ? maxRows : Integer.MAX_VALUE;
        int examined = 0;
        boolean reachedEnd = false;
        try {
            while (examined < ceiling) {
                int limit = Math.min(PAGE_SIZE, ceiling - examined);
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

        // Widen with types the rows did not reach. Enumerable where the row scan is a sample, and
        // never trusted on its own: each candidate has to match rows before it is queried.
        List<String> fromChildNames = deriveFromChildNames ? widenFromChildNames(ordered, probe) : List.of();
        ordered.addAll(fromChildNames);

        if (ordered.size() > 1) {
            log.info("Corpus holds {} embedding types: {} (read from {} embedding rows, {}{})",
                    ordered.size(), ordered, examined, reachedEnd ? "the whole index" : "sampled",
                    fromChildNames.isEmpty() ? "" : "; " + fromChildNames + " recovered from child names");
        } else {
            log.debug("Corpus holds one embedding type: {} (read from {} embedding rows, {})",
                    ordered, examined, reachedEnd ? "the whole index" : "sampled");
        }
        if (!reachedEnd) {
            // Worth saying out loud, and at WARN: the scan stopped at its ceiling, and it is
            // nearest-first over an arbitrary probe, so a type holding a small share of the corpus can
            // be absent from the sample. A type nobody discovers is a type nobody queries. The child-name
            // pass below covers the common case, but only for types that still have a child document.
            log.warn("Embedding-type discovery stopped at its {}-row ceiling, so a rare type may be "
                            + "missing from {}. Raise rag.embedding.type-discovery.max-scan-rows, or pin "
                            + "the types with rag.embedding.additional-models.",
                    ceiling, ordered);
        }
        return List.copyOf(ordered);
    }

    /**
     * Types derived from the {@code SysEmbeddings} child names that the row scan did not find, each
     * confirmed to match rows.
     *
     * <p>Child names are enumerable in one terms aggregation over {@code sys_name}, so this sees the
     * whole corpus however far the row scan got. It is a widening rather than a source of truth because
     * a child name is always the sanitized derivation: a corpus written before the derivation and the
     * stored type were reconciled (#113) carries the raw model name in its rows, so
     * {@code _e_ai-mxbai-embed-large} can be the child of rows whose type is
     * {@code ai/mxbai-embed-large}. Querying a type no row carries would spend an embedding and a
     * search per request for nothing, so each candidate is verified with one type-restricted probe and
     * dropped unless rows come back.</p>
     */
    private List<String> widenFromChildNames(Collection<String> known, List<Double> probe) {
        List<String> candidates;
        try {
            HxprTermsAggregationResult aggregation = hxprService.termsAggregation(
                    CHILD_NAME_QUERY, CHILD_NAME_PROPERTY, null, CHILD_NAME_BUCKETS);
            List<HxprTermsAggregationResult.Bucket> buckets =
                    aggregation == null ? null : aggregation.getAggregationsBuckets();
            if (buckets == null || buckets.isEmpty()) {
                return List.of();
            }
            candidates = new ArrayList<>();
            for (HxprTermsAggregationResult.Bucket bucket : buckets) {
                String name = bucket.getKey();
                if (name == null || !name.startsWith(EMBEDDING_CHILD_PREFIX)) {
                    continue;
                }
                String type = name.substring(EMBEDDING_CHILD_PREFIX.length()).trim();
                if (!type.isEmpty() && !known.contains(type) && !candidates.contains(type)) {
                    candidates.add(type);
                }
            }
        } catch (Exception e) {
            log.warn("Could not read the embedding child names to widen type discovery: {}",
                    e.getMessage());
            return List.of();
        }

        if (candidates.isEmpty()) {
            return List.of();
        }

        List<String> confirmed = new ArrayList<>();
        for (String candidate : candidates) {
            if (hasRows(candidate, probe)) {
                confirmed.add(candidate);
            } else {
                log.info("Embedding child name suggests type '{}', but no embedding row carries it, so "
                        + "it is not queried. This is the sanitized-derivation case: the rows of that "
                        + "model are stored under their raw name.", candidate);
            }
        }
        return List.copyOf(confirmed);
    }

    /** Whether any row carries {@code type}, asked as one row of a type-restricted vector query. */
    private boolean hasRows(String type, List<Double> probe) {
        try {
            VectorSearchResult result = hxprService.vectorSearch(probe, type, null, null, 1, 0);
            List<Embedding> rows = result == null ? null : result.getEmbeddings();
            return rows != null && !rows.isEmpty();
        } catch (Exception e) {
            log.warn("Could not confirm embedding type '{}' against the rows: {}", type, e.getMessage());
            return false;
        }
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
