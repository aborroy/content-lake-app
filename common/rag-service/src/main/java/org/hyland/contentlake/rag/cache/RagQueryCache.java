package org.hyland.contentlake.rag.cache;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.cache.CaffeineCacheMetrics;
import lombok.extern.slf4j.Slf4j;
import org.hyland.contentlake.rag.config.RagProperties;
import org.hyland.contentlake.security.CallerAuthentication;
import org.hyland.contentlake.security.CallerIdentities;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.security.core.Authentication;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.List;
import java.util.Locale;
import java.util.function.Supplier;

/**
 * Semantic query-result cache (#72): two bounded, short-TTL Caffeine caches fronting the RAG
 * retrieval path.
 *
 * <ul>
 *   <li><b>Result cache</b> - full {@code SemanticSearchResponse}/{@code HybridSearchResponse}
 *       objects, keyed on the normalized query, its filters, and the caller's effective permission
 *       scope. Including the principal in the key is what keeps a cached answer from ever crossing
 *       ACL contexts. Group membership can change within the TTL window, which
 *       {@code rag.cache.ttl-seconds} deliberately bounds.</li>
 *   <li><b>Embedding cache</b> - query vectors, keyed on the embedding type and normalized query
 *       text only. Embeddings are a pure function of (model, text), so they carry no ACL and are
 *       safe to share across principals.</li>
 * </ul>
 *
 * <p>When {@code rag.cache.enabled=false} the caches are never created and every accessor is a
 * pass-through, so the retrieval path behaves exactly as it did before this feature existed. Cache
 * statistics are bound to Micrometer (visible under {@code /actuator/metrics} as
 * {@code cache.gets{cache=rag.query.*}}) when a {@link MeterRegistry} is present.</p>
 */
@Slf4j
@Component
public class RagQueryCache {

    /** Separator that cannot appear in a normalized query, so key segments never collide. */
    private static final String SEP = " ";

    private final boolean enabled;
    private final Cache<String, Object> resultCache;
    private final Cache<String, List<Double>> embeddingCache;

    public RagQueryCache(RagProperties ragProperties, ObjectProvider<MeterRegistry> meterRegistryProvider) {
        RagProperties.CacheProperties cfg = ragProperties.getCache();
        this.enabled = cfg.isEnabled();

        if (!enabled) {
            this.resultCache = null;
            this.embeddingCache = null;
            log.info("RAG query cache disabled (rag.cache.enabled=false)");
            return;
        }

        Duration ttl = Duration.ofSeconds(cfg.getTtlSeconds());
        this.resultCache = Caffeine.newBuilder()
                .expireAfterWrite(ttl)
                .maximumSize(cfg.getMaxSize())
                .recordStats()
                .build();
        this.embeddingCache = Caffeine.newBuilder()
                .expireAfterWrite(ttl)
                .maximumSize(cfg.getMaxSize())
                .recordStats()
                .build();

        MeterRegistry registry = meterRegistryProvider.getIfAvailable();
        if (registry != null) {
            CaffeineCacheMetrics.monitor(registry, resultCache, "rag.query.results");
            CaffeineCacheMetrics.monitor(registry, embeddingCache, "rag.query.embeddings");
        }
        log.info("RAG query cache enabled (ttlSeconds={}, maxSize={})", cfg.getTtlSeconds(), cfg.getMaxSize());
    }

    public boolean isEnabled() {
        return enabled;
    }

    /** Returns the cached retrieval response for {@code key}, or {@code null} on miss / when disabled. */
    @SuppressWarnings("unchecked")
    public <T> T getResult(String key) {
        if (!enabled || key == null) {
            return null;
        }
        return (T) resultCache.getIfPresent(key);
    }

    /** Stores a retrieval response under {@code key}. No-op when disabled or given a null value. */
    public void putResult(String key, Object value) {
        if (enabled && key != null && value != null) {
            resultCache.put(key, value);
        }
    }

    /**
     * Returns the query embedding for {@code text}, computing it via {@code loader} on a miss. The
     * result is cached only when non-empty. When disabled, {@code loader} is always invoked.
     */
    public List<Double> embedQuery(String text, String embeddingType, Supplier<List<Double>> loader) {
        if (!enabled) {
            return loader.get();
        }
        String key = (embeddingType == null ? "" : embeddingType) + SEP + normalize(text);
        List<Double> cached = embeddingCache.getIfPresent(key);
        if (cached != null) {
            return cached;
        }
        List<Double> vector = loader.get();
        if (vector != null && !vector.isEmpty()) {
            embeddingCache.put(key, vector);
        }
        return vector;
    }

    /**
     * Normalizes a query so trivial variants (case, leading/trailing and repeated internal
     * whitespace) collapse onto the same cache key.
     */
    public static String normalize(String query) {
        if (query == null) {
            return "";
        }
        return query.trim().toLowerCase(Locale.ROOT).replaceAll("\\s+", " ");
    }

    /**
     * A cheap, REST-free identifier of the caller's effective permission scope for use in the result
     * key. Two requests from the same principal within the TTL window share retrieval results;
     * different principals never do. Deliberately avoids resolving group membership (which costs REST
     * calls) - the TTL bounds the staleness that introduces.
     *
     * <p>A caller carrying more than one identity is keyed by {@link CallerIdentities#scopeKey()}, which is
     * length-prefixed. The separator-joined form this replaced was not injective: a username containing the
     * separator produced another caller's key, and this string is what decides whether one caller is served
     * another's retrieval results.</p>
     *
     * <p>Static, and reads the marker interface rather than taking {@code CallerIdentityService}, because
     * both search services call it statically while building their cache key.</p>
     */
    public static String principalScope(Authentication auth) {
        if (auth == null) {
            return "anon";
        }
        if (auth instanceof CallerAuthentication caller) {
            return caller.identities().scopeKey();
        }
        return "u:" + auth.getName();
    }
}
