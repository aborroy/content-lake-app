package org.hyland.contentlake.rag.security;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.cache.CaffeineCacheMetrics;
import lombok.extern.slf4j.Slf4j;
import org.hyland.contentlake.rag.config.RagProperties;
import org.hyland.contentlake.security.AclFilterBuilder;
import org.hyland.contentlake.security.GroupResolutionFailurePolicy;
import org.hyland.contentlake.security.SourceGroupResolver;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The authorities a caller has on one source: their own name, {@code GROUP_EVERYONE}, and whatever the
 * source's directory says about their group membership (#143).
 *
 * <p>Every search service resolves authorities the same way, so it is resolved once here. The alternative,
 * a branch per known source type in each service, was the shape this replaced: adding a third source meant
 * finding every copy, and fixing one copy was a silent half-fix.</p>
 *
 * <p>Resolvers are selected by {@link SourceGroupResolver#sourceType()} rather than by a predicate over the
 * source id, because the type of a source id is already answered by the permission source catalogue and a
 * second matching mechanism could disagree with it. A source whose type no resolver claims is reported once
 * and then resolves to default authorities, which is fail-closed: its public documents and the documents
 * granted to the caller by name are retrievable, its group-granted ones are not.</p>
 *
 * <h3>Cache</h3>
 *
 * <p>Group membership is a directory round trip per source per query, and a query that runs several
 * expansion variants resolves the same authorities repeatedly. Entries are keyed on
 * {@code (sourceType, username)} and bounded by {@code rag.security.group-cache.ttl-seconds}, which is also
 * the ceiling on how stale a caller's group membership may be: a user removed from a group keeps reading that
 * group's documents until the entry expires. A TTL of zero disables the cache, as {@code rag.cache} does.
 * Failures are never cached, so a directory outage is retried rather than held for the TTL.</p>
 */
@Slf4j
@Component
public class SourceGroupResolverRegistry {

    /**
     * The registry a service constructed outside an application context falls back to: no resolvers, so
     * every source resolves to default authorities. Immutable and stateless apart from its warn-once set.
     */
    private static final SourceGroupResolverRegistry WITHOUT_RESOLVERS =
            new SourceGroupResolverRegistry(List.of(), GroupResolutionFailurePolicy.FAIL_CLOSED, 0L, 0L, null);

    private final Map<String, SourceGroupResolver> resolversByType;
    private final GroupResolutionFailurePolicy failurePolicy;

    /** Null when {@code rag.security.group-cache.ttl-seconds} is zero. */
    private final Cache<CacheKey, GroupLookup> cache;

    /** Sources already reported as having no resolver, so the warning is logged once each. */
    private final Set<String> unresolvableSources = ConcurrentHashMap.newKeySet();

    @Autowired
    public SourceGroupResolverRegistry(List<SourceGroupResolver> resolvers,
                                       RagProperties ragProperties,
                                       ObjectProvider<MeterRegistry> meterRegistryProvider) {
        this(resolvers,
                GroupResolutionFailurePolicy.parse(ragProperties.getSecurity().getGroupResolutionFailure()),
                ragProperties.getSecurity().getGroupCache().getTtlSeconds(),
                ragProperties.getSecurity().getGroupCache().getMaxSize(),
                meterRegistryProvider.getIfAvailable());
    }

    /**
     * The programmatic form, for a caller that holds the settings rather than the property object: a
     * {@code cacheTtlSeconds} of zero disables the cache, and a null {@code meterRegistry} leaves it
     * unmetered.
     */
    public SourceGroupResolverRegistry(List<SourceGroupResolver> resolvers,
                                       GroupResolutionFailurePolicy failurePolicy,
                                       long cacheTtlSeconds,
                                       long cacheMaxSize,
                                       MeterRegistry meterRegistry) {
        this.failurePolicy = failurePolicy;
        this.resolversByType = index(resolvers);

        if (cacheTtlSeconds <= 0) {
            this.cache = null;
        } else {
            this.cache = Caffeine.newBuilder()
                    .expireAfterWrite(Duration.ofSeconds(cacheTtlSeconds))
                    .maximumSize(cacheMaxSize)
                    .recordStats()
                    .build();
            if (meterRegistry != null) {
                CaffeineCacheMetrics.monitor(meterRegistry, cache, "rag.security.groups");
            }
        }

        log.info("Group resolution: resolvers={}, failurePolicy={}, cacheTtlSeconds={}",
                resolversByType.keySet(), failurePolicy, cacheTtlSeconds);
    }

    /** See {@link #WITHOUT_RESOLVERS}. */
    public static SourceGroupResolverRegistry withoutResolvers() {
        return WITHOUT_RESOLVERS;
    }

    /**
     * The caller's authorities on one source, or an empty list when they could not be resolved and the
     * configured policy is to fail closed. An empty list is not "no groups": it means the answer is unknown,
     * and the permission filter drops the source rather than guessing.
     *
     * @param sourceType the type of {@code sourceId}, or {@code null} when nothing knows it
     */
    public List<String> authorities(String username, String sourceId, String sourceType) {
        LinkedHashSet<String> authorities =
                new LinkedHashSet<>(AclFilterBuilder.defaultAuthorities(username));

        SourceGroupResolver resolver = resolverFor(sourceType);
        if (resolver == null) {
            warnUnresolvable(sourceId, sourceType);
            return List.copyOf(authorities);
        }

        GroupLookup lookup;
        try {
            lookup = groups(resolver, username);
        } catch (Exception e) {
            return onFailure(username, sourceId, e);
        }

        if (!lookup.identityKnown()) {
            log.debug("User {} has no identity in the {} directory, so source {} resolves to default "
                    + "authorities alone", username, resolver.sourceType(), sourceId);
            return List.copyOf(authorities);
        }

        authorities.addAll(lookup.groups());
        log.debug("Resolved {} authorities for user {} on source {}", authorities.size(), username, sourceId);
        return List.copyOf(authorities);
    }

    /** Whether a source of this type can have its group membership expanded. */
    public boolean canExpandGroups(String sourceType) {
        return resolverFor(sourceType) != null;
    }

    /** The source types a resolver is wired for, lower-cased. */
    public Set<String> resolvableSourceTypes() {
        return resolversByType.keySet();
    }

    private GroupLookup groups(SourceGroupResolver resolver, String username) {
        if (cache == null) {
            return lookup(resolver, username);
        }
        // Not cache.get(key, loader): a loader that throws would be wrapped in a CompletionException,
        // which hides the cause the failure policy logs.
        CacheKey key = new CacheKey(resolver.sourceType(), username);
        GroupLookup cached = cache.getIfPresent(key);
        if (cached != null) {
            return cached;
        }
        GroupLookup resolved = lookup(resolver, username);
        cache.put(key, resolved);
        return resolved;
    }

    private static GroupLookup lookup(SourceGroupResolver resolver, String username) {
        List<String> groups = resolver.resolveGroups(username);
        return groups == null ? GroupLookup.unknownIdentity() : new GroupLookup(List.copyOf(groups), true);
    }

    private SourceGroupResolver resolverFor(String sourceType) {
        String normalized = normalize(sourceType);
        return normalized == null ? null : resolversByType.get(normalized);
    }

    /**
     * Says once per source that its groups cannot be expanded, and what that costs.
     *
     * <p>Group expansion needs a directory to ask, and there is a resolver only for the source types named
     * at startup. Any other source still gets a permission clause, built from the caller's default
     * authorities, so its {@code __Everyone__} documents and documents granted to the caller by name are
     * retrievable while its group-granted ones are not.</p>
     */
    private void warnUnresolvable(String sourceId, String sourceType) {
        if (sourceId == null || sourceId.isBlank() || !unresolvableSources.add(sourceId)) {
            return;
        }
        log.warn("Source {} (type {}) has no group resolver, so its permission clause carries only the "
                        + "caller's own authorities: public documents and documents granted to the user by "
                        + "name are retrievable, group-granted documents are not. Resolvers are wired for "
                        + "{}. See docs/deployment-rag.md",
                sourceId, sourceType == null ? "unknown" : sourceType, resolversByType.keySet());
    }

    /**
     * Applies {@code rag.security.group-resolution-failure}. Both modes log at WARN: a directory outage is
     * worth knowing about whichever behaviour is configured, because in one mode the caller silently loses
     * group-granted documents and in the other they lose the source.
     */
    private List<String> onFailure(String username, String sourceId, Exception cause) {
        if (failurePolicy == GroupResolutionFailurePolicy.DEGRADE) {
            log.warn("Failed to resolve authorities for user {} on source {}; policy is {}, so proceeding "
                            + "with username + GROUP_EVERYONE and no group-granted access: {}",
                    username, sourceId, failurePolicy, cause.getMessage());
            return AclFilterBuilder.defaultAuthorities(username);
        }
        log.warn("Failed to resolve authorities for user {} on source {}; policy is {}, so the source is "
                        + "excluded from the permission filter and the caller sees nothing from it: {}",
                username, sourceId, failurePolicy, cause.getMessage());
        return List.of();
    }

    private static Map<String, SourceGroupResolver> index(List<SourceGroupResolver> resolvers) {
        Map<String, SourceGroupResolver> byType = new LinkedHashMap<>();
        for (SourceGroupResolver resolver : resolvers) {
            String type = normalize(resolver.sourceType());
            if (type == null) {
                throw new IllegalStateException(
                        resolver.getClass().getName() + " declares a blank sourceType()");
            }
            SourceGroupResolver previous = byType.put(type, resolver);
            if (previous != null) {
                // Which of the two wins would decide who can read what, so it must not be left to bean
                // ordering.
                throw new IllegalStateException("Two group resolvers claim source type '" + type + "': "
                        + previous.getClass().getName() + " and " + resolver.getClass().getName());
            }
        }
        return Map.copyOf(byType);
    }

    private static String normalize(String sourceType) {
        if (sourceType == null) {
            return null;
        }
        String trimmed = sourceType.trim().toLowerCase(Locale.ROOT);
        return trimmed.isBlank() ? null : trimmed;
    }

    /**
     * A cache entry's identity. A record rather than a joined string, so no source type or username can be
     * spelled in a way that collides with another pair, and one caller cannot be served another's membership.
     */
    private record CacheKey(String sourceType, String username) {
    }

    /**
     * One directory answer. {@code identityKnown} false is the resolver's {@code null}: the caller is not in
     * this directory, which is not a failure and must not be read as "in no groups" either, since the two
     * differ once anything starts caching negative answers.
     */
    private record GroupLookup(List<String> groups, boolean identityKnown) {

        private static final GroupLookup UNKNOWN_IDENTITY = new GroupLookup(List.of(), false);

        static GroupLookup unknownIdentity() {
            return UNKNOWN_IDENTITY;
        }
    }
}
