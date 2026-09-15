package org.hyland.contentlake.rag.service;

import lombok.extern.slf4j.Slf4j;
import org.hyland.contentlake.client.HxprService;
import org.hyland.contentlake.model.HxprTermsAggregationResult;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * The sources a permission filter has to cover, and the type each one is.
 *
 * <p>A permission clause is built per source, so a source that nothing names gets no clause, and a
 * document belonging to it can be matched by no query. Before this class the only sources that could be
 * named were Alfresco and Nuxeo: the configured ids of those two, plus Alfresco ids discovered by a
 * type-restricted document query. A third source, which today means the filesystem connector or any
 * plugin connector on {@code connector-batch-ingester} (#132), was therefore invisible unless an
 * operator pinned {@code rag.permission.source-ids} by hand (#133).</p>
 *
 * <h3>How sources are discovered</h3>
 *
 * <p>One terms aggregation over {@code cin_sourceId}, whose bucket keys are the stored
 * {@code <sourceType>:<sourceId>} values. That yields every source the index holds together with its
 * type, in a single call, for any type, and it is the same primitive {@code StatusController} and
 * {@code FacetsService} already use. Reading the type out of the id rather than inferring it is what
 * makes {@link #qualify} exact for a source this build was never compiled against: the qualified form
 * is what {@code cin_sourceId} is compared against, and a bare id can never match a stored
 * {@code <type>:<id>}.</p>
 *
 * <p>Two properties of the aggregation shape the contract here. Buckets are the top
 * {@link #MAX_SOURCE_BUCKETS} by document count, so a corpus with more distinct sources than that would
 * truncate, which is logged rather than silently accepted. And a failure returns no sources rather than
 * throwing: the caller then falls back to the configured ids, and a caller with no source at all builds
 * {@code AclFilterBuilder.unresolvedSourceClause()}, which matches nothing. The absence of a decision
 * must not become the absence of a filter.</p>
 *
 * <h3>What it deliberately does not decide</h3>
 *
 * <p>Whose authorities can be resolved on a source. Group expansion needs a directory to ask, and there
 * is one only for Alfresco and Nuxeo. A source whose type is unknown resolves to the caller's default
 * authorities alone (themselves, and Everyone), so its public documents are retrievable and its
 * group-restricted ones stay hidden. That is fail-closed, and it belongs to the search services that own
 * the directory clients rather than here.</p>
 *
 * <p>Configuration is passed in per call as {@link Configured} rather than injected, because both search
 * services already own those properties as {@code @Value} fields and a second copy that could disagree
 * with them would be a defect waiting to happen. The cache therefore holds only what came from the
 * index, which is the part that is expensive and the part that is shared.</p>
 */
@Slf4j
final class PermissionSourceCatalog {

    static final String ALFRESCO = "alfresco";
    static final String NUXEO = "nuxeo";

    /** Aggregation bucket ceiling. A deployment with more distinct sources than this is logged. */
    private static final int MAX_SOURCE_BUCKETS = 100;

    private static final String SOURCE_ID_FIELD = "cin_sourceId";

    /** Matches an explicit source pin in a caller-supplied filter, e.g. {@code cin_sourceId = 'x'}. */
    private static final Pattern SOURCE_ID_EQUALS_PATTERN =
            Pattern.compile("cin_sourceId\\s*=\\s*'([^']+)'");

    /** The source ids a deployment configures directly, as the search services hold them. */
    record Configured(String alfrescoSourceId, String nuxeoSourceId, String pinnedSourceIds) {

        boolean hasPin() {
            return pinnedSourceIds != null && !pinnedSourceIds.isBlank();
        }
    }

    private final HxprService hxprService;
    private final Duration ttl;
    private final Clock clock;

    /** Bare source id to source type, as read from the index. Empty until the first successful scan. */
    private volatile Map<String, String> cached;
    private volatile Instant cachedAt;

    PermissionSourceCatalog(HxprService hxprService, Duration ttl, Clock clock) {
        this.hxprService = hxprService;
        this.ttl = ttl;
        this.clock = clock;
    }

    /**
     * The sources to build clauses for, in precedence order.
     *
     * <ol>
     *   <li>A source pinned in the caller's own filter, which is the caller asking for one source.</li>
     *   <li>Every source of {@code sourceType}, when the request names one.</li>
     *   <li>{@code rag.permission.source-ids}, when an operator has pinned the set.</li>
     *   <li>Otherwise every source the index holds, plus the configured Alfresco and Nuxeo ids, which
     *       may name a source that has not been ingested into yet.</li>
     * </ol>
     *
     * @return bare source ids, never null
     */
    List<String> resolve(Configured configured, String sourceType, String additionalFilter) {
        Set<String> sourceIds = new LinkedHashSet<>();

        if (additionalFilter != null && !additionalFilter.isBlank()) {
            var matcher = SOURCE_ID_EQUALS_PATTERN.matcher(additionalFilter);
            while (matcher.find()) {
                addBareId(sourceIds, matcher.group(1));
            }
            if (!sourceIds.isEmpty()) {
                return List.copyOf(sourceIds);
            }
        }

        String normalizedType = normalizeType(sourceType);
        if (normalizedType != null) {
            addIdsOfType(sourceIds, configured, normalizedType);
            if (!sourceIds.isEmpty()) {
                return List.copyOf(sourceIds);
            }
        }

        if (configured.hasPin()) {
            for (String candidate : configured.pinnedSourceIds().split(",")) {
                addBareId(sourceIds, candidate);
            }
            return List.copyOf(sourceIds);
        }

        sourceIds.addAll(indexedSources().keySet());
        addBareId(sourceIds, configured.alfrescoSourceId());
        addBareId(sourceIds, configured.nuxeoSourceId());
        return List.copyOf(sourceIds);
    }

    /**
     * The type of one source, or {@code null} when nothing knows it.
     *
     * <p>Configuration wins over the index so a configured source is typed correctly before anything has
     * been ingested into it, which is the state every fresh deployment starts in.</p>
     */
    String sourceType(Configured configured, String sourceId) {
        if (sourceId == null || sourceId.isBlank()) {
            return null;
        }
        if (sourceId.equals(bareId(configured.alfrescoSourceId()))) {
            return ALFRESCO;
        }
        if (sourceId.equals(bareId(configured.nuxeoSourceId()))) {
            return NUXEO;
        }
        return indexedSources().get(sourceId);
    }

    /**
     * {@code <sourceType>:<sourceId>} as stored in {@code cin_sourceId}, or the bare id when the type is
     * unknown.
     *
     * <p>The bare fallback cannot match a stored value, and that is the safe direction: it is used only
     * for the full-source-access clause, so an unknown source falls back to ACL matching rather than to
     * an unfiltered read of a source nobody could type.</p>
     */
    String qualify(Configured configured, String sourceId) {
        String type = sourceType(configured, sourceId);
        return type == null ? sourceId : type + ":" + sourceId;
    }

    boolean isAlfresco(Configured configured, String sourceId) {
        return ALFRESCO.equals(sourceType(configured, sourceId));
    }

    boolean isNuxeo(Configured configured, String sourceId) {
        return NUXEO.equals(sourceType(configured, sourceId));
    }

    /** Bare source id to source type for every source in the index, cached for the TTL. */
    Map<String, String> indexedSources() {
        Map<String, String> current = cached;
        if (current != null && !isExpired()) {
            return current;
        }

        Map<String, String> discovered = discover();
        if (discovered.isEmpty()) {
            // Not cached: an empty index is the normal state of a stack that is still ingesting, and
            // holding that answer for the whole TTL would keep the first documents written unreadable.
            return Map.of();
        }

        cached = discovered;
        cachedAt = clock.instant();
        return discovered;
    }

    /** Discards the discovered sources, so the next call re-reads the index. */
    void invalidate() {
        cached = null;
        cachedAt = null;
    }

    private Map<String, String> discover() {
        try {
            HxprTermsAggregationResult aggregation =
                    hxprService.termsAggregation(null, SOURCE_ID_FIELD, null, MAX_SOURCE_BUCKETS);
            if (aggregation == null || aggregation.getAggregationsBuckets() == null) {
                return Map.of();
            }

            List<HxprTermsAggregationResult.Bucket> buckets = aggregation.getAggregationsBuckets();
            Map<String, String> sources = new LinkedHashMap<>();
            for (HxprTermsAggregationResult.Bucket bucket : buckets) {
                String key = bucket.getKey();
                if (key == null || key.isBlank()) {
                    continue;
                }
                int separator = key.indexOf(':');
                if (separator > 0 && separator < key.length() - 1) {
                    sources.put(key.substring(separator + 1).trim(),
                            key.substring(0, separator).trim().toLowerCase(Locale.ROOT));
                } else {
                    // A stored id with no type prefix predates the namespacing. It still needs a clause,
                    // so keep it with an unknown type rather than dropping it.
                    sources.put(key.trim(), null);
                }
            }

            if (buckets.size() >= MAX_SOURCE_BUCKETS) {
                log.warn("Permission source discovery hit its bucket ceiling of {}; a source outside the "
                                + "{} most populated ones would be missing from the permission filter. "
                                + "Pin rag.permission.source-ids to name the set explicitly",
                        MAX_SOURCE_BUCKETS, MAX_SOURCE_BUCKETS);
            }
            if (!sources.isEmpty()) {
                log.debug("Discovered permission sources {}", sources);
            }
            return Map.copyOf(sources);
        } catch (Exception e) {
            log.warn("Failed to discover permission sources: {}", e.getMessage());
            return Map.of();
        }
    }

    private void addIdsOfType(Set<String> sourceIds, Configured configured, String type) {
        if (ALFRESCO.equals(type)) {
            addBareId(sourceIds, configured.alfrescoSourceId());
        } else if (NUXEO.equals(type)) {
            addBareId(sourceIds, configured.nuxeoSourceId());
        }
        if (!sourceIds.isEmpty()) {
            // A configured id for the requested type answers it, without reading the index.
            return;
        }
        indexedSources().forEach((sourceId, indexedType) -> {
            if (type.equals(indexedType)) {
                sourceIds.add(sourceId);
            }
        });
    }

    private boolean isExpired() {
        Instant at = cachedAt;
        return at == null || Duration.between(at, clock.instant()).compareTo(ttl) >= 0;
    }

    /** A source type in the form the index stores it: trimmed and lower-cased, or null when blank. */
    static String normalizeType(String sourceType) {
        if (sourceType == null) {
            return null;
        }
        String trimmed = sourceType.trim().toLowerCase(Locale.ROOT);
        return trimmed.isBlank() ? null : trimmed;
    }

    private static void addBareId(Set<String> sourceIds, String candidate) {
        String bare = bareId(candidate);
        if (bare != null) {
            sourceIds.add(bare);
        }
    }

    /**
     * The source id without its type prefix, which is the form {@code sys_racl} principals are
     * namespaced with. {@code null} for a blank candidate.
     */
    static String bareId(String candidate) {
        if (candidate == null) {
            return null;
        }
        String trimmed = candidate.trim();
        if (trimmed.isBlank()) {
            return null;
        }
        int separator = trimmed.indexOf(':');
        return separator >= 0 && separator < trimmed.length() - 1
                ? trimmed.substring(separator + 1)
                : trimmed;
    }

    /** The ids named by a pinned {@code rag.permission.source-ids} value, bare and in order. */
    static List<String> parsePinned(String pinnedSourceIds) {
        if (pinnedSourceIds == null || pinnedSourceIds.isBlank()) {
            return List.of();
        }
        List<String> ids = new ArrayList<>();
        for (String candidate : pinnedSourceIds.split(",")) {
            String bare = bareId(candidate);
            if (bare != null && !ids.contains(bare)) {
                ids.add(bare);
            }
        }
        return List.copyOf(ids);
    }
}
