package org.hyland.contentlake.security;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Every identity one caller has, and which of them answers for a source type nothing else names.
 *
 * <p>Replaces the pair of usernames the query path used to carry. The shape that matters is not "a map of
 * identities" but <em>the fallback</em>: one identity may be nominated to answer for any source type no other
 * identity claims. That is what makes a single Basic credential behave as it always has while leaving room
 * for a caller who is several principals at once.</p>
 *
 * <h3>The two shapes in use, and why they are equivalent to what they replace</h3>
 *
 * <ul>
 *   <li>{@link #single(String)} builds one <em>untyped</em> identity that is also the fallback. Every source
 *       type resolves to that name, which is what a repository-authenticated caller has always got.</li>
 *   <li>A caller with two repository sessions is built as {@code fallback(alfresco:alice) + add(nuxeo:bob)}.
 *       Alfresco resolves to {@code alice}, Nuxeo to {@code bob}, and any third source type to {@code alice}
 *       through the fallback, which is what the {@code isNuxeoSource(id) ? nuxeoUser : alfrescoUser} it
 *       replaces did by having nowhere else to send it.</li>
 * </ul>
 *
 * <h3>A single identity is always untyped, and that is load-bearing</h3>
 *
 * <p>{@link #describe()} is not a log string. {@code RateLimitFilter} keys its token buckets on the
 * authentication's name, and the feedback API persists the current username as the submitter and filters
 * reads on it. So a single identity must keep describing itself as the bare {@code alice}: typing it as
 * {@code alfresco:alice} would re-bucket live callers' rate limits and orphan every feedback row its author
 * had already stored. Do not "tidy" this into a uniform {@code type:user} form.</p>
 */
public final class CallerIdentities {

    /** No identities at all: an authenticator declining, not a caller with nothing. */
    public static final CallerIdentities EMPTY = new CallerIdentities(null, Map.of());

    private final SourceIdentity fallback;

    /** Typed identities by source type, insertion-ordered. Includes the fallback when it is typed. */
    private final Map<String, SourceIdentity> byType;

    private CallerIdentities(SourceIdentity fallback, Map<String, SourceIdentity> byType) {
        this.fallback = fallback;
        this.byType = byType;
    }

    /**
     * One untyped identity, which is also the fallback: the shape every credential that proves a single
     * login produces.
     */
    public static CallerIdentities single(String username) {
        return builder().fallback(SourceIdentity.untyped(username)).build();
    }

    public static Builder builder() {
        return new Builder();
    }

    /** The identity answering for a source type no other identity claims, or {@code null}. */
    public SourceIdentity fallback() {
        return fallback;
    }

    /** The fallback when there is one, otherwise the first identity added. {@code null} only when empty. */
    public SourceIdentity primary() {
        if (fallback != null) {
            return fallback;
        }
        return byType.values().stream().findFirst().orElse(null);
    }

    public String primaryUsername() {
        SourceIdentity primary = primary();
        return primary == null ? null : primary.username();
    }

    /**
     * The caller's username in a source of this type.
     *
     * @return the typed identity's username, else the fallback's, else {@code null} when the caller has no
     *         identity for this source at all. {@code null} means the source is dropped from the permission
     *         filter rather than widened, which is the fail-closed direction.
     */
    public String usernameFor(String sourceType) {
        if (sourceType != null && !sourceType.isBlank()) {
            SourceIdentity typed = byType.get(sourceType.trim().toLowerCase(Locale.ROOT));
            if (typed != null) {
                return typed.username();
            }
        }
        return fallback == null ? null : fallback.username();
    }

    /** Every identity, fallback first, then the rest in the order they were added. */
    public List<SourceIdentity> all() {
        List<SourceIdentity> all = new ArrayList<>();
        if (fallback != null) {
            all.add(fallback);
        }
        for (SourceIdentity identity : byType.values()) {
            if (!identity.equals(fallback)) {
                all.add(identity);
            }
        }
        return List.copyOf(all);
    }

    public Set<String> sourceTypes() {
        return byType.keySet();
    }

    public boolean isEmpty() {
        return fallback == null && byType.isEmpty();
    }

    public int size() {
        return all().size();
    }

    /**
     * The caller's name for rate-limit bucketing, stored feedback and logs.
     *
     * <p>A lone untyped identity describes itself as the bare username. Anything else joins as
     * {@code alfresco:alice|nuxeo:bob}, reproducing exactly the string the two-username authentication it
     * replaces produced. See the class note on why neither form may change.</p>
     */
    public String describe() {
        List<SourceIdentity> all = all();
        if (all.isEmpty()) {
            return "";
        }
        if (all.size() == 1 && all.get(0).isUntyped()) {
            return all.get(0).username();
        }
        StringBuilder described = new StringBuilder();
        for (SourceIdentity identity : all) {
            if (!described.isEmpty()) {
                described.append('|');
            }
            if (!identity.isUntyped()) {
                described.append(identity.sourceType()).append(':');
            }
            described.append(identity.username());
        }
        return described.toString();
    }

    /**
     * A cache-key fragment that no caller can spell to look like another.
     *
     * <p>Length-prefixed rather than separator-joined, for the reason
     * {@code SourceGroupResolverRegistry}'s cache key is a record: this string decides whether one caller is
     * served another's retrieval results, and a username containing the separator would otherwise be enough
     * to collide with a different caller's key.</p>
     */
    public String scopeKey() {
        StringBuilder key = new StringBuilder();
        for (SourceIdentity identity : all()) {
            String type = identity.isUntyped() ? "" : identity.sourceType();
            key.append(type.length()).append(':').append(type)
                    .append(identity.username().length()).append(':').append(identity.username());
        }
        return key.toString();
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof CallerIdentities that)) {
            return false;
        }
        return Objects.equals(fallback, that.fallback) && byType.equals(that.byType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fallback, byType);
    }

    @Override
    public String toString() {
        return "CallerIdentities[" + describe() + "]";
    }

    public static final class Builder {

        private SourceIdentity fallback;
        private final Map<String, SourceIdentity> byType = new LinkedHashMap<>();

        /**
         * The identity answering for any source type nothing else claims. At most one.
         *
         * <p>Choosing {@code fallback} over {@link #add(SourceIdentity)} is the whole of the decision about
         * what a caller sees in a source they have no identity for: a fallback means "query it as this
         * name", {@code add} means "this name answers for its own type only, and an unclaimed source is
         * dropped".</p>
         */
        public Builder fallback(SourceIdentity identity) {
            if (identity == null) {
                return this;
            }
            if (fallback != null) {
                throw new IllegalStateException(
                        "A caller has at most one fallback identity; already set to " + fallback);
            }
            fallback = identity;
            if (!identity.isUntyped()) {
                register(identity);
            }
            return this;
        }

        /** An identity answering for its own source type only. */
        public Builder add(SourceIdentity identity) {
            if (identity == null) {
                return this;
            }
            if (identity.isUntyped()) {
                throw new IllegalArgumentException(
                        "An untyped identity answers for every source type, so it must be the fallback");
            }
            register(identity);
            return this;
        }

        public Builder add(String sourceType, String username) {
            return add(SourceIdentity.of(sourceType, username));
        }

        private void register(SourceIdentity identity) {
            SourceIdentity existing = byType.putIfAbsent(identity.sourceType(), identity);
            if (existing != null && !existing.equals(identity)) {
                // Which of two names wins would decide what the caller reads, so it must not be settled by
                // the order they happened to be added. SourceGroupResolverRegistry refuses the same thing
                // for the same reason.
                throw new IllegalStateException("Two identities claim source type '" + identity.sourceType()
                        + "': '" + existing.username() + "' and '" + identity.username() + "'");
            }
        }

        public CallerIdentities build() {
            if (fallback == null && byType.isEmpty()) {
                return EMPTY;
            }
            // Insertion order is part of the contract, because describe() must reproduce
            // "alfresco:alice|nuxeo:bob" exactly and that string is a rate-limit bucket key. Map.copyOf
            // leaves iteration order unspecified, so it cannot be used here.
            return new CallerIdentities(fallback,
                    Collections.unmodifiableMap(new LinkedHashMap<>(byType)));
        }
    }
}
