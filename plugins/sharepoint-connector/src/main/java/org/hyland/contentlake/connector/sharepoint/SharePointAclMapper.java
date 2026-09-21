package org.hyland.contentlake.connector.sharepoint;

import com.fasterxml.jackson.databind.JsonNode;
import org.hyland.contentlake.spi.PermissionRule;
import org.hyland.contentlake.spi.SecurityConfig;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

/**
 * Turns a {@code driveItem}'s Graph permissions collection into the read principals the pipeline stores.
 *
 * <p>Core does the namespacing: {@code AclFilterBuilder} writes {@code <authority>_#_<sourceId>}, reads a
 * {@code GROUP_} prefix as "this is a group", and rewrites the literal {@code GROUP_EVERYONE} to the
 * un-namespaced {@code __Everyone__}. So this emits raw principal identifiers and nothing else.</p>
 *
 * <h3>Fail-closed, and what that costs</h3>
 * <p>Every unrecognised shape grants nothing. The two failure modes are not symmetric: a document nobody
 * can retrieve is visible and fixable, while a document the wrong person retrieves is neither. That
 * asymmetry decides every judgement call below, including the ones that make the connector look worse in a
 * demonstration.</p>
 *
 * <h3>The mapping</h3>
 * <table>
 *   <caption>Graph permission shapes and what they emit</caption>
 *   <tr><th>Shape</th><th>Emitted</th></tr>
 *   <tr><td>{@code grantedToV2.user}</td><td>both the Entra object id and the {@code userPrincipalName}</td></tr>
 *   <tr><td>{@code grantedToV2.group}</td><td>{@code GROUP_<entra-object-id>}</td></tr>
 *   <tr><td>{@code grantedToV2.siteUser} or {@code siteGroup}, alone</td><td>a site-local form that resolves for nobody, counted and reported</td></tr>
 *   <tr><td>an "Everyone" or "Everyone except external users" claim</td><td>{@code GROUP_EVERYONE}</td></tr>
 *   <tr><td>{@code link.scope = organization}</td><td>{@code GROUP_EVERYONE}</td></tr>
 *   <tr><td>{@code link.scope = users}</td><td>{@code grantedToIdentitiesV2}, expanded as users</td></tr>
 *   <tr><td>{@code link.scope = anonymous}</td><td>nothing</td></tr>
 *   <tr><td>{@code roles} containing {@code read}, {@code write} or {@code owner}</td><td>grants read; any other role grants nothing</td></tr>
 * </table>
 *
 * <p>Four of those are decisions rather than details:</p>
 * <ul>
 *   <li><strong>Emitting both an object id and a UPN for one user is not a widening.</strong> They name the
 *       same identity, and {@code rag-service} matches a document against the caller's own username. Which
 *       of the two that username is depends on the deployment's identity provider, so emitting both is what
 *       makes a named grant work whether callers are known by UPN or by object id.</li>
 *   <li><strong>A group is never emitted by display name.</strong> Entra display names are not unique, so
 *       {@code GROUP_<displayName>} is a real leak vector: two groups called "Finance" in different parts
 *       of a directory would share a principal.</li>
 *   <li><strong>Anonymous is not everyone.</strong> An anonymous sharing link grants nothing to any
 *       authenticated principal, so it contributes no principal at all.</li>
 *   <li><strong>{@code denyPrincipals} stays empty.</strong> Graph's permissions collection has no deny ACE
 *       model, and synthesising one would be fiction.</li>
 * </ul>
 *
 * <h3>When {@code grantedToV2} carries several representations of one identity</h3>
 * <p>A single entry routinely holds {@code user} and {@code siteUser}, or {@code group} and
 * {@code siteGroup}, for the same person or group. Those are one identity described twice, so the directory
 * form wins and the site-local form is ignored. Only an entry whose <em>only</em> identity is site-local
 * produces a site-local principal, which is the case no resolver can ever expand.</p>
 *
 * <h3>Group grants depend on something outside this connector</h3>
 * <p>{@code rag-service} expands group membership through a source-type resolver registry, and its Entra
 * resolver is a conditional bean: with {@code rag.security.entra.enabled} unset there is no resolver for
 * this source type, and a document granted only to an Entra group is ingested with correct principals and
 * retrievable by nobody.</p>
 *
 * <p>This class behaves the same either way, which is the point: emit the group ids faithfully and report how
 * many documents depend on them, so enabling the resolver is a deployment decision rather than a re-ingest.
 * Widening a group grant to everyone so that a demonstration works is the exact failure this class is built
 * to prevent.</p>
 */
public final class SharePointAclMapper {

    private static final Logger log = Logger.getLogger(SharePointAclMapper.class.getName());

    /** The authority core rewrites to the un-namespaced {@code __Everyone__}. */
    public static final String EVERYONE_AUTHORITY = "GROUP_EVERYONE";

    /** Roles that grant read. Anything else grants nothing, and is counted so the omission is visible. */
    private static final Set<String> READ_GRANTING_ROLES = Set.of("read", "write", "owner");

    /**
     * Display names and claim fragments that mean "every authenticated user in this tenant".
     *
     * <p>Detection by display name is unsatisfying and is the assumption most likely to be wrong: the exact
     * strings Graph returns for these two pseudo-groups are unconfirmed against a real tenant. That is why
     * the set is configurable through {@code sharepoint.everyone-claims}, so a tenant that words them
     * differently is a configuration change rather than a release. {@code spo-grid-all-users} is the claim
     * fragment SharePoint uses for "everyone except external users" and does not depend on wording.</p>
     */
    private static final Set<String> DEFAULT_EVERYONE_CLAIMS = Set.of(
            "everyone",
            "everyone except external users");

    /**
     * Claim fragments that identify the tenant-wide pseudo-groups regardless of wording.
     *
     * <p>Matched as a substring, which display names deliberately are not: "Everyone in Finance" is not
     * everyone, and a {@code contains} test on names would map it to the whole tenant. A claim is a
     * structured identifier rather than a label, so a substring test on one is safe.</p>
     */
    private static final Set<String> EVERYONE_CLAIM_FRAGMENTS = Set.of("spo-grid-all-users");

    private final AclFallback fallback;
    private final GroupGrants groupGrants;
    private final Set<String> everyoneClaims;

    // Run counters. Reported at the end of a pass, because "some documents are retrievable by nobody" is
    // only actionable if the number is stated.
    private final AtomicLong documentsMapped = new AtomicLong();
    private final AtomicLong documentsWithNoReadableAcl = new AtomicLong();
    private final AtomicLong documentsGroupOnly = new AtomicLong();
    private final AtomicLong documentsSiteLocalOnly = new AtomicLong();
    private final AtomicLong documentsGrantedToNobody = new AtomicLong();
    private final Map<String, AtomicLong> unrecognisedRoles = new ConcurrentHashMap<>();

    public SharePointAclMapper(AclFallback fallback, GroupGrants groupGrants, Set<String> everyoneClaims) {
        this.fallback = fallback == null ? AclFallback.FAIL_CLOSED : fallback;
        this.groupGrants = groupGrants == null ? GroupGrants.MAP : groupGrants;
        this.everyoneClaims = everyoneClaims == null || everyoneClaims.isEmpty()
                ? DEFAULT_EVERYONE_CLAIMS
                : everyoneClaims.stream().map(claim -> claim.toLowerCase(Locale.ROOT))
                        .collect(java.util.stream.Collectors.toUnmodifiableSet());
    }

    /** Defaults: fail closed, map group grants faithfully, recognise the documented "everyone" claims. */
    public static SharePointAclMapper withDefaults() {
        return new SharePointAclMapper(AclFallback.FAIL_CLOSED, GroupGrants.MAP, null);
    }

    /**
     * One item's mapped ACL.
     *
     * @param readPrincipals      raw principals granted read, for the flat set the pipeline filters on.
     *                            Empty means "nobody", which is a legitimate answer and not an error
     * @param securityConfig      the structured form, one rule per emitted principal
     * @param ingestable          whether the item should be ingested at all. Only ever {@code false} when
     *                            the ACL could not be read and the fallback is fail-closed
     * @param hasUniquePermissions whether any entry named no ancestor in {@code inheritedFrom}, so this
     *                            item's permissions are its own rather than one it inherited. Note an
     *                            empty {@code inheritedFrom} object counts as naming none: Graph sends one
     *                            on an item's own permission
     */
    public record MappedAcl(Set<String> readPrincipals,
                            SecurityConfig securityConfig,
                            boolean ingestable,
                            boolean hasUniquePermissions) {

        static MappedAcl notIngestable() {
            return new MappedAcl(Set.of(), new SecurityConfig(false, List.of()), false, false);
        }

        /**
         * The same grants, marked as inherited: what a descendant of a permission-hierarchy root gets.
         *
         * <p>Two fields have to change rather than be copied. {@code inheritanceEnabled} becomes true,
         * because from the descendant's point of view every one of these rules came from above it, whatever
         * the ancestor's own collection said. And {@code hasUniquePermissions} becomes false, or the
         * descendant would be stored with the {@code sharepoint_uniquePermissions} marker that exists to
         * answer "why is this one document readable by someone else".</p>
         *
         * <p>{@code ingestable} is carried across unchanged, which is what makes a fail-closed ancestor
         * refuse its descendants too instead of them falling back to something more permissive.</p>
         */
        MappedAcl asInherited() {
            return new MappedAcl(readPrincipals,
                    new SecurityConfig(true, securityConfig == null ? List.of() : securityConfig.permissions()),
                    ingestable,
                    false);
        }
    }

    /**
     * Maps a permissions collection.
     *
     * @param permissionEntries every entry from {@code /permissions}, with all pages already followed. The
     *                          caller does the paging: a truncated collection here would silently drop
     *                          grants, so this class must never be handed a first page and told it is all
     * @param readable          whether the collection could be read at all. {@code false} routes to the
     *                          configured fallback, which is the one path that can refuse an item
     */
    public MappedAcl map(List<JsonNode> permissionEntries, boolean readable) {
        documentsMapped.incrementAndGet();

        if (!readable) {
            documentsWithNoReadableAcl.incrementAndGet();
            Set<String> fallbackPrincipals = fallbackPrincipals();
            if (fallbackPrincipals == null) {
                return MappedAcl.notIngestable();
            }
            return new MappedAcl(fallbackPrincipals, rulesFor(fallbackPrincipals), true, false);
        }

        Set<String> principals = new LinkedHashSet<>();
        List<PermissionRule> rules = new ArrayList<>();
        boolean anyInherited = false;
        boolean anyUnique = false;
        boolean anyDirectoryPrincipal = false;
        boolean anySiteLocalPrincipal = false;
        boolean anyGroupPrincipal = false;
        boolean anyUserPrincipal = false;
        boolean anyEveryone = false;

        for (JsonNode entry : permissionEntries == null ? List.<JsonNode>of() : permissionEntries) {
            if (entry == null || entry.isNull()) {
                continue;
            }
            if (isInherited(entry)) {
                anyInherited = true;
            } else {
                anyUnique = true;
            }
            if (!grantsRead(entry)) {
                continue;
            }

            Emitted emitted = principalsFor(entry);
            for (Principal principal : emitted.principals()) {
                if (principals.add(principal.identity())) {
                    rules.add(new PermissionRule(principal.identity(), principal.type(),
                            principal.displayName(), "READ"));
                }
            }
            anyDirectoryPrincipal |= emitted.directory();
            anySiteLocalPrincipal |= emitted.siteLocal();
            anyGroupPrincipal |= emitted.group();
            anyUserPrincipal |= emitted.user();
            anyEveryone |= emitted.everyone();
        }

        // Counters that make the limitations countable rather than anecdotal.
        if (principals.isEmpty()) {
            documentsGrantedToNobody.incrementAndGet();
        } else if (!anyEveryone && !anyUserPrincipal && anyGroupPrincipal) {
            // Retrievable only where the query path expands Entra group membership, which is off unless
            // rag.security.entra.enabled is set.
            documentsGroupOnly.incrementAndGet();
        }
        if (anySiteLocalPrincipal && !anyDirectoryPrincipal && !anyEveryone) {
            // Worse than group-only: no resolver can ever expand a site-local principal.
            documentsSiteLocalOnly.incrementAndGet();
        }

        // "Something is inherited" is the plain reading of the SPI flag. Whether this item also has
        // permissions of its own is a separate question, and the one the hierarchy cache cares about.
        return new MappedAcl(Set.copyOf(principals), new SecurityConfig(anyInherited, List.copyOf(rules)),
                true, anyUnique);
    }

    /**
     * What an item with no readable ACL is granted to, or {@code null} to refuse it.
     *
     * <p>There is deliberately no {@code sync-account} option, unlike the CMIS connector's: app-only auth
     * has no user account whose access could stand in for a document's.</p>
     */
    Set<String> fallbackPrincipals() {
        return switch (fallback) {
            case FAIL_CLOSED -> null;
            case PUBLIC -> Set.of(EVERYONE_AUTHORITY);
        };
    }

    /**
     * Whether this entry was inherited, which needs an ancestor for it to have come from.
     *
     * <p>Not simply "the key is present and not null". Confirmed against a real tenant in #142: Graph sends
     * {@code "inheritedFrom": {}} on an item's <em>own</em> permission, an empty object rather than an
     * absent key, and {@code hasNonNull} is true for that. Reading it as inheritance inverted both answers
     * this mapper gives about inheritance, so an item with permissions of its own was stored without the
     * {@code sharepoint_uniquePermissions} marker that exists to flag exactly that.</p>
     *
     * <p>Requiring an identifying field is what distinguishes the two, and it fails safe in the direction
     * that matters: an unfamiliar shape reads as "this item has its own permissions", which over-reports
     * uniqueness rather than hiding it.</p>
     */
    private static boolean isInherited(JsonNode entry) {
        JsonNode inheritedFrom = entry.get("inheritedFrom");
        if (inheritedFrom == null || inheritedFrom.isNull() || !inheritedFrom.isObject()) {
            return false;
        }
        return text(inheritedFrom, "id") != null
                || text(inheritedFrom, "driveId") != null
                || text(inheritedFrom, "path") != null;
    }

    /** Whether this entry's roles grant read, counting any role it does not recognise. */
    private boolean grantsRead(JsonNode entry) {
        JsonNode roles = entry.get("roles");
        if (roles == null || !roles.isArray() || roles.isEmpty()) {
            // No roles is not read. A sharing link entry always carries one, so an entry without any is a
            // shape this mapper does not understand.
            return false;
        }
        boolean grants = false;
        for (JsonNode role : roles) {
            String value = role == null || role.isNull() ? null : role.asText().trim().toLowerCase(Locale.ROOT);
            if (value == null || value.isEmpty()) {
                continue;
            }
            if (READ_GRANTING_ROLES.contains(value)) {
                grants = true;
            } else {
                unrecognisedRoles.computeIfAbsent(value, key -> new AtomicLong()).incrementAndGet();
            }
        }
        return grants;
    }

    /** One entry's principals, with the flags the run summary counts. */
    private Emitted principalsFor(JsonNode entry) {
        List<Principal> principals = new ArrayList<>();
        boolean directory = false;
        boolean siteLocal = false;
        boolean group = false;
        boolean user = false;
        boolean everyone = false;

        JsonNode link = entry.get("link");
        if (link != null && !link.isNull()) {
            String scope = text(link, "scope");
            String normalisedScope = scope == null ? "" : scope.trim().toLowerCase(Locale.ROOT);
            switch (normalisedScope) {
                case "organization" -> {
                    principals.add(new Principal(EVERYONE_AUTHORITY, "group",
                            "Everyone in the organisation"));
                    everyone = true;
                }
                case "users" -> {
                    // The link itself grants nothing; the named identities do.
                    for (JsonNode identity : arrayOf(entry, "grantedToIdentitiesV2")) {
                        Emitted expanded = identityPrincipals(identity);
                        principals.addAll(expanded.principals());
                        directory |= expanded.directory();
                        siteLocal |= expanded.siteLocal();
                        group |= expanded.group();
                        user |= expanded.user();
                        everyone |= expanded.everyone();
                    }
                }
                case "anonymous" -> {
                    // Grants nothing to any authenticated principal. Not everyone.
                }
                default -> log.warning("Unrecognised sharing link scope '" + scope
                        + "'; it grants nothing, which may under-share this item");
            }
        }

        JsonNode grantedTo = entry.hasNonNull("grantedToV2") ? entry.get("grantedToV2") : entry.get("grantedTo");
        if (grantedTo != null && !grantedTo.isNull()) {
            Emitted direct = identityPrincipals(grantedTo);
            principals.addAll(direct.principals());
            directory |= direct.directory();
            siteLocal |= direct.siteLocal();
            group |= direct.group();
            user |= direct.user();
            everyone |= direct.everyone();
        }

        return new Emitted(principals, directory, siteLocal, group, user, everyone);
    }

    /**
     * One {@code identitySet} into principals.
     *
     * <p>{@code user}/{@code group} win over {@code siteUser}/{@code siteGroup} because they are the same
     * identity described twice, and the directory form is the one anything can resolve.</p>
     */
    private Emitted identityPrincipals(JsonNode identitySet) {
        if (identitySet == null || identitySet.isNull()) {
            return Emitted.none();
        }

        JsonNode user = identitySet.get("user");
        JsonNode group = identitySet.get("group");
        JsonNode siteUser = identitySet.get("siteUser");
        JsonNode siteGroup = identitySet.get("siteGroup");

        List<Principal> principals = new ArrayList<>();

        if (isEveryoneClaim(group) || isEveryoneClaim(siteGroup)) {
            return new Emitted(List.of(new Principal(EVERYONE_AUTHORITY, "group",
                    displayName(group != null ? group : siteGroup))), false, false, true, false, true);
        }

        if (user != null && !user.isNull()) {
            String objectId = text(user, "id");
            String principalName = text(user, "userPrincipalName");
            String display = displayName(user);
            if (objectId != null) {
                principals.add(new Principal(objectId, "user", display));
            }
            if (principalName != null) {
                principals.add(new Principal(principalName, "user", display));
            }
            if (!principals.isEmpty()) {
                return new Emitted(principals, true, false, false, true, false);
            }
        }

        if (group != null && !group.isNull()) {
            String objectId = text(group, "id");
            if (objectId == null) {
                // A group with no id cannot be emitted: the display name is not unique enough to be a
                // principal, so this grant is dropped and the item under-shares.
                log.warning("A group grant carried no id and was ignored; the item will be retrievable by "
                        + "fewer people than the source allows");
                return Emitted.none();
            }
            if (groupGrants == GroupGrants.SKIP) {
                return Emitted.none();
            }
            return new Emitted(List.of(new Principal("GROUP_" + objectId, "group", displayName(group))),
                    true, false, true, false, false);
        }

        if (siteUser != null && !siteUser.isNull()) {
            String siteLocalId = text(siteUser, "id");
            if (siteLocalId == null) {
                return Emitted.none();
            }
            // Deliberately not parsed out of loginName. A SharePoint claim like
            // i:0#.f|membership|bob@contoso.com does carry a UPN, but the encoding is not documented as
            // stable, and guessing generously here is exactly how a restricted document becomes readable.
            return new Emitted(List.of(new Principal("SITEUSER_" + siteLocalId, "user",
                    displayName(siteUser))), false, true, false, true, false);
        }

        if (siteGroup != null && !siteGroup.isNull()) {
            String siteLocalId = text(siteGroup, "id");
            if (siteLocalId == null) {
                return Emitted.none();
            }
            if (groupGrants == GroupGrants.SKIP) {
                return Emitted.none();
            }
            // Site-local, so no Entra resolver can ever expand it. Emitted anyway, because a principal
            // nobody matches is the fail-closed outcome, and dropping it silently would hide the grant.
            return new Emitted(List.of(new Principal("GROUP_SITEGROUP_" + siteLocalId, "group",
                    displayName(siteGroup))), false, true, true, false, false);
        }

        return Emitted.none();
    }

    /**
     * Whether an identity is one of the tenant-wide pseudo-groups.
     *
     * <p>Matched on display name and on the {@code spo-grid-all-users} claim fragment. The display names
     * are the part that needs confirming against a real tenant, which is why they are configurable.</p>
     */
    private boolean isEveryoneClaim(JsonNode identity) {
        if (identity == null || identity.isNull()) {
            return false;
        }
        String displayName = text(identity, "displayName");
        if (displayName != null && everyoneClaims.contains(displayName.trim().toLowerCase(Locale.ROOT))) {
            return true;
        }
        for (String field : List.of("loginName", "id")) {
            String value = text(identity, field);
            if (value == null) {
                continue;
            }
            String normalised = value.trim().toLowerCase(Locale.ROOT);
            for (String fragment : EVERYONE_CLAIM_FRAGMENTS) {
                if (normalised.contains(fragment)) {
                    return true;
                }
            }
        }
        return false;
    }

    private SecurityConfig rulesFor(Set<String> principals) {
        List<PermissionRule> rules = new ArrayList<>();
        for (String principal : principals) {
            rules.add(new PermissionRule(principal,
                    principal.startsWith("GROUP_") ? "group" : "user", principal, "READ"));
        }
        return new SecurityConfig(false, List.copyOf(rules));
    }

    /**
     * One line per limitation at the end of a pass.
     *
     * <p>Logged even when every number is zero, because "no documents were group-only" is itself the
     * answer an operator is looking for after enabling a group resolver.</p>
     */
    public void logSummary(String context) {
        log.info(() -> context + ": mapped ACLs for " + documentsMapped.get() + " item(s); "
                + documentsGroupOnly.get() + " retrievable only where Entra group expansion is enabled, "
                + documentsSiteLocalOnly.get() + " granted only to site-local principals that no resolver "
                + "can expand, " + documentsGrantedToNobody.get() + " granted to nobody, "
                + documentsWithNoReadableAcl.get() + " with an unreadable ACL");
        if (!unrecognisedRoles.isEmpty()) {
            log.warning("These Graph roles were not recognised and granted nothing: "
                    + unrecognisedRolesByCount()
                    + ". A role that should grant read and does not means those items under-share.");
        }
    }

    /** Counters, for tests and for a status endpoint. */
    public Map<String, Long> counters() {
        return Map.of(
                "documentsMapped", documentsMapped.get(),
                "documentsWithNoReadableAcl", documentsWithNoReadableAcl.get(),
                "documentsGroupOnly", documentsGroupOnly.get(),
                "documentsSiteLocalOnly", documentsSiteLocalOnly.get(),
                "documentsGrantedToNobody", documentsGrantedToNobody.get());
    }

    /** Unrecognised roles and how often each appeared, sorted for a stable log line. */
    public Map<String, Long> unrecognisedRolesByCount() {
        Map<String, Long> result = new TreeMap<>();
        unrecognisedRoles.forEach((role, count) -> result.put(role, count.get()));
        return result;
    }

    private static String displayName(JsonNode identity) {
        String value = text(identity, "displayName");
        return value == null ? "" : value;
    }

    private static String text(JsonNode node, String field) {
        if (node == null || node.isNull()) {
            return null;
        }
        JsonNode value = node.get(field);
        if (value == null || value.isNull()) {
            return null;
        }
        String text = value.asText();
        return text == null || text.isBlank() ? null : text.trim();
    }

    private static List<JsonNode> arrayOf(JsonNode node, String field) {
        List<JsonNode> items = new ArrayList<>();
        if (node == null) {
            return items;
        }
        JsonNode value = node.get(field);
        if (value != null && value.isArray()) {
            value.forEach(items::add);
        }
        return items;
    }

    /** A principal plus what it is, so the structured rules can say user or group. */
    private record Principal(String identity, String type, String displayName) {}

    /** What one entry produced, and which categories it fell into. */
    private record Emitted(List<Principal> principals,
                           boolean directory,
                           boolean siteLocal,
                           boolean group,
                           boolean user,
                           boolean everyone) {

        static Emitted none() {
            return new Emitted(List.of(), false, false, false, false, false);
        }
    }

    /** What to do about an item whose permissions could not be read. */
    public enum AclFallback {

        /** Do not ingest it. The default. */
        FAIL_CLOSED,

        /**
         * Ingest it readable by every authenticated caller. Only correct for a corpus that is already
         * public to everyone who can reach the search endpoint, and it has to be chosen explicitly.
         */
        PUBLIC;

        public static AclFallback of(String value) {
            if (value == null || value.isBlank()) {
                return FAIL_CLOSED;
            }
            return "public".equalsIgnoreCase(value.trim()) ? PUBLIC : FAIL_CLOSED;
        }
    }

    /** What to do about a grant to a group. */
    public enum GroupGrants {

        /** Emit {@code GROUP_<objectId>} faithfully. The default. */
        MAP,

        /**
         * Omit group principals entirely, so an item granted only to a group has empty read principals and
         * is fail-closed per item.
         *
         * <p>There is deliberately no third option that turns a group grant into a tenant-wide one.</p>
         */
        SKIP;

        public static GroupGrants of(String value) {
            if (value == null || value.isBlank()) {
                return MAP;
            }
            return "skip".equalsIgnoreCase(value.trim()) ? SKIP : MAP;
        }
    }
}
