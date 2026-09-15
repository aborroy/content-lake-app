package org.hyland.contentlake.connector.cmis;

import org.apache.chemistry.opencmis.commons.data.Ace;
import org.apache.chemistry.opencmis.commons.data.Acl;
import org.apache.chemistry.opencmis.commons.data.AclCapabilities;
import org.apache.chemistry.opencmis.commons.data.PermissionMapping;
import org.apache.chemistry.opencmis.commons.enums.CapabilityAcl;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Turns a CMIS ACL into the read principals the ingestion pipeline stores.
 *
 * <p>CMIS makes ACL access an <em>optional capability</em>, which is the whole difficulty: the same
 * connector talks to a repository that reports every ACE and to one that reports none, and the second
 * must not quietly produce world-readable documents. What a repository supports is
 * {@link CapabilityAcl}: {@code NONE}, {@code DISCOVER} (readable) or {@code MANAGE} (readable and
 * writable). Only the last two can be mapped.</p>
 *
 * <h3>Which permission counts as read</h3>
 *
 * <p>CMIS defines three basic permissions ({@code cmis:read}, {@code cmis:write}, {@code cmis:all}) and
 * lets a repository define its own, so an Alfresco ACE says {@code Consumer} and a Documentum one says
 * something else again. Both are handled: the basic permissions are recognised directly, and a
 * repository-specific one is resolved through the repository's own {@code permissionMapping}, asking
 * which permissions it says allow {@code canGetProperties.Object} and {@code canViewContent.Object}.
 * That mapping is the repository describing its permission model in CMIS terms, which is exactly the
 * question being asked, and it means no permission name is hardcoded per vendor.</p>
 *
 * <p>An unrecognised permission grants nothing. The failure mode of guessing generously is a restricted
 * document becoming searchable by someone who cannot open it in the source, which is the one outcome
 * worth designing against; the failure mode of guessing conservatively is a document nobody retrieves,
 * which is visible and fixable.</p>
 *
 * <h3>Principal encoding</h3>
 *
 * <p>Principal ids pass through unchanged, because the pipeline is what namespaces them per source
 * ({@code <authority>_#_<sourceId>}) and it reads a {@code GROUP_} prefix as "this is a group". A
 * repository whose groups do not carry that prefix therefore produces user-shaped principals, which
 * matches fewer documents rather than more. The one id that is rewritten is the repository's own
 * "anyone" principal, mapped to {@code GROUP_EVERYONE} so the pipeline stores the un-namespaced
 * {@code __Everyone__} that means "any authenticated caller". The anonymous principal is deliberately
 * not mapped to it: anonymous is not everyone.</p>
 */
final class CmisAclMapper {

    /** The three permissions CMIS itself defines. */
    private static final String CMIS_READ = "cmis:read";
    private static final String CMIS_WRITE = "cmis:write";
    private static final String CMIS_ALL = "cmis:all";

    /** The authority the pipeline turns into the un-namespaced {@code __Everyone__} principal. */
    static final String EVERYONE_AUTHORITY = "GROUP_EVERYONE";

    private final CapabilityAcl capability;
    private final AclFallback fallback;
    private final String syncAccount;
    private final String anyonePrincipalId;
    private final Set<String> readGrantingPermissions;

    CmisAclMapper(CapabilityAcl capability,
                  AclCapabilities aclCapabilities,
                  AclFallback fallback,
                  String syncAccount,
                  String anyonePrincipalId) {
        this.capability = capability == null ? CapabilityAcl.NONE : capability;
        this.fallback = fallback == null ? AclFallback.FAIL_CLOSED : fallback;
        this.syncAccount = syncAccount;
        this.anyonePrincipalId = anyonePrincipalId;
        this.readGrantingPermissions = readGrantingPermissions(aclCapabilities);
    }

    /** Whether this repository can report ACLs at all. */
    boolean canReadAcls() {
        return capability == CapabilityAcl.DISCOVER || capability == CapabilityAcl.MANAGE;
    }

    /**
     * What a document with no readable ACL is granted to, or {@code null} when the deployment has asked
     * not to ingest such documents at all.
     *
     * <p>{@code null} is the fail-closed answer and the default. The caller turns it into a refusal, so
     * nothing is ingested from a repository whose permissions cannot be read until an operator has said
     * in configuration what should happen instead. Ingesting them as unreadable would be defensible too,
     * but it spends extraction and embedding on documents no query can return and leaves no trace of the
     * decision.</p>
     */
    Set<String> fallbackPrincipals() {
        return switch (fallback) {
            case FAIL_CLOSED -> null;
            case SYNC_ACCOUNT -> syncAccount == null || syncAccount.isBlank() ? null : Set.of(syncAccount);
            case PUBLIC -> Set.of(EVERYONE_AUTHORITY);
        };
    }

    /**
     * The principals granted read on one document.
     *
     * @param acl the document's ACL, or {@code null} when the repository did not return one
     * @return read principals, or {@link #fallbackPrincipals()} when the ACL could not be read. Never a
     *         silently widened set: an ACL that is present but grants read to nobody returns empty.
     */
    Set<String> readPrincipals(Acl acl) {
        if (!canReadAcls() || acl == null || acl.getAces() == null) {
            return fallbackPrincipals();
        }

        Set<String> principals = new LinkedHashSet<>();
        for (Ace ace : acl.getAces()) {
            if (ace == null || !grantsRead(ace)) {
                continue;
            }
            String principal = normalizePrincipal(ace.getPrincipalId());
            if (principal != null) {
                principals.add(principal);
            }
        }
        return principals;
    }

    /**
     * Whether this ACL grants read to anyone the mapper can recognise.
     *
     * <p>Used to decide whether an ACL that arrived with the object is usable as it stands, or whether the
     * vendor-neutral form has to be fetched. Measured against Alfresco: an ACL fetched with repository
     * permissions carries
     * {@code {http://www.alfresco.org/model/content/1.0}cmobject.Consumer}, which is a permission
     * <em>group</em>, while the repository's own {@code permissionMapping} lists the low-level
     * {@code base.ReadProperties} that group contains. Nothing in CMIS expands one into the other, so an
     * ACL in that form reads as granting nothing at all.</p>
     */
    boolean grantsAnyRead(Acl acl) {
        if (acl == null || acl.getAces() == null) {
            return false;
        }
        for (Ace ace : acl.getAces()) {
            if (ace != null && grantsRead(ace)) {
                return true;
            }
        }
        return false;
    }

    private boolean grantsRead(Ace ace) {
        List<String> permissions = ace.getPermissions();
        if (permissions == null) {
            return false;
        }
        for (String permission : permissions) {
            if (permission == null) {
                continue;
            }
            String candidate = permission.trim();
            if (CMIS_READ.equalsIgnoreCase(candidate)
                    || CMIS_WRITE.equalsIgnoreCase(candidate)
                    || CMIS_ALL.equalsIgnoreCase(candidate)
                    || readGrantingPermissions.contains(candidate.toLowerCase(Locale.ROOT))) {
                return true;
            }
        }
        return false;
    }

    private String normalizePrincipal(String principalId) {
        if (principalId == null || principalId.isBlank()) {
            return null;
        }
        String trimmed = principalId.trim();
        if (anyonePrincipalId != null && anyonePrincipalId.equalsIgnoreCase(trimmed)) {
            return EVERYONE_AUTHORITY;
        }
        return trimmed;
    }

    /**
     * The repository-specific permissions its own mapping says allow reading a document's properties or
     * its content.
     */
    private static Set<String> readGrantingPermissions(AclCapabilities aclCapabilities) {
        Map<String, PermissionMapping> mapping =
                aclCapabilities == null ? null : aclCapabilities.getPermissionMapping();
        if (mapping == null || mapping.isEmpty()) {
            return Set.of();
        }

        Set<String> permissions = new LinkedHashSet<>();
        for (String key : List.of(PermissionMapping.CAN_GET_PROPERTIES_OBJECT,
                PermissionMapping.CAN_VIEW_CONTENT_OBJECT)) {
            PermissionMapping entry = mapping.get(key);
            if (entry == null || entry.getPermissions() == null) {
                continue;
            }
            for (String permission : entry.getPermissions()) {
                if (permission != null && !permission.isBlank()) {
                    permissions.add(permission.trim().toLowerCase(Locale.ROOT));
                }
            }
        }
        return Set.copyOf(permissions);
    }

    /** What to do about a document whose ACL the repository will not report. */
    enum AclFallback {

        /** Ingest nothing. The default, and what makes an unmappable repository a startup failure. */
        FAIL_CLOSED,

        /** Readable by the account the connector authenticates as, and nobody else. */
        SYNC_ACCOUNT,

        /**
         * Readable by every authenticated caller. Only correct for a corpus that is already public to
         * everyone who can reach the search endpoint, and it has to be chosen explicitly.
         */
        PUBLIC;

        static AclFallback of(String value) {
            if (value == null || value.isBlank()) {
                return FAIL_CLOSED;
            }
            return switch (value.trim().toLowerCase(Locale.ROOT)) {
                case "sync-account", "sync_account" -> SYNC_ACCOUNT;
                case "public" -> PUBLIC;
                default -> FAIL_CLOSED;
            };
        }
    }
}
