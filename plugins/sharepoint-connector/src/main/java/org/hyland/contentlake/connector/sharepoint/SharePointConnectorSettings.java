package org.hyland.contentlake.connector.sharepoint;

import java.nio.file.Path;
import java.util.List;
import java.util.Set;

/**
 * Everything the SharePoint connector was configured with.
 *
 * <p>Read once by {@link SharePointConnectorPlugin} and passed to the client, so nothing downstream reaches
 * back into the {@code ConnectorContext}. That matters more here than it looks: the host builds a client for
 * every mounted jar whose schema validates, so a connector that read configuration lazily could still be
 * doing it long after startup decided it was fine.</p>
 *
 * @param graphBaseUrl        Graph endpoint, {@code https://graph.microsoft.com/v1.0} against the cloud and
 *                            a local address against the mock. One of the two seams that make a mock run
 *                            evidence about the cloud
 * @param authMode            how to obtain a token
 * @param authority           Entra ID authority, derived from the tenant id unless overridden for a
 *                            sovereign cloud
 * @param clientId            application (client) id of the app registration
 * @param clientSecret        client secret, or {@code null} when using a certificate
 * @param certificate         PKCS#12 client certificate, or {@code null} when using a secret
 * @param certificatePassword password for that certificate
 * @param accessToken         a bearer token supplied directly, for {@link AuthMode#STATIC_TOKEN}
 * @param driveIds            drives to ingest. Composite node ids are {@code <driveId>:<itemId>}, so this is
 *                            what a run is scoped to
 * @param sourceId            the second half of {@code cin_sourceId}; defaults to the first drive id
 * @param includePaths        path prefixes to ingest, empty for everything
 * @param excludePaths        path prefixes to skip, applied after the includes
 * @param includeMimeTypes    MIME types to ingest, {@code *} wildcards allowed, empty for every type
 * @param excludeMimeTypes    MIME types to skip, applied after the includes
 * @param aclFallback         what to do about an item whose permissions cannot be read
 * @param groupGrants         what to do about a grant to a group
 * @param permissionsMode     whether to read every item's permissions or resolve them through the sharing
 *                            hierarchy, which is the difference between about six resource units per
 *                            document and about one
 * @param everyoneClaims      display names that mean "every user in the tenant"; empty for the defaults
 * @param resourceUnitsPerMinute Graph budget to spend per minute, or zero to not meter at all
 * @param resourceUnitBurst   units allowed to accumulate, so a burst is not paced to the average
 */
public record SharePointConnectorSettings(String graphBaseUrl,
                                          AuthMode authMode,
                                          String authority,
                                          String clientId,
                                          String clientSecret,
                                          Path certificate,
                                          String certificatePassword,
                                          String accessToken,
                                          List<String> driveIds,
                                          String sourceId,
                                          List<String> includePaths,
                                          List<String> excludePaths,
                                          List<String> includeMimeTypes,
                                          List<String> excludeMimeTypes,
                                          SharePointAclMapper.AclFallback aclFallback,
                                          SharePointAclMapper.GroupGrants groupGrants,
                                          PermissionsMode permissionsMode,
                                          Set<String> everyoneClaims,
                                          int resourceUnitsPerMinute,
                                          int resourceUnitBurst) {

    public SharePointConnectorSettings {
        driveIds = driveIds == null ? List.of() : List.copyOf(driveIds);
        includePaths = includePaths == null ? List.of() : List.copyOf(includePaths);
        excludePaths = excludePaths == null ? List.of() : List.copyOf(excludePaths);
        includeMimeTypes = includeMimeTypes == null ? List.of() : List.copyOf(includeMimeTypes);
        excludeMimeTypes = excludeMimeTypes == null ? List.of() : List.copyOf(excludeMimeTypes);
        everyoneClaims = everyoneClaims == null ? Set.of() : Set.copyOf(everyoneClaims);
        permissionsMode = permissionsMode == null ? PermissionsMode.PER_ITEM : permissionsMode;
    }

    /** The source alias, defaulting to the first configured drive so a single-drive run needs no setting. */
    public String effectiveSourceId() {
        if (sourceId != null && !sourceId.isBlank()) {
            return sourceId.trim();
        }
        return driveIds.isEmpty() ? "sharepoint" : driveIds.get(0);
    }

    /** Builds the token provider this configuration asks for. Does no network I/O. */
    public GraphTokenProvider tokenProvider() {
        return switch (authMode) {
            case STATIC_TOKEN -> new StaticTokenProvider(accessToken);
            case CLIENT_CREDENTIALS -> new ClientCredentialsTokenProvider(authority, clientId, clientSecret,
                    certificate, certificatePassword, null);
        };
    }

    /** How the connector authenticates. */
    public enum AuthMode {

        /** App-only tokens from Entra ID. The only supported deployment mode. */
        CLIENT_CREDENTIALS,

        /**
         * A bearer token handed over in configuration, for a local mock run or for validating ACL mapping
         * against a developer's own OneDrive. Not refreshable, so not a deployment mode.
         */
        STATIC_TOKEN
    }

    /**
     * How an item's read principals are obtained, which is where a crawl spends most of its budget.
     *
     * <p>Graph charges 5 resource units for any permission operation and documents that {@code permissions}
     * cannot be {@code $expand}ed onto a {@code driveItem} GET, so reading them per item costs about six
     * units per document all in. Resolving them through the sharing hierarchy instead costs roughly one, and
     * moves the bottleneck to the content download, which is where it belongs.</p>
     *
     * <p>Which mode applies is always explicit, never inferred. The cheap mode depends on Graph honouring a
     * {@code Prefer} header that needs {@code Sites.FullControl.All}, and a connector that asked for it,
     * silently did not get it, and carried on would spend the tenant's daily budget five times over to
     * discover that.</p>
     */
    public enum PermissionsMode {

        /**
         * One {@code /permissions} call per item. The default: it needs no privileged {@code Prefer} header,
         * so it works in any tenant that can read the drive at all.
         */
        PER_ITEM,

        /**
         * Read permissions only where the sharing hierarchy says they are set, and inherit the rest.
         *
         * <p>Requires {@code Sites.FullControl.All}, because that is what
         * {@code Prefer: hierarchicalsharing} needs. The connector refuses to run in this mode against a
         * tenant that does not echo the preference back, rather than falling back to the expensive path.</p>
         */
        HIERARCHICAL;

        /**
         * Parses the setting, defaulting to {@link #PER_ITEM}.
         *
         * <p>An unrecognised value reads as {@code per-item}, which costs more and cannot be wrong. The
         * other direction -- a typo silently selecting the mode that needs a privileged header -- would turn
         * a configuration mistake into a startup failure on a tenant that was working.</p>
         */
        public static PermissionsMode of(String value) {
            if (value == null || value.isBlank()) {
                return PER_ITEM;
            }
            return switch (value.trim().toLowerCase(java.util.Locale.ROOT)) {
                case "hierarchical" -> HIERARCHICAL;
                default -> PER_ITEM;
            };
        }

        /** The setting value, for a log line or an error message an operator has to act on. */
        public String settingValue() {
            return this == HIERARCHICAL ? "hierarchical" : "per-item";
        }
    }
}
