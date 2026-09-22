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
 * @param scopes              delegated Graph scopes for {@link AuthMode#DEVICE_CODE}
 * @param tokenCachePath      where the delegated token cache lives, for {@link AuthMode#DEVICE_CODE}
 * @param siteUrl             the site as a human has it, resolved to its document libraries on first use
 * @param siteId              the composite Graph site id, for a caller that already has it
 * @param driveNames          document-library names to take from the site, empty for all of them
 * @param folderPaths         folder paths within a drive to start a pass from, empty for the drive root.
 *                            Unlike {@code includePaths} this scopes the walk rather than filtering it
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
                                          int resourceUnitBurst,
                                          List<String> scopes,
                                          Path tokenCachePath,
                                          String siteUrl,
                                          String siteId,
                                          List<String> driveNames,
                                          List<String> folderPaths) {

    public SharePointConnectorSettings {
        driveIds = driveIds == null ? List.of() : List.copyOf(driveIds);
        includePaths = includePaths == null ? List.of() : List.copyOf(includePaths);
        excludePaths = excludePaths == null ? List.of() : List.copyOf(excludePaths);
        includeMimeTypes = includeMimeTypes == null ? List.of() : List.copyOf(includeMimeTypes);
        excludeMimeTypes = excludeMimeTypes == null ? List.of() : List.copyOf(excludeMimeTypes);
        everyoneClaims = everyoneClaims == null ? Set.of() : Set.copyOf(everyoneClaims);
        permissionsMode = permissionsMode == null ? PermissionsMode.PER_ITEM : permissionsMode;
        scopes = scopes == null ? List.of() : List.copyOf(scopes);
        driveNames = driveNames == null ? List.of() : List.copyOf(driveNames);
        folderPaths = folderPaths == null ? List.of() : List.copyOf(folderPaths);
    }

    /**
     * The source alias, defaulting to the first configured drive so a single-drive run needs no setting.
     *
     * <h3>This must stay free of I/O, and must not depend on a resolved drive list</h3>
     * <p>The sync's first act is to derive the qualified source id from the client, so a Graph call here would
     * fail a job before it started. Worse, deriving it from drives resolved out of a site would mean that a
     * change in the order Graph returns libraries silently renames the source, orphaning its cursor and every
     * document already indexed under the old name. So a site-configured run derives its alias from the site
     * string itself, which is configuration and cannot move.</p>
     *
     * <p>Setting {@code sharepoint.source-id} explicitly is still worth doing with a site, because the slug
     * below is stable but not pretty.</p>
     */
    public String effectiveSourceId() {
        if (sourceId != null && !sourceId.isBlank()) {
            return sourceId.trim();
        }
        if (!driveIds.isEmpty()) {
            return driveIds.get(0);
        }
        String site = siteUrl != null && !siteUrl.isBlank() ? siteUrl : siteId;
        return site == null || site.isBlank() ? "sharepoint" : slug(site);
    }

    /** A source alias that is stable, readable and safe in an id. */
    private static String slug(String value) {
        String cleaned = value.trim()
                .replaceFirst("^[a-zA-Z][a-zA-Z0-9+.-]*://", "")
                .replaceAll("[^A-Za-z0-9]+", "-")
                .replaceAll("^-+|-+$", "");
        return cleaned.isBlank() ? "sharepoint" : cleaned.toLowerCase(java.util.Locale.ROOT);
    }

    /**
     * Builds the token provider this configuration asks for. Does no network I/O.
     *
     * <p>Note the client-credentials scope stays {@code null}, which the provider reads as the app-only
     * {@code .default} form. {@code sharepoint.scopes} is deliberately not threaded into it: app-only tokens
     * are scoped by the permissions granted to the registration, and resource scopes in a client-credentials
     * request are rejected by Entra. Mixing the two settings would turn a configuration mistake into a
     * runtime 400 from the token endpoint rather than something the schema can explain.</p>
     */
    public GraphTokenProvider tokenProvider() {
        return switch (authMode) {
            case STATIC_TOKEN -> new StaticTokenProvider(accessToken);
            case CLIENT_CREDENTIALS -> new ClientCredentialsTokenProvider(authority, clientId, clientSecret,
                    certificate, certificatePassword, null);
            case DEVICE_CODE -> new DeviceCodeTokenProvider(authority, clientId, scopes, tokenCachePath);
        };
    }

    /** How the connector authenticates. */
    public enum AuthMode {

        /** App-only tokens from Entra ID. The right answer for an unattended crawl across a tenant. */
        CLIENT_CREDENTIALS,

        /**
         * A bearer token handed over in configuration, for a local mock run or for validating ACL mapping
         * against a developer's own OneDrive. Not refreshable, so not a deployment mode.
         */
        STATIC_TOKEN,

        /**
         * Delegated tokens for a named user, refreshed silently from a cache a human populated once with
         * {@link SharePointDeviceLogin}. For a tenant that will not issue application permissions.
         *
         * <p>Indexes one identity's view: content the signed-in user cannot read is absent from the index
         * rather than present and unretrievable.</p>
         */
        DEVICE_CODE
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

    /**
     * A builder, because this record now has twenty-five components.
     *
     * <p>A positional constructor of that width is its own hazard: every call site has to count arguments, two
     * adjacent components of the same type can be transposed without the compiler noticing, and adding a
     * setting means editing every construction site whether or not it cares. The plugin builds one of these
     * and tests build several, and a silent transposition between, say, {@code clientSecret} and
     * {@code certificatePassword} would be a genuinely confusing failure.</p>
     *
     * <p>The canonical constructor stays public: it is what the record gives you, and nothing is gained by
     * hiding it.</p>
     */
    public static Builder builder() {
        return new Builder();
    }

    /** Named setters over the canonical constructor. Unset values take the record's own defaults. */
    public static final class Builder {

        private String graphBaseUrl = SharePointConnectorPlugin.DEFAULT_GRAPH_BASE_URL;
        private AuthMode authMode = AuthMode.CLIENT_CREDENTIALS;
        private String authority;
        private String clientId;
        private String clientSecret;
        private Path certificate;
        private String certificatePassword;
        private String accessToken;
        private List<String> driveIds = List.of();
        private String sourceId;
        private List<String> includePaths = List.of();
        private List<String> excludePaths = List.of();
        private List<String> includeMimeTypes = List.of();
        private List<String> excludeMimeTypes = List.of();
        private SharePointAclMapper.AclFallback aclFallback = SharePointAclMapper.AclFallback.FAIL_CLOSED;
        private SharePointAclMapper.GroupGrants groupGrants = SharePointAclMapper.GroupGrants.MAP;
        private PermissionsMode permissionsMode = PermissionsMode.PER_ITEM;
        private Set<String> everyoneClaims = Set.of();
        private int resourceUnitsPerMinute;
        private int resourceUnitBurst = 1;
        private List<String> scopes = List.of();
        private Path tokenCachePath;
        private String siteUrl;
        private String siteId;
        private List<String> driveNames = List.of();
        private List<String> folderPaths = List.of();

        private Builder() {
        }

        public Builder graphBaseUrl(String value) { this.graphBaseUrl = value; return this; }
        public Builder authMode(AuthMode value) { this.authMode = value; return this; }
        public Builder authority(String value) { this.authority = value; return this; }
        public Builder clientId(String value) { this.clientId = value; return this; }
        public Builder clientSecret(String value) { this.clientSecret = value; return this; }
        public Builder certificate(Path value) { this.certificate = value; return this; }
        public Builder certificatePassword(String value) { this.certificatePassword = value; return this; }
        public Builder accessToken(String value) { this.accessToken = value; return this; }
        public Builder driveIds(List<String> value) { this.driveIds = value; return this; }
        public Builder sourceId(String value) { this.sourceId = value; return this; }
        public Builder includePaths(List<String> value) { this.includePaths = value; return this; }
        public Builder excludePaths(List<String> value) { this.excludePaths = value; return this; }
        public Builder includeMimeTypes(List<String> value) { this.includeMimeTypes = value; return this; }
        public Builder excludeMimeTypes(List<String> value) { this.excludeMimeTypes = value; return this; }
        public Builder aclFallback(SharePointAclMapper.AclFallback value) { this.aclFallback = value; return this; }
        public Builder groupGrants(SharePointAclMapper.GroupGrants value) { this.groupGrants = value; return this; }
        public Builder permissionsMode(PermissionsMode value) { this.permissionsMode = value; return this; }
        public Builder everyoneClaims(Set<String> value) { this.everyoneClaims = value; return this; }
        public Builder resourceUnitsPerMinute(int value) { this.resourceUnitsPerMinute = value; return this; }
        public Builder resourceUnitBurst(int value) { this.resourceUnitBurst = value; return this; }
        public Builder scopes(List<String> value) { this.scopes = value; return this; }
        public Builder tokenCachePath(Path value) { this.tokenCachePath = value; return this; }
        public Builder siteUrl(String value) { this.siteUrl = value; return this; }
        public Builder siteId(String value) { this.siteId = value; return this; }
        public Builder driveNames(List<String> value) { this.driveNames = value; return this; }
        public Builder folderPaths(List<String> value) { this.folderPaths = value; return this; }

        public SharePointConnectorSettings build() {
            return new SharePointConnectorSettings(graphBaseUrl, authMode, authority, clientId, clientSecret,
                    certificate, certificatePassword, accessToken, driveIds, sourceId, includePaths,
                    excludePaths, includeMimeTypes, excludeMimeTypes, aclFallback, groupGrants,
                    permissionsMode, everyoneClaims, resourceUnitsPerMinute, resourceUnitBurst, scopes,
                    tokenCachePath, siteUrl, siteId, driveNames, folderPaths);
        }
    }
}
