package org.hyland.contentlake.connector.sharepoint;

import org.hyland.contentlake.spi.ConnectorContext;
import org.hyland.contentlake.spi.ConnectorPlugin;
import org.hyland.contentlake.spi.ConnectorSchema;
import org.hyland.contentlake.spi.ContentSourceClient;
import org.hyland.contentlake.spi.ScopeResolver;
import org.hyland.contentlake.spi.TextExtractor;

import java.nio.file.Path;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * A Content Lake source for SharePoint Online, through Microsoft Graph.
 *
 * <p>Graph is the only interface: Microsoft deprecated the SharePoint CMIS producer years ago, so the CMIS
 * connector next door is not a route to SharePoint. Graph is also cloud-only, which puts SharePoint Server
 * on premises out of scope entirely rather than merely untested; that would need a separate client against
 * the CSOM-shaped {@code _api/web/...} service.</p>
 *
 * <h3>What it does not do, stated up front</h3>
 * <ul>
 *   <li><strong>Batch only.</strong> Graph supports webhook subscriptions on a drive, but there is no live
 *       host for a plugin connector and the SPI has no hook for one. Incremental passes come from the change
 *       feed instead, which is a batch pass that reads {@code delta} rather than walking.</li>
 *   <li><strong>A document granted only to an Entra group needs the group resolver switched on.</strong>
 *       Ingestion records the group faithfully, and {@code rag-service} expands a caller's membership at
 *       query time only where {@code rag.security.entra.enabled} is true. Without it such a document is
 *       retrievable by nobody, so the run reports how many are affected rather than leaving it to be
 *       discovered.</li>
 *   <li><strong>Site-local principals are retrievable by nobody, whatever is configured.</strong> A
 *       {@code siteUser} or {@code siteGroup} grant with no directory identity alongside it names an
 *       identity that exists only inside the site collection, and no Entra resolver can expand one. Counted
 *       and reported separately for that reason.</li>
 *   <li><strong>Permissions are read per item by default, at five resource units each</strong>, which is
 *       about six units per document all in. {@code sharepoint.permissions-mode=hierarchical} reads them
 *       only where the sharing hierarchy says they are set, at about one unit per document, and needs
 *       {@code Sites.FullControl.All}. The choice is never inferred: whether a tenant honours the
 *       {@code Prefer} header decides whether a crawl is bounded near 200,000 or 1,000,000 documents a
 *       day.</li>
 *   <li><strong>No server-side extraction.</strong> {@link #createTextExtractor} returns null, so the host's
 *       in-process Tika chain is used unless {@code EXTRACTION_ENGINE_URLS} is set. Graph does not convert
 *       content the way Nuxeo does.</li>
 * </ul>
 *
 * <h3>Every setting that decides whether this can work is required</h3>
 * <p>The host validates each mounted jar's schema against the environment and refuses a plugin whose
 * required settings are missing, without failing the application. That refusal is what keeps an unconfigured
 * SharePoint jar from loading and becoming a second candidate in a deployment running every connector, so
 * making the credentials optional would be actively worse than the error line the refusal logs.</p>
 */
public class SharePointConnectorPlugin implements ConnectorPlugin {

    /** Prefix of {@code cin_sourceId} for every document this connector ingests. */
    static final String SOURCE_TYPE = SharePointConnectorClient.SOURCE_TYPE;

    static final String GRAPH_BASE_URL_SETTING = SOURCE_TYPE + ".graph-base-url";
    static final String AUTH_MODE_SETTING = SOURCE_TYPE + ".auth-mode";
    static final String TENANT_ID_SETTING = SOURCE_TYPE + ".tenant-id";
    static final String AUTHORITY_SETTING = SOURCE_TYPE + ".authority";
    static final String CLIENT_ID_SETTING = SOURCE_TYPE + ".client-id";
    static final String CLIENT_SECRET_SETTING = SOURCE_TYPE + ".client-secret";
    static final String CERTIFICATE_PATH_SETTING = SOURCE_TYPE + ".certificate-path";
    static final String CERTIFICATE_PASSWORD_SETTING = SOURCE_TYPE + ".certificate-password";
    static final String ACCESS_TOKEN_SETTING = SOURCE_TYPE + ".access-token";
    static final String DRIVE_IDS_SETTING = SOURCE_TYPE + ".drive-ids";
    static final String SOURCE_ID_SETTING = SOURCE_TYPE + ".source-id";
    static final String INCLUDE_PATHS_SETTING = SOURCE_TYPE + ".include-paths";
    static final String EXCLUDE_PATHS_SETTING = SOURCE_TYPE + ".exclude-paths";
    static final String INCLUDE_MIME_TYPES_SETTING = SOURCE_TYPE + ".include-mime-types";
    static final String EXCLUDE_MIME_TYPES_SETTING = SOURCE_TYPE + ".exclude-mime-types";
    static final String ACL_FALLBACK_SETTING = SOURCE_TYPE + ".acl-fallback";
    static final String GROUP_GRANTS_SETTING = SOURCE_TYPE + ".group-grants";
    static final String PERMISSIONS_MODE_SETTING = SOURCE_TYPE + ".permissions-mode";
    static final String EVERYONE_CLAIMS_SETTING = SOURCE_TYPE + ".everyone-claims";
    static final String RESOURCE_UNITS_PER_MINUTE_SETTING = SOURCE_TYPE + ".resource-units-per-minute";
    static final String RESOURCE_UNIT_BURST_SETTING = SOURCE_TYPE + ".resource-unit-burst";
    static final String SCOPES_SETTING = SOURCE_TYPE + ".scopes";
    static final String TOKEN_CACHE_PATH_SETTING = SOURCE_TYPE + ".token-cache-path";
    static final String SITE_URL_SETTING = SOURCE_TYPE + ".site-url";
    static final String SITE_ID_SETTING = SOURCE_TYPE + ".site-id";
    static final String DRIVE_NAMES_SETTING = SOURCE_TYPE + ".drive-names";
    static final String FOLDER_PATHS_SETTING = SOURCE_TYPE + ".folder-paths";

    static final String DEFAULT_GRAPH_BASE_URL = "https://graph.microsoft.com/v1.0";
    static final String DEFAULT_AUTHORITY_HOST = "https://login.microsoftonline.com/";

    /**
     * Below the documented per-application per-tenant cap of 1,250 units a minute, on purpose.
     *
     * <p>The cap is shared by everything using the same app registration, so aiming exactly at it pushes the
     * whole application into throttling rather than stopping short of it.
     */
    private static final int DEFAULT_RESOURCE_UNITS_PER_MINUTE = 1_000;
    private static final int DEFAULT_RESOURCE_UNIT_BURST = 200;

    @Override
    public String sourceType() {
        return SOURCE_TYPE;
    }

    @Override
    public String displayName() {
        return "SharePoint Online connector";
    }

    @Override
    public ConnectorSchema schema() {
        return ConnectorSchema.builder(SOURCE_TYPE)
                .optional(DRIVE_IDS_SETTING, ConnectorSchema.FieldType.LIST,
                        "Drive ids to ingest, comma separated. Node ids are '<driveId>:<itemId>', so this "
                                + "is what a run is scoped to. Optional now that a site can be named instead: "
                                + "supply this or " + SITE_URL_SETTING + " or " + SITE_ID_SETTING)
                .optional(SITE_URL_SETTING, ConnectorSchema.FieldType.URL,
                        "The site as a human has it, e.g. https://contoso.sharepoint.com/sites/lake. Resolved "
                                + "to its document libraries on first use, so drive ids need not be found out "
                                + "of band")
                .optional(SITE_ID_SETTING, ConnectorSchema.FieldType.STRING,
                        "The composite Graph site id (hostname,siteGuid,webGuid), for a caller that already "
                                + "has it and wants to skip the URL lookup")
                .optional(DRIVE_NAMES_SETTING, ConnectorSchema.FieldType.LIST,
                        "Document-library names to take from the site, comma separated. Empty takes every "
                                + "library. Ignored when " + DRIVE_IDS_SETTING + " is set")
                .required(CLIENT_ID_SETTING, ConnectorSchema.FieldType.STRING,
                        "Application (client) id of the Entra ID app registration")
                .optional(TENANT_ID_SETTING, ConnectorSchema.FieldType.STRING,
                        "Directory (tenant) id; the authority is derived from it. Required for "
                                + "client-credentials unless " + AUTHORITY_SETTING + " is set")
                .optional(AUTHORITY_SETTING, ConnectorSchema.FieldType.URL,
                        "Entra ID authority, only for a sovereign cloud. Must be https: msal4j rejects any "
                                + "other scheme")
                .enumeration(AUTH_MODE_SETTING,
                        "How to authenticate: client-credentials (app-only, for an unattended crawl across a "
                                + "tenant), device-code (delegated tokens for a named user, refreshed from the "
                                + "cache at " + TOKEN_CACHE_PATH_SETTING + "; indexes that user's view only), "
                                + "or static-token (a token supplied directly, for a local mock run or for "
                                + "validating ACL mapping against your own OneDrive; it cannot be refreshed)",
                        false, List.of("client-credentials", "device-code", "static-token"))
                .secret(CLIENT_SECRET_SETTING,
                        "Client secret. Supply this or " + CERTIFICATE_PATH_SETTING + ", never both", false)
                .optional(CERTIFICATE_PATH_SETTING, ConnectorSchema.FieldType.STRING,
                        "PKCS#12 client certificate, preferred over a secret because a secret expires on a "
                                + "date nobody diarises")
                .secret(CERTIFICATE_PASSWORD_SETTING, "Password for that certificate", false)
                .secret(ACCESS_TOKEN_SETTING,
                        "Bearer token for auth-mode=static-token. Development only", false)
                .secret(TOKEN_CACHE_PATH_SETTING,
                        "File holding the msal4j token cache for auth-mode=device-code. Required for that "
                                + "mode. Marked secret because the file contains a refresh token, which "
                                + "outlives the access tokens it mints. Populate it on the host with "
                                + SharePointDeviceLogin.COMMAND_HINT + " and mount it read-only", false)
                .optional(SCOPES_SETTING, ConnectorSchema.FieldType.LIST,
                        "Delegated Graph scopes for auth-mode=device-code, comma separated; defaults to "
                                + "Sites.Read.All plus offline_access. Ignored by the other modes, because an "
                                + "app-only token is scoped by the registration's granted permissions and "
                                + "Entra rejects resource scopes in a client-credentials request")
                .optional(GRAPH_BASE_URL_SETTING, ConnectorSchema.FieldType.URL,
                        "Graph endpoint; defaults to " + DEFAULT_GRAPH_BASE_URL + ". Point it at the mock "
                                + "Graph service to run without a tenant")
                .optional(SOURCE_ID_SETTING, ConnectorSchema.FieldType.STRING,
                        "Source alias stored as the second half of cin_sourceId; defaults to the first drive id")
                .optional(FOLDER_PATHS_SETTING, ConnectorSchema.FieldType.LIST,
                        "Folder paths within a drive to start a pass from, comma separated, e.g. /Finance. "
                                + "Unlike " + INCLUDE_PATHS_SETTING + " this scopes the walk rather than "
                                + "filtering it afterwards, so an unselected folder is never enumerated and "
                                + "never costs resource units")
                .optional(INCLUDE_PATHS_SETTING, ConnectorSchema.FieldType.LIST,
                        "Path prefixes to ingest; empty means everything in the configured drives")
                .optional(EXCLUDE_PATHS_SETTING, ConnectorSchema.FieldType.LIST,
                        "Path prefixes to skip, applied after the include patterns")
                .optional(INCLUDE_MIME_TYPES_SETTING, ConnectorSchema.FieldType.LIST,
                        "MIME types to ingest, '*' wildcards allowed; empty means every type")
                .optional(EXCLUDE_MIME_TYPES_SETTING, ConnectorSchema.FieldType.LIST,
                        "MIME types to skip, applied after the include patterns")
                .enumeration(ACL_FALLBACK_SETTING,
                        "What to do about an item whose permissions cannot be read: fail-closed (do not "
                                + "ingest it) or public (readable by everyone, for an already-public corpus)",
                        false, List.of("fail-closed", "public"))
                .enumeration(GROUP_GRANTS_SETTING,
                        "What to do about a grant to a group: map (emit GROUP_<objectId> faithfully) or skip "
                                + "(omit group principals, so a group-only item is readable by nobody). There "
                                + "is deliberately no option that widens a group grant to the whole tenant",
                        false, List.of("map", "skip"))
                .enumeration(PERMISSIONS_MODE_SETTING,
                        "How to obtain an item's permissions: per-item (the default, one 5-unit call per "
                                + "item, about six resource units per document) or hierarchical (read them "
                                + "only where the sharing hierarchy says they are set, about one unit per "
                                + "document). Hierarchical needs the Sites.FullControl.All application "
                                + "permission, and refuses to run rather than degrade if the tenant does not "
                                + "honour it",
                        false, List.of("per-item", "hierarchical"))
                .optional(EVERYONE_CLAIMS_SETTING, ConnectorSchema.FieldType.LIST,
                        "Display names that mean every user in the tenant, for a tenant that words 'Everyone "
                                + "except external users' differently. Empty uses the documented English names")
                .optional(RESOURCE_UNITS_PER_MINUTE_SETTING, ConnectorSchema.FieldType.INTEGER,
                        "Graph resource units to spend per minute, below the per-application cap of 1250. "
                                + "Zero disables metering, which is only right against a mock")
                .optional(RESOURCE_UNIT_BURST_SETTING, ConnectorSchema.FieldType.INTEGER,
                        "Resource units allowed to accumulate, so a burst is not paced to the average")
                .build();
    }

    @Override
    public ContentSourceClient createClient(ConnectorContext context) {
        return new SharePointConnectorClient(settingsFrom(context));
    }

    @Override
    public ScopeResolver createScopeResolver(ConnectorContext context, ContentSourceClient client) {
        return new SharePointScopeResolver(
                context.listProperty(INCLUDE_PATHS_SETTING),
                context.listProperty(EXCLUDE_PATHS_SETTING),
                context.listProperty(INCLUDE_MIME_TYPES_SETTING),
                context.listProperty(EXCLUDE_MIME_TYPES_SETTING));
    }

    /**
     * No extractor of its own, so the host's chain is used.
     *
     * <p>Graph will not convert a binary to text on the server the way Nuxeo's conversion service does, so
     * there is nothing to return and claiming otherwise would cost a download plus a failed conversion per
     * document.</p>
     */
    @Override
    public TextExtractor createTextExtractor(ConnectorContext context) {
        return null;
    }

    /**
     * Reads the settings, failing here rather than at first use.
     *
     * <p>The one thing this must not do is any network I/O: the host builds a client for every mounted jar
     * whose schema validates, so a connector that opened a session while being constructed would slow or
     * break a deployment that is running a different connector entirely.</p>
     */
    SharePointConnectorSettings settingsFrom(ConnectorContext context) {
        SharePointConnectorSettings.AuthMode authMode = authMode(context);
        String tenantId = context.property(TENANT_ID_SETTING);
        String authority = context.property(AUTHORITY_SETTING);

        // Every mode that talks to Entra needs an authority. Only static-token does not, because it never
        // acquires anything. Testing for that one rather than listing the others means a mode added later
        // gets the check by default instead of silently running with a null authority.
        boolean acquiresTokens = authMode != SharePointConnectorSettings.AuthMode.STATIC_TOKEN;

        if (acquiresTokens && (authority == null || authority.isBlank())) {
            if (tenantId == null || tenantId.isBlank()) {
                // Conditionally required, which ConnectorSchema cannot express: the tenant id is needed by
                // the modes that acquire a token and meaningless for static-token, so naming it here is the
                // only way an operator gets told which of the two settings to supply.
                throw new GraphException("auth-mode is " + settingValue(authMode) + ", so either "
                        + TENANT_ID_SETTING + " or " + AUTHORITY_SETTING + " must be set");
            }
            authority = DEFAULT_AUTHORITY_HOST + tenantId.trim();
        }

        String tokenCachePath = context.property(TOKEN_CACHE_PATH_SETTING);
        if (authMode == SharePointConnectorSettings.AuthMode.DEVICE_CODE
                && (tokenCachePath == null || tokenCachePath.isBlank())) {
            // Same reason: conditionally required. Without it the mode has nowhere to read the refresh token
            // a sign-in produced, and the failure would otherwise surface on the first Graph call instead of
            // at load, where it names the setting.
            throw new GraphException("auth-mode is device-code, so " + TOKEN_CACHE_PATH_SETTING
                    + " must be set. Populate it on the host with " + SharePointDeviceLogin.COMMAND_HINT
                    + " and mount the file read-only.");
        }

        // Conditionally required, which ConnectorSchema cannot express: exactly one of three ways to say what
        // to ingest. Leaving drive-ids unconditionally required would make naming a site impossible; making
        // none of them required would let the jar load and then ingest nothing, which is the failure this
        // check exists to convert into a message naming the settings.
        boolean hasDrives = !context.listProperty(DRIVE_IDS_SETTING).isEmpty();
        boolean hasSite = isSet(context.property(SITE_URL_SETTING)) || isSet(context.property(SITE_ID_SETTING));
        if (!hasDrives && !hasSite) {
            throw new GraphException("Nothing says what to ingest: set " + DRIVE_IDS_SETTING + ", or "
                    + SITE_URL_SETTING + " or " + SITE_ID_SETTING + " to have the libraries resolved for you.");
        }

        String certificatePath = context.property(CERTIFICATE_PATH_SETTING);

        return SharePointConnectorSettings.builder()
                .graphBaseUrl(context.property(GRAPH_BASE_URL_SETTING, DEFAULT_GRAPH_BASE_URL))
                .authMode(authMode)
                .authority(authority)
                .clientId(context.property(CLIENT_ID_SETTING))
                .clientSecret(context.property(CLIENT_SECRET_SETTING))
                .certificate(pathOrNull(certificatePath))
                .certificatePassword(context.property(CERTIFICATE_PASSWORD_SETTING))
                .accessToken(context.property(ACCESS_TOKEN_SETTING))
                .driveIds(context.listProperty(DRIVE_IDS_SETTING))
                .sourceId(context.property(SOURCE_ID_SETTING))
                .includePaths(context.listProperty(INCLUDE_PATHS_SETTING))
                .excludePaths(context.listProperty(EXCLUDE_PATHS_SETTING))
                .includeMimeTypes(context.listProperty(INCLUDE_MIME_TYPES_SETTING))
                .excludeMimeTypes(context.listProperty(EXCLUDE_MIME_TYPES_SETTING))
                .aclFallback(SharePointAclMapper.AclFallback.of(context.property(ACL_FALLBACK_SETTING)))
                .groupGrants(SharePointAclMapper.GroupGrants.of(context.property(GROUP_GRANTS_SETTING)))
                .permissionsMode(SharePointConnectorSettings.PermissionsMode.of(
                        context.property(PERMISSIONS_MODE_SETTING)))
                .everyoneClaims(everyoneClaims(context))
                .resourceUnitsPerMinute(context.intProperty(RESOURCE_UNITS_PER_MINUTE_SETTING,
                        DEFAULT_RESOURCE_UNITS_PER_MINUTE))
                .resourceUnitBurst(context.intProperty(RESOURCE_UNIT_BURST_SETTING,
                        DEFAULT_RESOURCE_UNIT_BURST))
                .scopes(context.listProperty(SCOPES_SETTING))
                .tokenCachePath(pathOrNull(tokenCachePath))
                .siteUrl(context.property(SITE_URL_SETTING))
                .siteId(context.property(SITE_ID_SETTING))
                .driveNames(context.listProperty(DRIVE_NAMES_SETTING))
                .folderPaths(context.listProperty(FOLDER_PATHS_SETTING))
                .build();
    }

    private static Path pathOrNull(String value) {
        return value == null || value.isBlank() ? null : Path.of(value.trim());
    }

    private static boolean isSet(String value) {
        return value != null && !value.isBlank();
    }

    /**
     * Parses the auth mode, falling back to the app-only one for anything unrecognised.
     *
     * <p>The fallback is not the error path an operator sees: {@code auth-mode} is an {@code ENUM} schema
     * field, so a typo is reported by schema validation naming the allowed values, before this runs. Any new
     * mode must be added to that list in {@link #schema()} as well as here, or the jar is refused at load
     * with a schema problem rather than reaching this parse at all.</p>
     */
    private static SharePointConnectorSettings.AuthMode authMode(ConnectorContext context) {
        String value = context.property(AUTH_MODE_SETTING);
        if (value == null || value.isBlank()) {
            return SharePointConnectorSettings.AuthMode.CLIENT_CREDENTIALS;
        }
        return switch (value.trim().toLowerCase(java.util.Locale.ROOT)) {
            case "static-token", "static_token" -> SharePointConnectorSettings.AuthMode.STATIC_TOKEN;
            case "device-code", "device_code" -> SharePointConnectorSettings.AuthMode.DEVICE_CODE;
            default -> SharePointConnectorSettings.AuthMode.CLIENT_CREDENTIALS;
        };
    }

    /** The setting spelling of a mode, for an error message an operator has to act on. */
    private static String settingValue(SharePointConnectorSettings.AuthMode authMode) {
        return switch (authMode) {
            case CLIENT_CREDENTIALS -> "client-credentials";
            case DEVICE_CODE -> "device-code";
            case STATIC_TOKEN -> "static-token";
        };
    }

    private static Set<String> everyoneClaims(ConnectorContext context) {
        return new LinkedHashSet<>(context.listProperty(EVERYONE_CLAIMS_SETTING));
    }
}
