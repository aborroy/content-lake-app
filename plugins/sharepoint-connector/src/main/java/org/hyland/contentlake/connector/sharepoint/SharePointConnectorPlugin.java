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
                .required(DRIVE_IDS_SETTING, ConnectorSchema.FieldType.LIST,
                        "Drive ids to ingest, comma separated. Node ids are '<driveId>:<itemId>', so this "
                                + "is what a run is scoped to")
                .required(CLIENT_ID_SETTING, ConnectorSchema.FieldType.STRING,
                        "Application (client) id of the Entra ID app registration")
                .optional(TENANT_ID_SETTING, ConnectorSchema.FieldType.STRING,
                        "Directory (tenant) id; the authority is derived from it. Required for "
                                + "client-credentials unless " + AUTHORITY_SETTING + " is set")
                .optional(AUTHORITY_SETTING, ConnectorSchema.FieldType.URL,
                        "Entra ID authority, only for a sovereign cloud. Must be https: msal4j rejects any "
                                + "other scheme")
                .enumeration(AUTH_MODE_SETTING,
                        "How to authenticate: client-credentials (app-only, the only supported deployment "
                                + "mode) or static-token (a token supplied directly, for a local mock run or "
                                + "for validating ACL mapping against your own OneDrive; it cannot be "
                                + "refreshed)",
                        false, List.of("client-credentials", "static-token"))
                .secret(CLIENT_SECRET_SETTING,
                        "Client secret. Supply this or " + CERTIFICATE_PATH_SETTING + ", never both", false)
                .optional(CERTIFICATE_PATH_SETTING, ConnectorSchema.FieldType.STRING,
                        "PKCS#12 client certificate, preferred over a secret because a secret expires on a "
                                + "date nobody diarises")
                .secret(CERTIFICATE_PASSWORD_SETTING, "Password for that certificate", false)
                .secret(ACCESS_TOKEN_SETTING,
                        "Bearer token for auth-mode=static-token. Development only", false)
                .optional(GRAPH_BASE_URL_SETTING, ConnectorSchema.FieldType.URL,
                        "Graph endpoint; defaults to " + DEFAULT_GRAPH_BASE_URL + ". Point it at the mock "
                                + "Graph service to run without a tenant")
                .optional(SOURCE_ID_SETTING, ConnectorSchema.FieldType.STRING,
                        "Source alias stored as the second half of cin_sourceId; defaults to the first drive id")
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

        if (authMode == SharePointConnectorSettings.AuthMode.CLIENT_CREDENTIALS
                && (authority == null || authority.isBlank())) {
            if (tenantId == null || tenantId.isBlank()) {
                // Conditionally required, which ConnectorSchema cannot express: the tenant id is needed for
                // client-credentials and meaningless for static-token, so naming it here is the only way an
                // operator gets told which of the two settings to supply.
                throw new GraphException("auth-mode is client-credentials, so either " + TENANT_ID_SETTING
                        + " or " + AUTHORITY_SETTING + " must be set");
            }
            authority = DEFAULT_AUTHORITY_HOST + tenantId.trim();
        }

        String certificatePath = context.property(CERTIFICATE_PATH_SETTING);

        return new SharePointConnectorSettings(
                context.property(GRAPH_BASE_URL_SETTING, DEFAULT_GRAPH_BASE_URL),
                authMode,
                authority,
                context.property(CLIENT_ID_SETTING),
                context.property(CLIENT_SECRET_SETTING),
                certificatePath == null || certificatePath.isBlank() ? null : Path.of(certificatePath.trim()),
                context.property(CERTIFICATE_PASSWORD_SETTING),
                context.property(ACCESS_TOKEN_SETTING),
                context.listProperty(DRIVE_IDS_SETTING),
                context.property(SOURCE_ID_SETTING),
                context.listProperty(INCLUDE_PATHS_SETTING),
                context.listProperty(EXCLUDE_PATHS_SETTING),
                context.listProperty(INCLUDE_MIME_TYPES_SETTING),
                context.listProperty(EXCLUDE_MIME_TYPES_SETTING),
                SharePointAclMapper.AclFallback.of(context.property(ACL_FALLBACK_SETTING)),
                SharePointAclMapper.GroupGrants.of(context.property(GROUP_GRANTS_SETTING)),
                SharePointConnectorSettings.PermissionsMode.of(context.property(PERMISSIONS_MODE_SETTING)),
                everyoneClaims(context),
                context.intProperty(RESOURCE_UNITS_PER_MINUTE_SETTING, DEFAULT_RESOURCE_UNITS_PER_MINUTE),
                context.intProperty(RESOURCE_UNIT_BURST_SETTING, DEFAULT_RESOURCE_UNIT_BURST));
    }

    private static SharePointConnectorSettings.AuthMode authMode(ConnectorContext context) {
        String value = context.property(AUTH_MODE_SETTING);
        if (value == null || value.isBlank()) {
            return SharePointConnectorSettings.AuthMode.CLIENT_CREDENTIALS;
        }
        return switch (value.trim().toLowerCase(java.util.Locale.ROOT)) {
            case "static-token", "static_token" -> SharePointConnectorSettings.AuthMode.STATIC_TOKEN;
            default -> SharePointConnectorSettings.AuthMode.CLIENT_CREDENTIALS;
        };
    }

    private static Set<String> everyoneClaims(ConnectorContext context) {
        return new LinkedHashSet<>(context.listProperty(EVERYONE_CLAIMS_SETTING));
    }
}
