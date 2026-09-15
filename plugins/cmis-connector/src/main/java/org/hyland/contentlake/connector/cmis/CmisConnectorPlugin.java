package org.hyland.contentlake.connector.cmis;

import org.hyland.contentlake.spi.ConnectorContext;
import org.hyland.contentlake.spi.ConnectorPlugin;
import org.hyland.contentlake.spi.ConnectorSchema;
import org.hyland.contentlake.spi.ContentSourceClient;
import org.hyland.contentlake.spi.ScopeResolver;

import java.util.List;

/**
 * A Content Lake source for any CMIS 1.1 repository (#125).
 *
 * <p>Alfresco and Nuxeo have purpose-built adapters, and that is the right choice for both: it is what
 * gives them change events, scope aspects, ACL expansion and audit watermarks. The consequence is that a
 * repository with no adapter of its own is unsupported, and CMIS is the one interface several of them
 * share. This complements those adapters rather than competing with them.</p>
 *
 * <h3>What it does not do, stated up front</h3>
 * <ul>
 *   <li><strong>Batch only.</strong> CMIS has no change-event mechanism this connector uses, so there is
 *       no live path. Re-ingestion is a batch pass, and unchanged content is skipped by the host's
 *       content-reuse check rather than by anything here.</li>
 *   <li><strong>ACL support is repository-dependent.</strong> CMIS makes ACL access an optional
 *       capability. Where the repository reports one, permissions are read per document and mapped; where
 *       it does not, this connector fails closed and ingests nothing readable by anyone but the sync
 *       account. See {@link CmisAclMapper}.</li>
 *   <li><strong>No aspect-based scope.</strong> There is no CMIS equivalent of {@code cl:indexed}, so
 *       scope is path and MIME patterns ({@link CmisScopeResolver}).</li>
 * </ul>
 *
 * <p>Shipped as a jar rather than an in-tree module group, which is what #124's plugin mechanism is for:
 * the browser binding, the client bindings and their transitive dependencies are shaded into it, while
 * the SPI and Spring come from the host.</p>
 */
public class CmisConnectorPlugin implements ConnectorPlugin {

    /** Prefix of {@code cin_sourceId} for every document this connector ingests. */
    static final String SOURCE_TYPE = "cmis";

    static final String URL_SETTING = SOURCE_TYPE + ".url";
    static final String BINDING_SETTING = SOURCE_TYPE + ".binding";
    static final String REPOSITORY_ID_SETTING = SOURCE_TYPE + ".repository-id";
    static final String USERNAME_SETTING = SOURCE_TYPE + ".username";
    static final String PASSWORD_SETTING = SOURCE_TYPE + ".password";
    static final String ROOT_PATH_SETTING = SOURCE_TYPE + ".root-path";
    static final String SOURCE_ID_SETTING = SOURCE_TYPE + ".source-id";
    static final String INCLUDE_PATHS_SETTING = SOURCE_TYPE + ".include-paths";
    static final String EXCLUDE_PATHS_SETTING = SOURCE_TYPE + ".exclude-paths";
    static final String INCLUDE_MIME_TYPES_SETTING = SOURCE_TYPE + ".include-mime-types";
    static final String EXCLUDE_MIME_TYPES_SETTING = SOURCE_TYPE + ".exclude-mime-types";
    static final String PAGE_SIZE_SETTING = SOURCE_TYPE + ".page-size";
    static final String ACL_FALLBACK_SETTING = SOURCE_TYPE + ".acl-fallback";

    /** Values of {@link #ACL_FALLBACK_SETTING}; see {@link CmisAclMapper.AclFallback}. */
    private static final List<String> ACL_FALLBACK_VALUES = List.of("fail-closed", "sync-account", "public");

    private static final int DEFAULT_PAGE_SIZE = 100;

    @Override
    public String sourceType() {
        return SOURCE_TYPE;
    }

    @Override
    public String displayName() {
        return "CMIS connector";
    }

    /**
     * What this connector needs.
     *
     * <p>{@code url} is a {@code URL} field, so a value that is not an absolute endpoint is reported by
     * name at startup instead of failing later inside the CMIS session factory. The credentials are
     * {@code secret}, so they are never printed by a validation message or the schema endpoint.</p>
     *
     * <p>{@code repository-id} is optional because a repository that exposes exactly one can be resolved
     * without being told, and requiring it would make a single-repository deployment impossible to
     * configure right except by luck. {@code root-path} is optional for the same reason: absent, the
     * CMIS root folder is where a batch pass starts.</p>
     */
    @Override
    public ConnectorSchema schema() {
        return ConnectorSchema.builder(SOURCE_TYPE)
                .required(URL_SETTING, ConnectorSchema.FieldType.URL,
                        "CMIS service endpoint, e.g. http://host/alfresco/api/-default-/public/cmis/versions/1.1/browser")
                .enumeration(BINDING_SETTING, "CMIS binding to use", false, List.of("browser", "atompub"))
                .optional(REPOSITORY_ID_SETTING, ConnectorSchema.FieldType.STRING,
                        "Repository to ingest; resolved automatically when the endpoint exposes exactly one")
                .secret(USERNAME_SETTING, "Account the connector authenticates as", true)
                .secret(PASSWORD_SETTING, "Password for that account", true)
                .optional(ROOT_PATH_SETTING, ConnectorSchema.FieldType.STRING,
                        "Folder path a batch pass starts from; defaults to the repository root")
                .optional(SOURCE_ID_SETTING, ConnectorSchema.FieldType.STRING,
                        "Source alias stored as the second half of cin_sourceId; defaults to the repository id")
                .optional(INCLUDE_PATHS_SETTING, ConnectorSchema.FieldType.LIST,
                        "Path prefixes to ingest; empty means everything under the root")
                .optional(EXCLUDE_PATHS_SETTING, ConnectorSchema.FieldType.LIST,
                        "Path prefixes to skip, applied after the include patterns")
                .optional(INCLUDE_MIME_TYPES_SETTING, ConnectorSchema.FieldType.LIST,
                        "MIME types to ingest, '*' wildcards allowed; empty means every type")
                .optional(EXCLUDE_MIME_TYPES_SETTING, ConnectorSchema.FieldType.LIST,
                        "MIME types to skip, applied after the include patterns")
                .optional(PAGE_SIZE_SETTING, ConnectorSchema.FieldType.INTEGER,
                        "Children requested per folder listing")
                .enumeration(ACL_FALLBACK_SETTING,
                        "What to do when the repository cannot report ACLs: fail-closed (ingest nothing), "
                                + "sync-account (readable by the configured account only), or public "
                                + "(readable by everyone, for an already-public corpus)",
                        false, ACL_FALLBACK_VALUES)
                .build();
    }

    @Override
    public ContentSourceClient createClient(ConnectorContext context) {
        CmisConnectorSettings settings = new CmisConnectorSettings(
                context.property(URL_SETTING),
                context.enumProperty(BINDING_SETTING, CmisConnectorSettings.Binding.class,
                        CmisConnectorSettings.Binding.BROWSER),
                context.property(REPOSITORY_ID_SETTING),
                context.property(USERNAME_SETTING),
                context.property(PASSWORD_SETTING),
                context.property(ROOT_PATH_SETTING),
                context.property(SOURCE_ID_SETTING),
                context.intProperty(PAGE_SIZE_SETTING, DEFAULT_PAGE_SIZE),
                CmisAclMapper.AclFallback.of(context.property(ACL_FALLBACK_SETTING)));
        return new CmisConnectorClient(settings);
    }

    /**
     * Path and MIME scope, because CMIS has no aspect to carry it.
     *
     * <p>Returned even when nothing is configured: the resolver then admits everything, which is the same
     * decision the host's default makes, and keeping one implementation means the traversal rules for a
     * folder are decided in one place.</p>
     */
    @Override
    public ScopeResolver createScopeResolver(ConnectorContext context, ContentSourceClient client) {
        return new CmisScopeResolver(
                context.listProperty(INCLUDE_PATHS_SETTING),
                context.listProperty(EXCLUDE_PATHS_SETTING),
                context.listProperty(INCLUDE_MIME_TYPES_SETTING),
                context.listProperty(EXCLUDE_MIME_TYPES_SETTING));
    }
}
