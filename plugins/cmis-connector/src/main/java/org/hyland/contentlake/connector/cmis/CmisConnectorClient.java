package org.hyland.contentlake.connector.cmis;

import org.apache.chemistry.opencmis.client.api.CmisObject;
import org.apache.chemistry.opencmis.client.api.Document;
import org.apache.chemistry.opencmis.client.api.FileableCmisObject;
import org.apache.chemistry.opencmis.client.api.Folder;
import org.apache.chemistry.opencmis.client.api.ItemIterable;
import org.apache.chemistry.opencmis.client.api.OperationContext;
import org.apache.chemistry.opencmis.client.api.Session;
import org.apache.chemistry.opencmis.client.api.SessionFactory;
import org.apache.chemistry.opencmis.client.runtime.SessionFactoryImpl;
import org.apache.chemistry.opencmis.commons.SessionParameter;
import org.apache.chemistry.opencmis.commons.data.Acl;
import org.apache.chemistry.opencmis.commons.data.ContentStream;
import org.apache.chemistry.opencmis.commons.data.RepositoryInfo;
import org.apache.chemistry.opencmis.commons.enums.BindingType;
import org.apache.chemistry.opencmis.commons.exceptions.CmisObjectNotFoundException;
import org.hyland.contentlake.spi.ContentSourceClient;
import org.hyland.contentlake.spi.SourceNode;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Reads content and metadata from a CMIS repository. Node ids are CMIS object ids.
 *
 * <p>The session is opened on first use rather than in the constructor, so a plugin whose repository is
 * not up yet is still loaded and reported by {@code GET /api/connectors} instead of vanishing behind a
 * connection error at startup. Once opened it is reused: a CMIS session caches the repository info and
 * type definitions, and re-authenticating per call would dominate the cost of a batch pass.</p>
 *
 * <h3>ACLs are asked for inline and then verified</h3>
 * <p>Every read goes through an {@link OperationContext} with {@code includeAcls} set, which is free when
 * the repository honours it. It often does not, and the ACL that does arrive may carry permission names
 * only that repository understands, so the read decision falls back to an explicit basic-permission
 * fetch. {@link #aclOf} documents both cases and what was measured.</p>
 *
 * <h3>Failing closed on a repository that cannot report ACLs</h3>
 * <p>Checked once, when the session opens. If the repository reports {@code CapabilityAcl.NONE} and the
 * deployment has not chosen a fallback, the connector refuses to run and says which setting to set. It
 * would be easy to ingest such documents as unreadable instead, but that spends a pass producing
 * documents no query can return, and it is the sort of decision that must be visible rather than
 * inferred from an empty result set.</p>
 */
public class CmisConnectorClient implements ContentSourceClient {

    private static final Logger log = Logger.getLogger(CmisConnectorClient.class.getName());

    /** Kept small: OpenCMIS pages internally and the host asks for its own page sizes anyway. */
    private static final int PROPERTY_STRING_LIMIT = 4_000;

    private final CmisConnectorSettings settings;
    private final Supplier<Session> sessionSupplier;

    private volatile Session session;
    private volatile OperationContext readContext;
    private volatile CmisAclMapper aclMapper;
    private volatile String resolvedSourceId;

    public CmisConnectorClient(CmisConnectorSettings settings) {
        this(settings, null);
    }

    /**
     * @param sessionSupplier opens the CMIS session, or {@code null} to open one from {@code settings}.
     *                        Injected by tests, which is what keeps every behaviour here reachable
     *                        without a live repository.
     */
    CmisConnectorClient(CmisConnectorSettings settings, Supplier<Session> sessionSupplier) {
        this.settings = settings;
        this.sessionSupplier = sessionSupplier != null ? sessionSupplier : () -> openSession(settings);
    }

    @Override
    public String getSourceId() {
        if (settings.sourceId() != null && !settings.sourceId().isBlank()) {
            return settings.sourceId();
        }
        String resolved = resolvedSourceId;
        if (resolved == null) {
            // The repository id is the natural default: it is stable, and it distinguishes two CMIS
            // sources in one deployment without asking an operator to invent an alias.
            resolved = session().getRepositoryInfo().getId();
            resolvedSourceId = resolved;
        }
        return resolved;
    }

    @Override
    public String getSourceType() {
        return CmisConnectorPlugin.SOURCE_TYPE;
    }

    /**
     * Where a batch pass starts: the configured folder path, or the repository root.
     *
     * <p>A repository always has a root, so unlike most connectors this never returns {@code null} and a
     * deployment needs no {@code connector.roots}. A configured path that does not exist is an error
     * rather than an empty pass, because the alternative reads as "the source is empty".</p>
     */
    @Override
    public String getRootNodeId() {
        String path = settings.rootPath();
        if (path == null || path.isBlank()) {
            return session().getRootFolder().getId();
        }
        CmisObject object = session().getObjectByPath(path.trim(), readContext());
        if (!(object instanceof Folder folder)) {
            throw new IllegalStateException(CmisConnectorPlugin.ROOT_PATH_SETTING + " '" + path
                    + "' is not a folder in repository " + session().getRepositoryInfo().getId());
        }
        return folder.getId();
    }

    @Override
    public SourceNode getNode(String nodeId) {
        try {
            return toSourceNode(session().getObject(nodeId, readContext()));
        } catch (CmisObjectNotFoundException e) {
            // Null is "gone", which the pipeline reconciles as a deletion rather than reporting as a
            // failure. Any other CMIS exception propagates: it means the repository is unhealthy, and
            // treating that as "every document was deleted" would empty the index.
            return null;
        }
    }

    @Override
    public List<SourceNode> getChildren(String containerId, int skip, int maxItems) {
        CmisObject object;
        try {
            object = session().getObject(containerId, readContext());
        } catch (CmisObjectNotFoundException e) {
            return List.of();
        }
        if (!(object instanceof Folder folder)) {
            return List.of();
        }

        int limit = maxItems > 0 ? maxItems : settings.pageSize();
        OperationContext context = session().createOperationContext();
        context.setIncludeAcls(aclMapper().canReadAcls());
        context.setIncludePathSegments(false);
        context.setMaxItemsPerPage(limit);

        ItemIterable<CmisObject> children = folder.getChildren(context);
        if (skip > 0) {
            children = children.skipTo(skip);
        }

        List<SourceNode> page = new ArrayList<>();
        for (CmisObject child : children.getPage(limit)) {
            SourceNode node = toSourceNode(child);
            if (node != null) {
                page.add(node);
            }
        }
        return page;
    }

    @Override
    public Resource downloadContent(String nodeId, String fileName) {
        ContentStream stream = contentStream(nodeId);
        if (stream == null || stream.getStream() == null) {
            throw new IllegalStateException("CMIS object " + nodeId + " has no content stream");
        }
        try (InputStream in = stream.getStream()) {
            // A temp file rather than an in-memory resource: the pipeline hands this to an extractor
            // that may stream a large binary, and it deletes what it gets when it is done.
            Path temp = Files.createTempFile("cmis-connector-", "-" + safeSuffix(fileName, stream));
            Files.copy(in, temp, StandardCopyOption.REPLACE_EXISTING);
            return new FileSystemResource(temp);
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot download CMIS object " + nodeId, e);
        }
    }

    @Override
    public byte[] getContent(String nodeId) {
        ContentStream stream = contentStream(nodeId);
        if (stream == null || stream.getStream() == null) {
            return new byte[0];
        }
        try (InputStream in = stream.getStream()) {
            return in.readAllBytes();
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot read content of CMIS object " + nodeId, e);
        }
    }

    // ------------------------------------------------------------------
    // Mapping
    // ------------------------------------------------------------------

    /**
     * A CMIS object as a {@link SourceNode}, or {@code null} when it is neither a document nor a folder.
     *
     * <p>Relationships, policies and items are the things skipped. They carry no content the pipeline
     * could index, and a repository that exposes them as children of a folder would otherwise have them
     * ingested as empty documents.</p>
     */
    SourceNode toSourceNode(CmisObject object) {
        if (object == null) {
            return null;
        }
        boolean folder = object instanceof Folder;
        if (!folder && !(object instanceof Document)) {
            return null;
        }

        Set<String> readPrincipals = aclMapper().readPrincipals(aclOf(object));
        if (readPrincipals == null) {
            // Only reachable with fail-closed configured, which the session check already refuses, so
            // this is the belt to that braces: never emit a node whose permissions are unknown.
            throw new IllegalStateException("CMIS object " + object.getId()
                    + " has no readable ACL and " + CmisConnectorPlugin.ACL_FALLBACK_SETTING
                    + " is fail-closed");
        }

        String mimeType = null;
        if (object instanceof Document document) {
            mimeType = document.getContentStreamMimeType();
        }

        return new SourceNode(
                object.getId(),
                getSourceId(),
                CmisConnectorPlugin.SOURCE_TYPE,
                object.getName(),
                pathOf(object, folder),
                mimeType,
                lastModified(object),
                folder,
                readPrincipals,
                Set.of(),
                sourceProperties(object, folder));
    }

    /**
     * The ACL to base the read decision on, in a form whose permissions mean the same thing everywhere.
     *
     * <p>Two measured properties of a real repository (Alfresco 26.2, browser binding) shape this, and
     * both are traps:</p>
     * <ul>
     *   <li><strong>A folder listing carries no ACLs at all</strong>, whatever {@code includeAcls} asks
     *       for. Verified: the same object fetched on its own comes back with an {@code acl} field, and
     *       fetched as a child of its parent does not. So a connector that reads ACLs only from listed
     *       objects sees none for any document it discovered by walking, which under fail-closed is every
     *       document.</li>
     *   <li><strong>The ACL that does arrive carries repository permissions, not CMIS ones.</strong> An
     *       ACE reads {@code {http://www.alfresco.org/model/content/1.0}cmobject.Consumer}, a permission
     *       group, and no CMIS call expands a group into the low-level permissions the repository's
     *       {@code permissionMapping} names. Asking for basic permissions instead
     *       ({@code getAcl(object, true)}) returns {@code cmis:read} for the same ACE, which is the
     *       vendor-neutral answer CMIS defines for exactly this purpose.</li>
     * </ul>
     *
     * <p>Hence: use an inline ACL when it already grants a read this mapper recognises, and otherwise ask
     * for the basic-permission form. One extra request per document on a repository like Alfresco, none on
     * one that reports basic permissions inline, and a read decision that does not depend on knowing a
     * vendor's permission names.</p>
     */
    private Acl aclOf(CmisObject object) {
        if (!aclMapper().canReadAcls()) {
            return null;
        }
        Acl inline = object.getAcl();
        if (aclMapper().grantsAnyRead(inline)) {
            return inline;
        }
        try {
            return session().getAcl(object, true);
        } catch (RuntimeException e) {
            log.warning(() -> "Could not read the ACL of CMIS object " + object.getId() + ": "
                    + e.getMessage());
            return inline;
        }
    }

    /**
     * The node's path, and for a document its parent folder's path.
     *
     * <p>That asymmetry matches the other connectors: {@code SourceNode.path()} is the container a
     * document sits in. CMIS multi-filing means a document can have several paths; the first is used, and
     * the rest are kept in {@code cmis_paths} so nothing is silently lost. The host's discovery visits
     * each node id once regardless, so a multi-filed document is ingested once.</p>
     */
    private static String pathOf(CmisObject object, boolean folder) {
        if (folder) {
            return ((Folder) object).getPath();
        }
        List<String> paths = pathsOf(object);
        if (paths.isEmpty()) {
            // An unfiled document. Legal in CMIS, and the pipeline treats a null path as "flat source".
            return null;
        }
        String first = paths.get(0);
        int lastSlash = first.lastIndexOf('/');
        return lastSlash > 0 ? first.substring(0, lastSlash) : "/";
    }

    private static List<String> pathsOf(CmisObject object) {
        if (!(object instanceof FileableCmisObject fileable)) {
            return List.of();
        }
        try {
            List<String> paths = fileable.getPaths();
            return paths == null ? List.of() : paths;
        } catch (RuntimeException e) {
            // Some repositories refuse getPaths for an unfiled or version-specific object.
            return List.of();
        }
    }

    private static OffsetDateTime lastModified(CmisObject object) {
        GregorianCalendar modified = object.getLastModificationDate();
        if (modified == null) {
            // Null makes the pipeline treat the node as always stale, which re-extracts and re-embeds it
            // on every pass. Worth knowing about, hence the log, but not worth failing an ingest over.
            log.log(Level.FINE, () -> "CMIS object " + object.getId() + " reports no modification date");
            return null;
        }
        return modified.toZonedDateTime().toOffsetDateTime().withOffsetSameInstant(ZoneOffset.UTC);
    }

    /**
     * CMIS metadata worth keeping, under a {@code cmis_} namespace as the SPI expects.
     *
     * <p>Deliberately a small, fixed set rather than every property the repository returns: the whole
     * property bag would put vendor-specific keys of unbounded size into {@code cin_ingestProperties},
     * which is indexed.</p>
     */
    private Map<String, Object> sourceProperties(CmisObject object, boolean folder) {
        Map<String, Object> properties = new LinkedHashMap<>();
        properties.put("cmis_objectId", object.getId());
        properties.put("cmis_objectTypeId", object.getType() == null ? null : object.getType().getId());
        properties.put("cmis_baseTypeId", object.getBaseTypeId() == null ? null : object.getBaseTypeId().value());
        putIfPresent(properties, "cmis_createdBy", object.getCreatedBy());
        putIfPresent(properties, "cmis_lastModifiedBy", object.getLastModifiedBy());
        properties.put("cmis_repositoryId", session().getRepositoryInfo().getId());

        List<String> paths = pathsOf(object);
        if (!paths.isEmpty()) {
            properties.put("cmis_paths", paths);
        }
        if (!folder && object instanceof Document document) {
            properties.put("cmis_contentStreamLength", document.getContentStreamLength());
            putIfPresent(properties, "cmis_versionLabel", document.getVersionLabel());
        }
        properties.values().removeIf(java.util.Objects::isNull);
        return properties;
    }

    private static void putIfPresent(Map<String, Object> properties, String key, String value) {
        if (value != null && !value.isBlank()) {
            properties.put(key, value.length() > PROPERTY_STRING_LIMIT
                    ? value.substring(0, PROPERTY_STRING_LIMIT)
                    : value);
        }
    }

    // ------------------------------------------------------------------
    // Session
    // ------------------------------------------------------------------

    Session session() {
        Session current = session;
        if (current == null) {
            synchronized (this) {
                current = session;
                if (current == null) {
                    current = sessionSupplier.get();
                    session = current;
                    aclMapper = buildAclMapper(current);
                }
            }
        }
        return current;
    }

    private CmisAclMapper aclMapper() {
        session();
        return aclMapper;
    }

    private OperationContext readContext() {
        OperationContext current = readContext;
        if (current == null) {
            current = session().createOperationContext();
            current.setIncludeAcls(aclMapper().canReadAcls());
            current.setIncludePathSegments(false);
            current.setMaxItemsPerPage(settings.pageSize());
            readContext = current;
        }
        return current;
    }

    /**
     * Builds the ACL mapper and, with it, applies the fail-closed rule once per session.
     *
     * @throws IllegalStateException when the repository cannot report ACLs and no fallback was chosen
     */
    private CmisAclMapper buildAclMapper(Session opened) {
        RepositoryInfo info = opened.getRepositoryInfo();
        CmisAclMapper mapper = new CmisAclMapper(
                info.getCapabilities() == null ? null : info.getCapabilities().getAclCapability(),
                info.getAclCapabilities(),
                settings.aclFallback(),
                settings.username(),
                info.getPrincipalIdAnyone());

        if (!mapper.canReadAcls()) {
            if (mapper.fallbackPrincipals() == null) {
                throw new IllegalStateException("Repository " + info.getId()
                        + " reports no CMIS ACL capability, so this connector cannot tell who may read a "
                        + "document. Refusing to ingest. Set " + CmisConnectorPlugin.ACL_FALLBACK_SETTING
                        + " to sync-account (readable by " + settings.username()
                        + " only) or public (readable by everyone) to proceed deliberately.");
            }
            log.warning(() -> "Repository " + info.getId() + " reports no CMIS ACL capability; every "
                    + "document will be ingested with " + CmisConnectorPlugin.ACL_FALLBACK_SETTING
                    + "=" + settings.aclFallback());
        }
        return mapper;
    }

    private ContentStream contentStream(String nodeId) {
        CmisObject object = session().getObject(nodeId, readContext());
        if (!(object instanceof Document document)) {
            return null;
        }
        return document.getContentStream();
    }

    private static Session openSession(CmisConnectorSettings settings) {
        SessionFactory factory = SessionFactoryImpl.newInstance();
        Map<String, String> parameters = new HashMap<>();
        parameters.put(SessionParameter.USER, settings.username());
        parameters.put(SessionParameter.PASSWORD, settings.password());
        // Every call goes to the configured endpoint, not to the host the repository advertises in its
        // service document. See CmisEndpointHttpInvoker: without this the session opens and the next call
        // dials the ingester container's own port 80.
        parameters.put(SessionParameter.HTTP_INVOKER_CLASS, CmisEndpointHttpInvoker.class.getName());

        if (settings.binding() == CmisConnectorSettings.Binding.ATOMPUB) {
            parameters.put(SessionParameter.BINDING_TYPE, BindingType.ATOMPUB.value());
            parameters.put(SessionParameter.ATOMPUB_URL, settings.url());
        } else {
            parameters.put(SessionParameter.BINDING_TYPE, BindingType.BROWSER.value());
            parameters.put(SessionParameter.BROWSER_URL, settings.url());
        }

        if (settings.repositoryId() != null && !settings.repositoryId().isBlank()) {
            parameters.put(SessionParameter.REPOSITORY_ID, settings.repositoryId().trim());
            return factory.createSession(parameters);
        }

        // No repository id configured: resolvable only when the endpoint exposes exactly one, and worth
        // naming the alternatives when it does not, since the id is otherwise hard to find.
        List<org.apache.chemistry.opencmis.client.api.Repository> repositories =
                factory.getRepositories(parameters);
        if (repositories == null || repositories.isEmpty()) {
            throw new IllegalStateException("No CMIS repository at " + settings.url());
        }
        if (repositories.size() > 1) {
            List<String> ids = repositories.stream()
                    .map(org.apache.chemistry.opencmis.client.api.Repository::getId)
                    .toList();
            throw new IllegalStateException("The endpoint at " + settings.url() + " exposes "
                    + repositories.size() + " repositories " + ids + "; set "
                    + CmisConnectorPlugin.REPOSITORY_ID_SETTING + " to one of them");
        }
        return repositories.get(0).createSession();
    }

    /** Keeps the extension, which extractors use to sniff the format. */
    private static String safeSuffix(String fileName, ContentStream stream) {
        String name = (fileName == null || fileName.isBlank()) ? stream.getFileName() : fileName;
        if (name == null || name.isBlank()) {
            return "content";
        }
        String sanitised = name.replaceAll("[^A-Za-z0-9._-]", "_").replaceAll("\\.{2,}", "_");
        return sanitised.isBlank() ? "content" : sanitised;
    }
}
