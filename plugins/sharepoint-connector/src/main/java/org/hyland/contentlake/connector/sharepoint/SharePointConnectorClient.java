package org.hyland.contentlake.connector.sharepoint;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.hyland.contentlake.spi.ConnectorSchema;
import org.hyland.contentlake.spi.ContentSourceClient;
import org.hyland.contentlake.spi.SourceChangePage;
import org.hyland.contentlake.spi.SourceNode;
import org.hyland.contentlake.spi.SourceTombstone;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Reads SharePoint Online through Microsoft Graph.
 *
 * <h3>Node ids are composite</h3>
 * <p>A Graph item id is unique only within a drive, so a node id here is {@code <driveId>:<itemId>}. The
 * split is on the first colon, because a SharePoint drive id contains {@code !} but no colon.</p>
 *
 * <h3>Enumeration is a change feed, and the walk still has to work</h3>
 * <p>Graph documents {@code delta} as the only enumeration guaranteed complete under concurrent writes, and
 * warns that paging a folder's {@code children} collection may miss items if writes happen during the walk.
 * So {@link #supportsChangeFeed()} is true and a {@code deltaLink} becomes the host's cursor.</p>
 *
 * <p>That does not retire {@link #getChildren}. With no stored cursor the host reads
 * {@link #initialCursor()}, walks the source in full, and only then saves the position, so the walk runs on
 * the first pass of every deployment and for the whole life of any deployment that leaves
 * {@code connector.change-feed.enabled} at its default of false.</p>
 *
 * <h3>Why the walk needs a page-link cache</h3>
 * <p>{@code getChildren(containerId, skip, maxItems)} is skip-based; Graph pages drive collections with
 * {@code $top} plus an opaque {@code @odata.nextLink} and does not support {@code $skip}. Passing the host's
 * {@code skip} to Graph would therefore return page one every time, and the host would walk a container
 * until its page limit cut it off, re-ingesting the same children. The bridge is to remember the
 * {@code nextLink} against the {@code skip} at which it will be wanted. It has to be keyed per container,
 * because the host recurses into each child before asking the parent for its next page, so several
 * containers are part-paged at once.</p>
 */
public final class SharePointConnectorClient implements ContentSourceClient {

    private static final Logger log = Logger.getLogger(SharePointConnectorClient.class.getName());

    static final String SOURCE_TYPE = "sharepoint";

    /** Graph's own root alias, and the item id half of the node id a batch pass starts from. */
    static final String ROOT_ITEM_ID = "root";

    /**
     * Cap on remembered page links. A miss is not a failure, only slower: the walk re-pages the container
     * from the start. So dropping the whole map when it grows past this is safe, and it means a crawl of a
     * million items cannot turn this cache into the memory problem the withdrawn item index was.
     */
    private static final int MAX_CACHED_PAGE_LINKS = 10_000;

    private final SharePointConnectorSettings settings;
    private final GraphHttpClient graph;
    private final SharePointAclMapper aclMapper;
    private final ObjectMapper json = new ObjectMapper();

    /** {@code <containerNodeId>@<skip>} to the Graph link that serves that page. */
    private final Map<String, String> pageLinks = new ConcurrentHashMap<>();

    public SharePointConnectorClient(SharePointConnectorSettings settings) {
        this(settings, new GraphHttpClient(settings.graphBaseUrl(), settings.tokenProvider(),
                new ResourceUnitMeter(settings.resourceUnitsPerMinute(), settings.resourceUnitBurst())));
    }

    /** Test seam: lets a test supply a client already pointed at the mock. */
    SharePointConnectorClient(SharePointConnectorSettings settings, GraphHttpClient graph) {
        this.settings = settings;
        this.graph = graph;
        this.aclMapper = new SharePointAclMapper(settings.aclFallback(), settings.groupGrants(),
                settings.everyoneClaims());
        log.info("SharePoint connector ready: " + graph.describe() + ", drives " + settings.driveIds());
    }

    @Override
    public String getSourceId() {
        return settings.effectiveSourceId();
    }

    @Override
    public String getSourceType() {
        return SOURCE_TYPE;
    }

    @Override
    public ConnectorSchema connectorSchema() {
        return new SharePointConnectorPlugin().schema();
    }

    /**
     * Where a batch pass starts, which this can only answer for a single-drive run.
     *
     * <p>With several drives configured there is no one root, and inventing one would mean the host walked
     * whichever drive happened to be first. {@code null} makes the host require {@code connector.roots},
     * which takes composite {@code <driveId>:<itemId>} values.</p>
     */
    @Override
    public String getRootNodeId() {
        List<String> drives = settings.driveIds();
        return drives.size() == 1 ? nodeId(drives.get(0), ROOT_ITEM_ID) : null;
    }

    /**
     * One item, or {@code null} when it must not be ingested.
     *
     * <p>{@code null} rather than an exception for a fail-closed item, because that is the answer the host
     * already understands: {@code ConnectorDiscoveryService} reads a null root as "the connector answered,
     * and the node is not there", records the reason, and marks the pass incomplete so the reconciliation
     * sweep does not treat it as authoritative. Throwing would abort a walk over an item that is merely
     * unreadable.</p>
     */
    @Override
    public SourceNode getNode(String nodeId) {
        Composite composite = Composite.parse(nodeId);
        JsonNode item = graph.getJson(itemPath(composite), ResourceUnitMeter.SINGLE_ITEM, List.of()).body();
        return toSourceNode(composite.driveId(), item);
    }

    /**
     * One page of a container's children, bridging the host's skip-based contract onto Graph's paging.
     *
     * @param skip     advanced by {@code maxItems} per call for a given container, which is what makes the
     *                 page-link cache usable
     * @param maxItems becomes {@code $top}
     */
    @Override
    public List<SourceNode> getChildren(String containerId, int skip, int maxItems) {
        Composite composite = Composite.parse(containerId);
        int top = Math.max(1, maxItems);

        String link = skip <= 0 ? null : pageLinks.get(pageKey(containerId, skip));
        if (skip > 0 && link == null) {
            // A cache miss. Re-page from the start rather than sending Graph a $skip it ignores, which
            // would silently serve page one and make the host loop over the same children.
            link = replayTo(composite, containerId, skip, top);
            if (link == null) {
                return List.of();
            }
        }

        String url = link != null ? link : childrenPath(composite, top);
        GraphHttpClient.GraphResponse response =
                graph.getJson(url, ResourceUnitMeter.MULTI_ITEM_QUERY, List.of());

        String next = GraphHttpClient.text(response.body(), "@odata.nextLink");
        if (next != null) {
            rememberPageLink(containerId, skip + top, next);
        }

        List<SourceNode> children = new ArrayList<>();
        for (JsonNode item : GraphHttpClient.array(response.body(), "value")) {
            SourceNode node = toSourceNode(composite.driveId(), item);
            if (node != null) {
                children.add(node);
            }
        }
        return children;
    }

    /**
     * Walks a container's pages from the start to find the link that serves {@code skip}.
     *
     * <p>Only reached on a cache miss, which the cap on remembered links makes possible. It costs the pages
     * it walks, which is the honest price of the host's contract not matching Graph's paging.</p>
     *
     * @return the link serving {@code skip}, or {@code null} when the container has no such page
     */
    private String replayTo(Composite composite, String containerId, int skip, int top) {
        log.fine(() -> "Re-paging " + containerId + " from the start to reach skip=" + skip);
        String url = childrenPath(composite, top);
        for (int position = 0; position < skip; position += top) {
            GraphHttpClient.GraphResponse response =
                    graph.getJson(url, ResourceUnitMeter.MULTI_ITEM_QUERY, List.of());
            String next = GraphHttpClient.text(response.body(), "@odata.nextLink");
            if (next == null) {
                return null;
            }
            rememberPageLink(containerId, position + top, next);
            url = next;
        }
        return url;
    }

    private void rememberPageLink(String containerId, int skip, String link) {
        if (pageLinks.size() >= MAX_CACHED_PAGE_LINKS) {
            // Bounded and disposable. Losing links costs re-paging, never correctness.
            log.fine("Page-link cache reached its cap; clearing it");
            pageLinks.clear();
        }
        pageLinks.put(pageKey(containerId, skip), link);
    }

    private static String pageKey(String containerId, int skip) {
        return containerId + "@" + skip;
    }

    @Override
    public boolean supportsChangeFeed() {
        return true;
    }

    /**
     * Every configured drive's current delta position, as one opaque cursor.
     *
     * <p>{@code ?token=latest} is what makes this a position rather than a licence to skip anything: it
     * returns an empty page with a delta link, so the host walks the existing corpus and stores this
     * afterwards. A feed opened at "now" would never mention the documents that are already there.</p>
     *
     * <p>Several drives share one cursor because the SPI has one. It is encoded as JSON, which the host
     * stores verbatim and never parses.</p>
     */
    @Override
    public String initialCursor() {
        if (settings.driveIds().isEmpty()) {
            return null;
        }
        Map<String, String> positions = new LinkedHashMap<>();
        for (String driveId : settings.driveIds()) {
            GraphHttpClient.GraphResponse response = graph.getJson(
                    "/drives/" + driveId + "/root/delta?token=latest",
                    ResourceUnitMeter.DELTA_WITH_TOKEN, List.of());
            String deltaLink = GraphHttpClient.text(response.body(), "@odata.deltaLink");
            if (deltaLink == null) {
                log.warning("Drive " + driveId + " returned no deltaLink for token=latest; this source will "
                        + "be walked on every pass rather than read incrementally");
                return null;
            }
            positions.put(driveId, deltaLink);
        }
        return encodeCursor(positions);
    }

    /**
     * One page of changes across every configured drive.
     *
     * <p>An expired token in <em>any</em> drive expires the whole cursor. The SPI cannot express "this drive
     * is fine and that one needs a walk", and reporting a partial page while the host treated the deletion
     * list as authoritative is the failure mode {@code SourceChangePage.expired()} exists to prevent.</p>
     */
    @Override
    public SourceChangePage changesSince(String cursor, int maxItems) {
        Map<String, String> positions = decodeCursor(cursor);
        if (positions.isEmpty()) {
            return SourceChangePage.expired();
        }

        List<SourceNode> changed = new ArrayList<>();
        List<SourceTombstone> deleted = new ArrayList<>();
        Map<String, String> nextPositions = new LinkedHashMap<>();
        boolean moreAvailable = false;

        for (Map.Entry<String, String> position : positions.entrySet()) {
            String driveId = position.getKey();
            GraphHttpClient.GraphResponse response =
                    graph.getJson(position.getValue(), ResourceUnitMeter.DELTA_WITH_TOKEN, List.of());

            if (isResyncRequired(response.body())) {
                log.warning("Drive " + driveId + " reports its delta token expired; the whole cursor is "
                        + "discarded and the host will walk");
                return SourceChangePage.expired();
            }

            for (JsonNode item : GraphHttpClient.array(response.body(), "value")) {
                String itemId = GraphHttpClient.text(item, "id");
                if (itemId == null) {
                    continue;
                }
                if (item.hasNonNull("deleted")) {
                    deleted.add(SourceTombstone.deleted(nodeId(driveId, itemId)));
                    continue;
                }
                SourceNode node = toSourceNode(driveId, item);
                if (node != null) {
                    changed.add(node);
                }
            }

            String next = GraphHttpClient.text(response.body(), "@odata.nextLink");
            String deltaLink = GraphHttpClient.text(response.body(), "@odata.deltaLink");
            if (next != null) {
                nextPositions.put(driveId, next);
                moreAvailable = true;
            } else if (deltaLink != null) {
                nextPositions.put(driveId, deltaLink);
            } else {
                // Neither link. Keeping the position we came in on would replay this page for ever.
                log.warning("Drive " + driveId + " returned a delta page with no nextLink and no deltaLink; "
                        + "the cursor cannot advance and is treated as expired");
                return SourceChangePage.expired();
            }

            if (changed.size() + deleted.size() >= maxItems) {
                // Report what is in hand and let the host come back, rather than draining every drive into
                // one page.
                moreAvailable = moreAvailable || nextPositions.size() < positions.size();
                positions.entrySet().stream()
                        .filter(remaining -> !nextPositions.containsKey(remaining.getKey()))
                        .forEach(remaining -> nextPositions.put(remaining.getKey(), remaining.getValue()));
                break;
            }
        }

        return SourceChangePage.of(changed, deleted, encodeCursor(nextPositions), moreAvailable);
    }

    private static boolean isResyncRequired(JsonNode body) {
        JsonNode error = body == null ? null : body.get("error");
        String code = GraphHttpClient.text(error, "code");
        return code != null && code.toLowerCase(java.util.Locale.ROOT).startsWith("resync");
    }

    @Override
    public Resource downloadContent(String nodeId, String fileName) {
        Composite composite = Composite.parse(nodeId);
        try (InputStream in = graph.getStream(itemPath(composite) + "/content",
                ResourceUnitMeter.CONTENT_DOWNLOAD)) {
            // A temp file rather than an in-memory resource: the pipeline hands this to an extractor that
            // may stream a large binary, and it deletes what it gets when it is done.
            Path temp = Files.createTempFile("sharepoint-connector-", "-" + safeSuffix(fileName));
            Files.copy(in, temp, StandardCopyOption.REPLACE_EXISTING);
            return new FileSystemResource(temp);
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot download SharePoint item " + nodeId, e);
        }
    }

    @Override
    public byte[] getContent(String nodeId) {
        Composite composite = Composite.parse(nodeId);
        try (InputStream in = graph.getStream(itemPath(composite) + "/content",
                ResourceUnitMeter.CONTENT_DOWNLOAD)) {
            return in.readAllBytes();
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot read content of SharePoint item " + nodeId, e);
        }
    }

    /** The ACL mapper's run counters, so a pass can report what it could not make retrievable. */
    public SharePointAclMapper aclMapper() {
        return aclMapper;
    }

    /** One line each for the ACL limitations and the Graph spend. */
    public void logSummary(long documents) {
        aclMapper.logSummary("SharePoint source " + getSourceId());
        graph.meter().logSummary("SharePoint source " + getSourceId(), documents);
    }

    /**
     * Maps a Graph {@code driveItem} to a {@link SourceNode}, reading its permissions.
     *
     * @return {@code null} when the item must not be ingested, which today means only that its ACL could not
     *         be read and the fallback is fail-closed
     */
    private SourceNode toSourceNode(String driveId, JsonNode item) {
        String itemId = GraphHttpClient.text(item, "id");
        if (itemId == null) {
            log.warning("Skipping a driveItem with no id in drive " + driveId);
            return null;
        }
        boolean folder = item.hasNonNull("folder");
        String composite = nodeId(driveId, itemId);

        SharePointAclMapper.MappedAcl acl = readAcl(driveId, itemId);
        if (!acl.ingestable()) {
            log.warning("Not ingesting " + composite + ": its permissions could not be read and "
                    + "sharepoint.acl-fallback is fail-closed");
            return null;
        }

        Map<String, Object> properties = new LinkedHashMap<>();
        properties.put("sharepoint_driveId", driveId);
        properties.put("sharepoint_itemId", itemId);
        putIfPresent(properties, "sharepoint_webUrl", GraphHttpClient.text(item, "webUrl"));
        putIfPresent(properties, "sharepoint_eTag", GraphHttpClient.text(item, "eTag"));
        putIfPresent(properties, "sharepoint_createdDateTime",
                GraphHttpClient.text(item, "createdDateTime"));
        JsonNode parent = item.get("parentReference");
        putIfPresent(properties, "sharepoint_parentItemId", GraphHttpClient.text(parent, "id"));
        putIfPresent(properties, "sharepoint_siteId", GraphHttpClient.text(parent, "siteId"));
        putIfPresent(properties, "sharepoint_createdBy", userDisplayName(item.get("createdBy")));
        putIfPresent(properties, "sharepoint_lastModifiedBy", userDisplayName(item.get("lastModifiedBy")));
        if (acl.hasUniquePermissions()) {
            // Recorded because it is what a permission-hierarchy cache keys on, and because "why is this
            // one document readable by someone else" is a question the index should be able to answer.
            properties.put("sharepoint_uniquePermissions", true);
        }

        return new SourceNode(
                composite,
                getSourceId(),
                SOURCE_TYPE,
                GraphHttpClient.text(item, "name"),
                pathOf(item),
                folder ? null : mimeTypeOf(item),
                parseTimestamp(GraphHttpClient.text(item, "lastModifiedDateTime")),
                folder,
                acl.readPrincipals(),
                // Graph's permissions collection has no deny model; synthesising one would be fiction.
                Set.of(),
                properties,
                acl.securityConfig());
    }

    /**
     * Reads and maps one item's permissions.
     *
     * <p>Per item, which is the naive mode and costs 5 resource units each. The
     * {@code Prefer: hierarchicalsharing} optimisation is a separate change, and it is deliberately not
     * attempted silently: whether a tenant honours that header decides whether a crawl costs about one unit
     * per document or about six, and a connector that assumed the cheap path and got the expensive one would
     * quietly spend the tenant's daily budget.</p>
     */
    private SharePointAclMapper.MappedAcl readAcl(String driveId, String itemId) {
        List<JsonNode> entries = new ArrayList<>();
        String url = "/drives/" + driveId + "/items/" + itemId + "/permissions";
        try {
            while (url != null) {
                GraphHttpClient.GraphResponse response =
                        graph.getJson(url, ResourceUnitMeter.PERMISSIONS, List.of());
                entries.addAll(GraphHttpClient.array(response.body(), "value"));
                // The collection pages, and a truncated one silently drops grants.
                url = GraphHttpClient.text(response.body(), "@odata.nextLink");
            }
        } catch (GraphException e) {
            log.log(Level.WARNING, "Could not read permissions for " + nodeId(driveId, itemId)
                    + "; applying the configured ACL fallback", e);
            return aclMapper.map(List.of(), false);
        }
        return aclMapper.map(entries, true);
    }

    private static void putIfPresent(Map<String, Object> properties, String key, String value) {
        if (value != null && !value.isBlank()) {
            properties.put(key, value);
        }
    }

    private static String userDisplayName(JsonNode identitySet) {
        JsonNode user = identitySet == null ? null : identitySet.get("user");
        return GraphHttpClient.text(user, "displayName");
    }

    /**
     * The item's path, from {@code parentReference.path} plus its name.
     *
     * <p>{@code null} when Graph did not send one, which is the normal case in a delta response: delta omits
     * {@code parentReference.path}, and renaming a folder does not re-emit its descendants. So a path is
     * usable for display and for scope patterns during a walk, and must never be treated as an identity.</p>
     */
    private static String pathOf(JsonNode item) {
        String parentPath = GraphHttpClient.text(item.get("parentReference"), "path");
        String name = GraphHttpClient.text(item, "name");
        if (parentPath == null) {
            return null;
        }
        // Graph writes "/drive/root:/folder"; the part after the colon is the human path.
        int marker = parentPath.indexOf("root:");
        String relative = marker >= 0 ? parentPath.substring(marker + "root:".length()) : parentPath;
        if (relative.isEmpty()) {
            relative = "/";
        }
        return name == null ? relative : (relative.endsWith("/") ? relative + name : relative + "/" + name);
    }

    private static String mimeTypeOf(JsonNode item) {
        return GraphHttpClient.text(item.get("file"), "mimeType");
    }

    private static OffsetDateTime parseTimestamp(String value) {
        if (value == null) {
            return null;
        }
        try {
            return OffsetDateTime.parse(value);
        } catch (DateTimeParseException e) {
            log.warning("Could not parse the Graph timestamp '" + value + "'");
            return null;
        }
    }

    private static String safeSuffix(String fileName) {
        if (fileName == null || fileName.isBlank()) {
            return "download";
        }
        String cleaned = fileName.replaceAll("[^A-Za-z0-9._-]", "_");
        return cleaned.length() > 64 ? cleaned.substring(cleaned.length() - 64) : cleaned;
    }

    private String itemPath(Composite composite) {
        return "/drives/" + composite.driveId() + "/items/" + composite.itemId();
    }

    private String childrenPath(Composite composite, int top) {
        return itemPath(composite) + "/children?$top=" + top;
    }

    static String nodeId(String driveId, String itemId) {
        return driveId + ":" + itemId;
    }

    private String encodeCursor(Map<String, String> positions) {
        try {
            return json.writeValueAsString(positions);
        } catch (IOException e) {
            throw new GraphException("Could not encode the delta cursor", e);
        }
    }

    private Map<String, String> decodeCursor(String cursor) {
        if (cursor == null || cursor.isBlank()) {
            return Map.of();
        }
        try {
            JsonNode parsed = json.readTree(cursor);
            if (!parsed.isObject()) {
                return Map.of();
            }
            Map<String, String> positions = new LinkedHashMap<>();
            parsed.fields().forEachRemaining(field -> positions.put(field.getKey(),
                    field.getValue().asText()));
            return positions;
        } catch (IOException e) {
            // A cursor this connector did not write, or one from an older encoding. Treating it as expired
            // makes the host walk, which is always safe.
            log.warning("Could not read the stored delta cursor; treating it as expired");
            return Map.of();
        }
    }

    /** A node id split into the drive it belongs to and the item within it. */
    record Composite(String driveId, String itemId) {

        static Composite parse(String nodeId) {
            if (nodeId == null || nodeId.isBlank()) {
                throw new GraphException("Empty SharePoint node id");
            }
            int colon = nodeId.indexOf(':');
            if (colon <= 0 || colon == nodeId.length() - 1) {
                throw new GraphException("SharePoint node ids are '<driveId>:<itemId>', but got '"
                        + nodeId + "'");
            }
            return new Composite(nodeId.substring(0, colon), nodeId.substring(colon + 1));
        }
    }
}
