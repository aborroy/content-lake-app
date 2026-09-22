package org.hyland.contentlake.connector.sharepoint;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.logging.Logger;

/**
 * Which drives a run covers, resolving a site to its document libraries when it was given one.
 *
 * <p>Before this, {@code sharepoint.drive-ids} was required and there was no supported way to find out what to
 * put in it: the connector never called {@code GET /sites/{id}/drives}, so a drive id was copied out of a Graph
 * Explorer session. That is fine for a developer and is not a configuration story, and it also made a folder
 * picker impossible, since a UI cannot offer a tree for a site whose libraries the backend cannot enumerate.</p>
 *
 * <h3>Lazy and memoised, and that is not an optimisation</h3>
 * <p>Resolution must not happen during construction. The host builds a client for every mounted jar whose
 * schema validates, across all six ingesters, so a Graph call there would let one connector's outage break a
 * deployment running a different one. It must not happen per pass either: the host asks for roots on every
 * pass, so an unmemoised lookup would spend the tenant's budget on the same two calls forever.</p>
 *
 * <p>So: resolved on first use, held afterwards, and reached only from paths that are allowed to do I/O.
 * {@code getSourceId()} and the settings record stay clear of it, because the sync's first act is to derive the
 * qualified source id and a Graph blip there would fail the job before it started.</p>
 */
final class SharePointDriveCatalog {

    private static final Logger log = Logger.getLogger(SharePointDriveCatalog.class.getName());

    /** The only kind of drive worth walking. A site also exposes personal and system drives. */
    private static final String DOCUMENT_LIBRARY = "documentLibrary";

    private final SharePointConnectorSettings settings;
    private final GraphHttpClient graph;

    /** Written once under the monitor, read freely afterwards. */
    private volatile List<String> resolved;
    private volatile String siteDisplayName;

    SharePointDriveCatalog(SharePointConnectorSettings settings, GraphHttpClient graph) {
        this.settings = settings;
        this.graph = graph;
    }

    /**
     * The drive ids to walk.
     *
     * <p>Explicitly configured ids win and cost nothing: an operator who named specific libraries meant it, and
     * a run configured that way makes no Graph call here at all, which is what keeps every existing deployment
     * and the mock-based suite behaving exactly as before.</p>
     */
    List<String> driveIds() {
        List<String> known = resolved;
        if (known != null) {
            return known;
        }
        synchronized (this) {
            if (resolved == null) {
                resolved = resolve();
            }
            return resolved;
        }
    }

    /** The site's display name once resolved, or {@code null} when no site was configured. */
    String siteDisplayName() {
        return siteDisplayName;
    }

    /** Whether asking for drive ids would reach the network, so a caller on a pure path can avoid it. */
    boolean resolvesFromConfiguration() {
        return !settings.driveIds().isEmpty();
    }

    private List<String> resolve() {
        if (!settings.driveIds().isEmpty()) {
            return settings.driveIds();
        }

        String siteId = settings.siteId();
        if (siteId == null || siteId.isBlank()) {
            siteId = resolveSiteId(settings.siteUrl());
        }

        return librariesOf(siteId.trim());
    }

    /**
     * Turns a site URL into the composite id Graph addresses a site by.
     *
     * <p>Note the addressing form: {@code /sites/{hostname}:/{server-relative-path}}, colon-delimited rather
     * than a query. It is easy to write as a query and get a 400 that does not explain itself.</p>
     */
    private String resolveSiteId(String siteUrl) {
        if (siteUrl == null || siteUrl.isBlank()) {
            throw new GraphException("Neither " + SharePointConnectorPlugin.DRIVE_IDS_SETTING + " nor "
                    + SharePointConnectorPlugin.SITE_URL_SETTING + " nor "
                    + SharePointConnectorPlugin.SITE_ID_SETTING + " is set, so there is nothing to ingest");
        }

        java.net.URI uri;
        try {
            uri = java.net.URI.create(siteUrl.trim());
        } catch (IllegalArgumentException e) {
            throw new GraphException(SharePointConnectorPlugin.SITE_URL_SETTING + " '" + siteUrl
                    + "' is not a usable URL", e);
        }
        String host = uri.getHost();
        if (host == null || host.isBlank()) {
            throw new GraphException(SharePointConnectorPlugin.SITE_URL_SETTING + " '" + siteUrl
                    + "' has no host; it should look like https://contoso.sharepoint.com/sites/lake");
        }
        String path = uri.getPath() == null ? "" : uri.getPath().replaceAll("^/+", "").replaceAll("/+$", "");

        String address = path.isBlank()
                ? "/sites/" + host
                : "/sites/" + host + ":/" + path;

        GraphHttpClient.GraphResponse response;
        try {
            response = graph.getJson(address, ResourceUnitMeter.SINGLE_ITEM, List.of());
        } catch (GraphException e) {
            throw new GraphException("Could not resolve " + SharePointConnectorPlugin.SITE_URL_SETTING + " '"
                    + siteUrl + "' through " + address + ": " + e.getMessage(), e);
        }

        String id = GraphHttpClient.text(response.body(), "id");
        if (id == null || id.isBlank()) {
            throw new GraphException("Graph answered for site '" + siteUrl + "' without an id");
        }
        siteDisplayName = GraphHttpClient.text(response.body(), "displayName");
        log.info("Resolved site '" + siteUrl + "' to " + id);
        return id;
    }

    private List<String> librariesOf(String siteId) {
        GraphHttpClient.GraphResponse response;
        try {
            response = graph.getJson("/sites/" + siteId + "/drives",
                    ResourceUnitMeter.MULTI_ITEM_QUERY, List.of());
        } catch (GraphException e) {
            throw new GraphException("Could not list the document libraries of site '" + siteId + "': "
                    + e.getMessage(), e);
        }

        Set<String> wanted = lowercased(settings.driveNames());
        List<String> ids = new ArrayList<>();
        List<String> skipped = new ArrayList<>();

        for (JsonNode drive : GraphHttpClient.array(response.body(), "value")) {
            String id = GraphHttpClient.text(drive, "id");
            String name = GraphHttpClient.text(drive, "name");
            String type = GraphHttpClient.text(drive, "driveType");

            if (id == null || id.isBlank()) {
                continue;
            }
            if (type != null && !DOCUMENT_LIBRARY.equalsIgnoreCase(type)) {
                skipped.add(name + " (" + type + ")");
                continue;
            }
            if (!wanted.isEmpty() && (name == null || !wanted.contains(name.toLowerCase(Locale.ROOT)))) {
                skipped.add(name + " (not named in " + SharePointConnectorPlugin.DRIVE_NAMES_SETTING + ")");
                continue;
            }
            ids.add(id);
        }

        if (ids.isEmpty()) {
            throw new GraphException("Site '" + siteId + "' has no document library to ingest"
                    + (skipped.isEmpty() ? "" : "; skipped " + skipped)
                    + ". Check " + SharePointConnectorPlugin.DRIVE_NAMES_SETTING + " if it is set.");
        }

        // Sorted, because the order decides nothing about correctness but everything about stability: the first
        // drive id is the default source alias, so an order that varied between restarts would rename the
        // source and orphan its cursor and its indexed documents.
        List<String> stable = ids.stream().sorted().toList();
        log.info("Resolved " + stable.size() + " drive(s) from site " + siteId
                + (skipped.isEmpty() ? "" : ", skipping " + skipped.size()));
        return stable;
    }

    private static Set<String> lowercased(List<String> values) {
        return values.stream()
                .filter(value -> value != null && !value.isBlank())
                .map(value -> value.trim().toLowerCase(Locale.ROOT))
                .collect(java.util.stream.Collectors.toUnmodifiableSet());
    }
}
