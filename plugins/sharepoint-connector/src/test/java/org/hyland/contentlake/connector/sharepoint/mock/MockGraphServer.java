package org.hyland.contentlake.connector.sharepoint.mock;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * A local stand-in for the Microsoft Graph endpoints the SharePoint connector calls.
 *
 * <p>Exists so the connector can be built and demonstrated without a Microsoft 365 tenant, which is
 * otherwise blocked: registering an application is disabled in this tenant, and a SharePoint site a
 * developer is merely a member of returns a truncated ACL, so it cannot validate permission mapping at
 * all. It runs both in process from a unit test and as a container beside the ingester, which is why it is
 * one file with no dependencies: the same source the tests compile is what
 * {@code java MockGraphServer.java} runs.</p>
 *
 * <h3>What it is faithful about, deliberately</h3>
 * <p>A mock that is easier to talk to than the real service is worse than no mock, because the connector
 * then encodes the mock's shape. Four behaviours are reproduced precisely because each one is a way the
 * connector could be wrong against the cloud:</p>
 * <ul>
 *   <li><strong>Collections page with an opaque {@code @odata.nextLink}, and {@code $skip} is ignored.</strong>
 *       Graph does not support {@code $skip} on drive collections. A mock that honoured it would let a
 *       connector ship a skip-based pager that silently re-reads page one forever against a tenant.</li>
 *   <li><strong>{@code /content} answers 302 to a pre-authenticated URL, and that URL rejects a request
 *       carrying {@code Authorization}.</strong> A signed storage URL given a bearer token as well is
 *       refused, and the JDK HTTP client does not strip the header across a redirect, so this is a real
 *       trap that only a faithful mock catches.</li>
 *   <li><strong>A delta token that this server does not recognise answers {@code 410 Gone} with
 *       {@code resyncRequired}</strong>, which is the case the SPI models as
 *       {@code SourceChangePage.expired()}.</li>
 *   <li><strong>{@code Preference-Applied} lists only the preferences it was configured to honour.</strong>
 *       Whether a tenant honours {@code hierarchicalsharing} decides whether a crawl costs one unit per
 *       document or six, so both answers have to be testable.</li>
 * </ul>
 *
 * <p>What it is not: an authentication server. msal4j rejects an authority that is not {@code https}, so
 * this serves Graph only and a local run uses {@code sharepoint.auth-mode=static-token}. It checks that a
 * bearer token is present and rejects a request without one, so the connector's decoration is exercised,
 * but it does not validate the token's contents.</p>
 *
 * <h3>Fixtures are verbatim Graph payloads</h3>
 * <p>Nothing here parses JSON, which is what keeps it dependency-free and, more usefully, means a response
 * recorded from Graph Explorer can be dropped in unchanged. The layout under the fixture root:</p>
 * <pre>
 *   site.json                    GET /sites/{siteId}
 *   drives.json                  GET /sites/{siteId}/drives
 *   drive.json                   GET /drives/{driveId}
 *   items/&lt;itemId&gt;.json          one driveItem, exactly as Graph returns it
 *   permissions/&lt;itemId&gt;.json     that item's /permissions response, verbatim
 *   content/&lt;itemId&gt;             raw bytes served for that item's /content
 *   order.txt                    item ids in the order delta reports them, one per line
 *   children.txt                 "&lt;parentId&gt;: &lt;childId&gt; &lt;childId&gt;", one parent per line
 *   changes/&lt;n&gt;/order.txt        items reported by the n-th incremental delta call
 * </pre>
 *
 * <p>{@code children.txt} states the tree rather than deriving it from each item's
 * {@code parentReference}, because deriving it would mean parsing. The cost is that a fixture set can
 * disagree with itself; the benefit is that pasted payloads stay pasted.</p>
 */
public final class MockGraphServer implements AutoCloseable {

    /** Everything a test or a container may want to vary, with defaults that behave like a healthy tenant. */
    public record Options(int port,
                          Path fixtures,
                          int pageSize,
                          Set<String> honouredPreferences,
                          int throttleEveryNthRequest,
                          int retryAfterSeconds,
                          boolean requireBearerToken) {

        public static Options defaults(Path fixtures) {
            return new Options(0, fixtures, 2,
                    // The two a healthy tenant honours for a connector holding Sites.FullControl.All.
                    Set.of("hierarchicalsharing", "deltashowremovedasdeleted"),
                    0, 1, true);
        }

        public Options withPort(int newPort) {
            return new Options(newPort, fixtures, pageSize, honouredPreferences, throttleEveryNthRequest,
                    retryAfterSeconds, requireBearerToken);
        }

        public Options withPageSize(int newPageSize) {
            return new Options(port, fixtures, newPageSize, honouredPreferences, throttleEveryNthRequest,
                    retryAfterSeconds, requireBearerToken);
        }

        public Options withHonouredPreferences(Set<String> preferences) {
            return new Options(port, fixtures, pageSize, preferences, throttleEveryNthRequest,
                    retryAfterSeconds, requireBearerToken);
        }

        /** Refuse every n-th request with 429, to exercise the connector's pause-and-resume. */
        public Options withThrottleEveryNthRequest(int n, int seconds) {
            return new Options(port, fixtures, pageSize, honouredPreferences, n, seconds,
                    requireBearerToken);
        }
    }

    private static final String API_PREFIX = "/v1.0";
    private static final String STORAGE_PREFIX = "/mock-storage";

    private final Options options;
    private final HttpServer server;
    private final String baseUrl;

    /** Cursor to the index in {@link #deltaOrder} it resumes from, so a nextLink is opaque to the client. */
    private final Map<String, Integer> pageCursors = new ConcurrentHashMap<>();

    /** Delta tokens this server issued. Anything else is treated as aged out, and answers 410. */
    private final Set<String> issuedDeltaTokens = Collections.synchronizedSet(new LinkedHashSet<>());

    /** Which incremental generation the next call on a delta token should serve. */
    private final Map<String, Integer> deltaGenerations = new ConcurrentHashMap<>();

    private final AtomicLong tokenSequence = new AtomicLong();
    private final AtomicInteger requestCount = new AtomicInteger();
    private final List<String> requestLog = Collections.synchronizedList(new ArrayList<>());

    private final List<String> deltaOrder;
    private final Map<String, List<String>> childrenByParent;

    public MockGraphServer(Options options) throws IOException {
        this.options = options;
        this.deltaOrder = readOrder(options.fixtures().resolve("order.txt"));
        this.childrenByParent = readChildren(options.fixtures().resolve("children.txt"));
        this.server = HttpServer.create(new InetSocketAddress(options.port()), 0);
        // A fixed pool rather than the default serial executor: the connector pauses every thread on a
        // 429, and proving that needs more than one request in flight.
        this.server.setExecutor(Executors.newFixedThreadPool(4));
        this.server.createContext("/", this::dispatch);
        this.server.start();
        this.baseUrl = "http://localhost:" + this.server.getAddress().getPort();
    }

    /** Where the connector should point {@code sharepoint.graph-base-url}. */
    public String graphBaseUrl() {
        return baseUrl + API_PREFIX;
    }

    /** Every request path this server has answered, for assertions about what the connector actually did. */
    public List<String> requestLog() {
        return List.copyOf(requestLog);
    }

    public int requestCount() {
        return requestCount.get();
    }

    @Override
    public void close() {
        server.stop(0);
    }

    private void dispatch(HttpExchange exchange) throws IOException {
        try {
            String path = exchange.getRequestURI().getPath();
            Map<String, String> query = parseQuery(exchange.getRequestURI());
            requestCount.incrementAndGet();
            requestLog.add(exchange.getRequestMethod() + " " + path
                    + (exchange.getRequestURI().getRawQuery() == null
                            ? "" : "?" + exchange.getRequestURI().getRawQuery()));

            if (path.startsWith(STORAGE_PREFIX)) {
                serveStorage(exchange, path);
                return;
            }
            if (options.requireBearerToken()
                    && exchange.getRequestHeaders().getFirst("Authorization") == null) {
                error(exchange, 401, "InvalidAuthenticationToken",
                        "Access token is empty or missing");
                return;
            }
            if (shouldThrottle()) {
                exchange.getResponseHeaders().add("Retry-After",
                        String.valueOf(options.retryAfterSeconds()));
                error(exchange, 429, "activityLimitReached", "Request throttled by the mock");
                return;
            }
            if (!path.startsWith(API_PREFIX)) {
                error(exchange, 404, "itemNotFound", "No such path: " + path);
                return;
            }

            route(exchange, path.substring(API_PREFIX.length()), query);
        } catch (FixtureMissingException e) {
            error(exchange, 404, "itemNotFound", e.getMessage());
        } catch (RuntimeException e) {
            error(exchange, 500, "generalException", String.valueOf(e.getMessage()));
        } finally {
            exchange.close();
        }
    }

    private void route(HttpExchange exchange, String path, Map<String, String> query) throws IOException {
        List<String> segments = segments(path);

        // /sites/{siteId} and /sites/{siteId}/drives
        if (segments.size() == 2 && segments.get(0).equals("sites")) {
            serveFixture(exchange, "site.json", Set.of());
            return;
        }
        if (segments.size() == 3 && segments.get(0).equals("sites") && segments.get(2).equals("drives")) {
            serveFixture(exchange, "drives.json", Set.of());
            return;
        }

        if (!segments.isEmpty() && segments.get(0).equals("drives")) {
            // /drives/{driveId}
            if (segments.size() == 2) {
                serveFixture(exchange, "drive.json", Set.of());
                return;
            }
            // /drives/{driveId}/root/delta
            if (segments.size() == 4 && segments.get(2).equals("root") && segments.get(3).equals("delta")) {
                serveDelta(exchange, segments.get(1), query);
                return;
            }
            if (segments.size() >= 4 && segments.get(2).equals("items")) {
                String itemId = segments.get(3);
                if (segments.size() == 4) {
                    serveFixture(exchange, "items/" + itemId + ".json", Set.of());
                    return;
                }
                switch (segments.get(4)) {
                    case "children" -> {
                        serveChildren(exchange, segments.get(1), itemId, query);
                        return;
                    }
                    case "content" -> {
                        serveContentRedirect(exchange, segments.get(1), itemId);
                        return;
                    }
                    case "permissions" -> {
                        serveFixture(exchange, "permissions/" + itemId + ".json", Set.of());
                        return;
                    }
                    default -> { /* falls through to 404 */ }
                }
            }
        }

        error(exchange, 404, "itemNotFound", "The mock does not implement " + path);
    }

    /**
     * {@code /root/delta}, in the three shapes the connector depends on.
     *
     * <p>{@code ?token=latest} answers an empty page with a delta link, which is how a host seeds a cursor
     * before its first walk: a feed opened at "now" must not report the documents that already exist, or
     * the walk that indexes them would be skipped. An unrecognised token answers 410. Anything else pages
     * through {@code order.txt} and finishes with a delta link.</p>
     */
    private void serveDelta(HttpExchange exchange, String driveId, Map<String, String> query)
            throws IOException {
        String token = query.get("token");
        Set<String> applied = applyPreferences(exchange);

        if ("latest".equals(token)) {
            String deltaToken = issueDeltaToken();
            respondJson(exchange, 200, "{\"value\":[],\"@odata.deltaLink\":\""
                    + deltaLink(driveId, deltaToken) + "\"}", applied);
            return;
        }

        int from = 0;
        if (token != null) {
            if (pageCursors.containsKey(token)) {
                from = pageCursors.get(token);
            } else if (issuedDeltaTokens.contains(token)) {
                serveIncrementalGeneration(exchange, driveId, token, applied);
                return;
            } else {
                // Aged out, or from a previous run of this server. Either way the host has to walk.
                error(exchange, 410, "resyncRequired",
                        "The delta token is no longer valid; a full enumeration is required");
                return;
            }
        }

        int to = Math.min(deltaOrder.size(), from + options.pageSize());
        List<String> page = deltaOrder.subList(from, to);
        String body;
        if (to < deltaOrder.size()) {
            String next = issuePageToken(to);
            body = "{\"value\":[" + itemsJson(page) + "],\"@odata.nextLink\":\""
                    + deltaLink(driveId, next) + "\"}";
        } else {
            String deltaToken = issueDeltaToken();
            body = "{\"value\":[" + itemsJson(page) + "],\"@odata.deltaLink\":\""
                    + deltaLink(driveId, deltaToken) + "\"}";
        }
        respondJson(exchange, 200, body, applied);
    }

    /**
     * Serves {@code changes/<n>/order.txt} for the n-th call on a given delta token.
     *
     * <p>This is how a deletion is testable: a generation lists an item whose fixture carries a
     * {@code deleted} facet, and the connector has to turn it into a tombstone.</p>
     */
    private void serveIncrementalGeneration(HttpExchange exchange, String driveId, String token,
                                            Set<String> applied) throws IOException {
        int generation = deltaGenerations.merge(token, 1, Integer::sum);
        Path order = options.fixtures().resolve("changes").resolve(String.valueOf(generation))
                .resolve("order.txt");
        List<String> ids = Files.isReadable(order) ? readOrder(order) : List.of();
        String nextToken = issueDeltaToken();
        deltaGenerations.put(nextToken, generation);
        respondJson(exchange, 200, "{\"value\":[" + itemsJson(ids) + "],\"@odata.deltaLink\":\""
                + deltaLink(driveId, nextToken) + "\"}", applied);
    }

    /**
     * A container's children, paged the way Graph pages them.
     *
     * <p>{@code $top} bounds the page and {@code $skiptoken} resumes it. A {@code $skip} parameter is
     * accepted and <em>ignored</em>, exactly as Graph does for drive collections: a connector that paged
     * with {@code skip} would read page one repeatedly, and it is better for that to be reproducible here
     * than discovered against a tenant.</p>
     */
    private void serveChildren(HttpExchange exchange, String driveId, String itemId,
                               Map<String, String> query) throws IOException {
        List<String> children = childrenByParent.getOrDefault(itemId, List.of());
        int from = 0;
        String skipToken = query.get("$skiptoken");
        if (skipToken != null) {
            Integer cursor = pageCursors.get(skipToken);
            if (cursor == null) {
                error(exchange, 400, "invalidRequest", "Unknown $skiptoken");
                return;
            }
            from = cursor;
        }
        int top = parsePositiveInt(query.get("$top"), options.pageSize());
        int to = Math.min(children.size(), from + top);
        List<String> page = children.subList(Math.min(from, children.size()), to);

        String body;
        if (to < children.size()) {
            String next = issuePageToken(to);
            String nextLink = baseUrl + API_PREFIX + "/drives/" + driveId + "/items/" + itemId
                    + "/children?$top=" + top + "&$skiptoken=" + next;
            body = "{\"value\":[" + itemsJson(page) + "],\"@odata.nextLink\":\"" + nextLink + "\"}";
        } else {
            body = "{\"value\":[" + itemsJson(page) + "]}";
        }
        respondJson(exchange, 200, body, applyPreferences(exchange));
    }

    /** 302 to a URL that carries its own signature, which is what Graph does. */
    private void serveContentRedirect(HttpExchange exchange, String driveId, String itemId)
            throws IOException {
        if (!Files.isReadable(contentPath(itemId))) {
            error(exchange, 404, "itemNotFound", "No content fixture for item " + itemId);
            return;
        }
        String location = baseUrl + STORAGE_PREFIX + "/" + driveId + "/" + itemId
                + "?sig=mock-signature-" + tokenSequence.incrementAndGet();
        exchange.getResponseHeaders().add("Location", location);
        exchange.sendResponseHeaders(302, -1);
    }

    /**
     * The pre-authenticated download, which refuses a request that also carries our bearer token.
     *
     * <p>Not pedantry: a signed storage URL given an {@code Authorization} header as well is rejected, and
     * the JDK client does not strip that header when it follows a redirect. A mock that accepted both would
     * let the connector ship a download path that fails on the first real file.</p>
     */
    private void serveStorage(HttpExchange exchange, String path) throws IOException {
        if (exchange.getRequestHeaders().getFirst("Authorization") != null) {
            error(exchange, 400, "invalidRequest",
                    "A pre-authenticated URL must not be called with an Authorization header");
            return;
        }
        List<String> segments = segments(path.substring(STORAGE_PREFIX.length()));
        if (segments.size() != 2) {
            error(exchange, 404, "itemNotFound", "Malformed storage path " + path);
            return;
        }
        Path content = contentPath(segments.get(1));
        if (!Files.isReadable(content)) {
            error(exchange, 404, "itemNotFound", "No content fixture for item " + segments.get(1));
            return;
        }
        byte[] bytes = Files.readAllBytes(content);
        exchange.getResponseHeaders().add("Content-Type", "application/octet-stream");
        exchange.sendResponseHeaders(200, bytes.length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(bytes);
        }
    }

    /** Echoes only the preferences this server was told to honour, and stays silent about the rest. */
    private Set<String> applyPreferences(HttpExchange exchange) {
        String requested = exchange.getRequestHeaders().getFirst("Prefer");
        if (requested == null || requested.isBlank()) {
            return Set.of();
        }
        Set<String> applied = new LinkedHashSet<>();
        for (String token : requested.split(",")) {
            String cleaned = token.trim().toLowerCase(Locale.ROOT);
            if (options.honouredPreferences().contains(cleaned)) {
                applied.add(cleaned);
            }
        }
        return applied;
    }

    private boolean shouldThrottle() {
        int every = options.throttleEveryNthRequest();
        return every > 0 && requestCount.get() % every == 0;
    }

    private String issuePageToken(int resumeAt) {
        String token = "page-" + tokenSequence.incrementAndGet();
        pageCursors.put(token, resumeAt);
        return token;
    }

    private String issueDeltaToken() {
        String token = "delta-" + tokenSequence.incrementAndGet();
        issuedDeltaTokens.add(token);
        return token;
    }

    private String deltaLink(String driveId, String token) {
        return baseUrl + API_PREFIX + "/drives/" + driveId + "/root/delta?token=" + token;
    }

    /** Concatenates item fixtures verbatim; nothing here understands their contents. */
    private String itemsJson(List<String> itemIds) {
        return itemIds.stream().map(this::readItem).collect(Collectors.joining(","));
    }

    private String readItem(String itemId) {
        return readFixture("items/" + itemId + ".json").trim();
    }

    private Path contentPath(String itemId) {
        return options.fixtures().resolve("content").resolve(itemId);
    }

    private String readFixture(String relative) {
        Path file = options.fixtures().resolve(relative);
        if (!Files.isReadable(file)) {
            throw new FixtureMissingException("No fixture at " + relative);
        }
        try {
            return Files.readString(file, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new FixtureMissingException("Could not read fixture " + relative + ": " + e.getMessage());
        }
    }

    private void serveFixture(HttpExchange exchange, String relative, Set<String> applied)
            throws IOException {
        respondJson(exchange, 200, readFixture(relative), applied);
    }

    private void respondJson(HttpExchange exchange, int status, String body, Set<String> appliedPreferences)
            throws IOException {
        if (!appliedPreferences.isEmpty()) {
            exchange.getResponseHeaders().add("Preference-Applied",
                    String.join(", ", appliedPreferences));
        }
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().add("Content-Type", "application/json");
        exchange.sendResponseHeaders(status, bytes.length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(bytes);
        }
    }

    /** Graph's error envelope, because a connector that reads {@code error.code} has to find one. */
    private void error(HttpExchange exchange, int status, String code, String message) throws IOException {
        String body = "{\"error\":{\"code\":\"" + code + "\",\"message\":\"" + escape(message) + "\"}}";
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().add("Content-Type", "application/json");
        exchange.sendResponseHeaders(status, bytes.length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(bytes);
        }
    }

    private static String escape(String value) {
        return value == null ? "" : value.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    private static List<String> segments(String path) {
        List<String> segments = new ArrayList<>();
        for (String segment : path.split("/")) {
            if (!segment.isBlank()) {
                segments.add(URLDecoder.decode(segment, StandardCharsets.UTF_8));
            }
        }
        return segments;
    }

    private static Map<String, String> parseQuery(URI uri) {
        Map<String, String> result = new LinkedHashMap<>();
        String raw = uri.getRawQuery();
        if (raw == null || raw.isBlank()) {
            return result;
        }
        for (String pair : raw.split("&")) {
            int equals = pair.indexOf('=');
            if (equals < 0) {
                result.put(URLDecoder.decode(pair, StandardCharsets.UTF_8), "");
            } else {
                result.put(URLDecoder.decode(pair.substring(0, equals), StandardCharsets.UTF_8),
                        URLDecoder.decode(pair.substring(equals + 1), StandardCharsets.UTF_8));
            }
        }
        return result;
    }

    private static int parsePositiveInt(String value, int fallback) {
        if (value == null || value.isBlank()) {
            return fallback;
        }
        try {
            int parsed = Integer.parseInt(value.trim());
            return parsed > 0 ? parsed : fallback;
        } catch (NumberFormatException e) {
            return fallback;
        }
    }

    private static List<String> readOrder(Path file) throws IOException {
        if (!Files.isReadable(file)) {
            return List.of();
        }
        List<String> ids = new ArrayList<>();
        for (String line : Files.readAllLines(file, StandardCharsets.UTF_8)) {
            String trimmed = line.trim();
            if (!trimmed.isEmpty() && !trimmed.startsWith("#")) {
                ids.add(trimmed);
            }
        }
        return List.copyOf(ids);
    }

    private static Map<String, List<String>> readChildren(Path file) throws IOException {
        if (!Files.isReadable(file)) {
            return Map.of();
        }
        Map<String, List<String>> result = new HashMap<>();
        for (String line : Files.readAllLines(file, StandardCharsets.UTF_8)) {
            String trimmed = line.trim();
            if (trimmed.isEmpty() || trimmed.startsWith("#")) {
                continue;
            }
            int colon = trimmed.indexOf(':');
            if (colon < 0) {
                throw new IOException("children.txt needs '<parentId>: <childId> ...' but found: " + line);
            }
            String parent = trimmed.substring(0, colon).trim();
            List<String> children = new ArrayList<>();
            for (String child : trimmed.substring(colon + 1).trim().split("\\s+")) {
                if (!child.isBlank()) {
                    children.add(child);
                }
            }
            result.put(parent, List.copyOf(children));
        }
        return Map.copyOf(result);
    }

    /** A fixture the connector asked for does not exist, which is a 404 rather than a server fault. */
    private static final class FixtureMissingException extends RuntimeException {
        FixtureMissingException(String message) {
            super(message);
        }
    }

    /**
     * Runs the mock as a service, for the compose profile and for a local dev loop.
     *
     * <p>Configured from the environment so the container needs no arguments:
     * {@code MOCK_GRAPH_PORT} (default 8099), {@code MOCK_GRAPH_FIXTURES} (default {@code /fixtures}),
     * {@code MOCK_GRAPH_PAGE_SIZE}, {@code MOCK_GRAPH_HONOURED_PREFERENCES} (comma separated, empty string
     * for a tenant that honours none), {@code MOCK_GRAPH_THROTTLE_EVERY} and
     * {@code MOCK_GRAPH_RETRY_AFTER_SECONDS}.</p>
     */
    public static void main(String[] args) throws Exception {
        int port = parsePositiveInt(System.getenv("MOCK_GRAPH_PORT"), 8099);
        Path fixtures = Path.of(env("MOCK_GRAPH_FIXTURES", "/fixtures"));
        int pageSize = parsePositiveInt(System.getenv("MOCK_GRAPH_PAGE_SIZE"), 200);
        String preferences = env("MOCK_GRAPH_HONOURED_PREFERENCES",
                "hierarchicalsharing,deltashowremovedasdeleted");
        int throttleEvery = parsePositiveInt(System.getenv("MOCK_GRAPH_THROTTLE_EVERY"), 0);
        int retryAfter = parsePositiveInt(System.getenv("MOCK_GRAPH_RETRY_AFTER_SECONDS"), 1);

        Options options = new Options(port, fixtures, pageSize,
                preferences.isBlank() ? Set.of() : Set.of(preferences.toLowerCase(Locale.ROOT).split(",")),
                throttleEvery, retryAfter, true);

        MockGraphServer mock = new MockGraphServer(options);
        System.out.println("Mock Graph listening on " + mock.graphBaseUrl()
                + ", fixtures from " + fixtures.toAbsolutePath()
                + ", honouring " + options.honouredPreferences()
                + (throttleEvery > 0 ? ", throttling every " + throttleEvery + " requests" : ""));
        Runtime.getRuntime().addShutdownHook(new Thread(mock::close));
        Thread.currentThread().join();
    }

    private static String env(String name, String fallback) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? fallback : value;
    }
}
