package org.hyland.contentlake.connector.sharepoint;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Every Graph call this connector makes, against a service that meters and throttles all of them.
 *
 * <p>Hand-rolled on {@code java.net.http.HttpClient} rather than built on the official Kiota-generated SDK.
 * The objection is not the SDK's size but its transitive graph: OkHttp, the Kotlin standard library,
 * reactor-core and a second Jackson, inside a parent-first plugin classloader, against a Spring Boot host
 * that supplies some of the same artifacts at different versions. That is a runtime failure with no
 * build-time signal, and this connector calls about eight endpoints, so the SDK's breadth buys nothing.</p>
 *
 * <h3>The base URL is one of the two seams</h3>
 * <p>Nothing here knows whether it is talking to {@code https://graph.microsoft.com/v1.0} or to a mock on
 * {@code http://localhost:8099/v1.0}: the base URL is configuration and the token comes from a
 * {@link GraphTokenProvider}. Those two seams are what make a run against the mock evidence about the
 * cloud. Everything else in this class is real protocol handling, and there is deliberately no branch
 * anywhere on which of the two it is speaking to.</p>
 *
 * <h3>Throttling is handled by stopping, not by retrying harder</h3>
 * <p>A throttled request still counts against the tenant's budget, so retrying aggressively lengthens the
 * throttle instead of surviving it. Two consequences are built in. A {@code Retry-After} is obeyed
 * unconditionally, and it pauses <em>every</em> thread using this client rather than only the one that was
 * refused: the cap is per application per tenant, so a sibling thread continuing at full rate is spending
 * the same budget that just ran out. SharePoint Online does not implement the IETF {@code RateLimit}
 * headers, so {@code Retry-After} is the only signal there is.</p>
 *
 * <p>Traffic is decorated with a {@code NONISV|Hyland|...} user agent because Microsoft prioritises
 * decorated traffic and names undecorated traffic as a deprioritisation candidate.</p>
 */
public final class GraphHttpClient {

    private static final Logger log = Logger.getLogger(GraphHttpClient.class.getName());

    /** Microsoft's documented decoration format is {@code NONISV|CompanyName|AppName/Version}. */
    private static final String USER_AGENT = "NONISV|Hyland|content-lake-sharepoint/1.0.0";

    private static final Duration CONNECT_TIMEOUT = Duration.ofSeconds(10);
    private static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(60);
    private static final Duration DOWNLOAD_TIMEOUT = Duration.ofMinutes(5);

    /** Applies to 429 and 503 only; a request that has waited this many times is a real failure. */
    private static final int MAX_ATTEMPTS = 5;

    /** Used only when a 429 or 503 arrives with no {@code Retry-After}, which is not supposed to happen. */
    private static final long FALLBACK_RETRY_SECONDS = 10;

    private final String baseUrl;
    private final GraphTokenProvider tokenProvider;
    private final ResourceUnitMeter meter;
    private final HttpClient httpClient;

    /**
     * Redirect handling has to be manual for content downloads; see {@link #getStream}. Kept separate from
     * {@link #httpClient} so JSON calls still follow a redirect normally.
     */
    private final HttpClient noRedirectClient;

    /**
     * {@code System.nanoTime()} before which no request may be sent. The tenant-wide pause.
     *
     * <p>Seeded with a real {@code nanoTime()} reading rather than a sentinel like {@code Long.MIN_VALUE}.
     * {@code nanoTime()} has an arbitrary origin and is routinely a large positive number, so
     * {@code Long.MIN_VALUE - nanoTime()} overflows to a large <em>positive</em> remaining time and the
     * first request of every crawl sleeps for centuries. Keeping both sides of the subtraction real
     * readings is what makes the comparison overflow-safe.</p>
     */
    private final AtomicLong resumeAtNanos = new AtomicLong(System.nanoTime());

    private final ObjectMapper json = new ObjectMapper();

    public GraphHttpClient(String baseUrl, GraphTokenProvider tokenProvider, ResourceUnitMeter meter) {
        this(baseUrl, tokenProvider, meter, HttpClient.newBuilder()
                .connectTimeout(CONNECT_TIMEOUT)
                .followRedirects(HttpClient.Redirect.NORMAL)
                .build());
    }

    /** Test seam for the client itself; production uses the three-argument constructor. */
    GraphHttpClient(String baseUrl, GraphTokenProvider tokenProvider, ResourceUnitMeter meter,
                    HttpClient httpClient) {
        if (baseUrl == null || baseUrl.isBlank()) {
            throw new GraphException("sharepoint.graph-base-url is empty");
        }
        this.baseUrl = baseUrl.endsWith("/") ? baseUrl.substring(0, baseUrl.length() - 1) : baseUrl;
        this.tokenProvider = tokenProvider;
        this.meter = meter;
        this.httpClient = httpClient;
        this.noRedirectClient = HttpClient.newBuilder()
                .connectTimeout(CONNECT_TIMEOUT)
                .followRedirects(HttpClient.Redirect.NEVER)
                .build();
    }

    /**
     * One Graph JSON response, plus whether the {@code Prefer} header was honoured.
     *
     * @param body                 the parsed response
     * @param requestedPreferences what was asked for, empty when nothing was
     * @param appliedPreferences   what {@code Preference-Applied} reported, empty when the header was absent
     */
    public record GraphResponse(JsonNode body,
                                Set<String> requestedPreferences,
                                Set<String> appliedPreferences) {

        /** Whether every requested preference came back in {@code Preference-Applied}. */
        public boolean allPreferencesApplied() {
            return appliedPreferences.containsAll(requestedPreferences);
        }

        /** What was asked for and not honoured, which is what a caller has to decide about. */
        public Set<String> unappliedPreferences() {
            Set<String> missing = new LinkedHashSet<>(requestedPreferences);
            missing.removeAll(appliedPreferences);
            return missing;
        }
    }

    /**
     * GETs a Graph resource and parses it.
     *
     * @param pathOrUrl either a path relative to the base URL, or an absolute URL. Absolute is the normal
     *                  case for paging: an {@code @odata.nextLink} or {@code @odata.deltaLink} is a
     *                  complete URL and must be used verbatim, never reconstructed
     * @param units     what this call costs, from the constants on {@link ResourceUnitMeter}. Passed in
     *                  rather than inferred, because the price depends on what is being asked for and
     *                  guessing it wrong is how a budget is silently overspent
     * @param preferences {@code Prefer} tokens to request, or empty
     */
    public GraphResponse getJson(String pathOrUrl, int units, List<String> preferences) {
        Set<String> requested = preferences == null || preferences.isEmpty()
                ? Set.of()
                : new LinkedHashSet<>(preferences);

        HttpResponse<InputStream> response = send(pathOrUrl, units, requested, httpClient,
                REQUEST_TIMEOUT);
        try (InputStream body = response.body()) {
            JsonNode parsed = json.readTree(body);
            Set<String> applied = parseAppliedPreferences(response);
            GraphResponse result = new GraphResponse(parsed == null ? json.nullNode() : parsed,
                    requested, applied);
            warnAboutUnappliedPreferences(pathOrUrl, result);
            return result;
        } catch (IOException e) {
            throw new GraphException("Could not read the Graph response from " + pathOrUrl, e);
        }
    }

    /**
     * GETs a content stream, following Graph's pre-authenticated redirect by hand.
     *
     * <p>{@code /content} answers 302 to a storage URL that carries its own signature. That URL must be
     * fetched <em>without</em> the {@code Authorization} header: the JDK client does not strip it across a
     * redirect, and a storage endpoint given both a bearer token and a signed query rejects the request.
     * So downloads use a non-redirecting client and this method follows the {@code Location} itself. A 200
     * on the first response is handled too, because a small file may be returned inline.</p>
     *
     * <p>The caller closes the stream. It is not buffered in memory here: content is the one Graph response
     * whose size is unbounded.</p>
     */
    public InputStream getStream(String pathOrUrl, int units) {
        HttpResponse<InputStream> response = send(pathOrUrl, units, Set.of(), noRedirectClient,
                DOWNLOAD_TIMEOUT);
        int status = response.statusCode();
        if (status == 200) {
            return response.body();
        }
        if (status == 301 || status == 302 || status == 303 || status == 307 || status == 308) {
            String location = response.headers().firstValue("Location")
                    .orElseThrow(() -> new GraphException(
                            "Graph answered " + status + " for " + pathOrUrl + " with no Location header",
                            status, null));
            closeQuietly(response.body());
            return getUnauthenticated(location);
        }
        closeQuietly(response.body());
        throw new GraphException("Unexpected status " + status + " downloading " + pathOrUrl, status, null);
    }

    /** Fetches a pre-authenticated URL with no credentials of ours attached. */
    private InputStream getUnauthenticated(String url) {
        HttpRequest request = HttpRequest.newBuilder(URI.create(url))
                .timeout(DOWNLOAD_TIMEOUT)
                .header("User-Agent", USER_AGENT)
                .GET()
                .build();
        try {
            HttpResponse<InputStream> response = httpClient.send(request,
                    HttpResponse.BodyHandlers.ofInputStream());
            if (response.statusCode() != 200) {
                closeQuietly(response.body());
                throw new GraphException("Pre-authenticated download returned " + response.statusCode(),
                        response.statusCode(), null);
            }
            return response.body();
        } catch (IOException e) {
            throw new GraphException("Pre-authenticated download failed", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new GraphException("Interrupted during a pre-authenticated download", e);
        }
    }

    /**
     * Sends one request, waiting out the tenant-wide pause first and obeying any {@code Retry-After}.
     *
     * <p>The order matters: the resource-unit budget is spent before the request goes out, so pacing
     * happens whether or not the tenant is currently throttling, and the pause is re-checked after each
     * retry because another thread may have been refused in the meantime.</p>
     */
    private HttpResponse<InputStream> send(String pathOrUrl,
                                           int units,
                                           Set<String> preferences,
                                           HttpClient client,
                                           Duration timeout) {
        URI uri = resolve(pathOrUrl);

        for (int attempt = 1; ; attempt++) {
            awaitResume();
            meter.spend(units);

            HttpRequest.Builder builder = HttpRequest.newBuilder(uri)
                    .timeout(timeout)
                    .header("Authorization", "Bearer " + tokenProvider.token())
                    .header("User-Agent", USER_AGENT)
                    .header("Accept", "application/json")
                    .GET();
            if (!preferences.isEmpty()) {
                builder.header("Prefer", String.join(", ", preferences));
            }

            HttpResponse<InputStream> response;
            try {
                response = client.send(builder.build(), HttpResponse.BodyHandlers.ofInputStream());
            } catch (IOException e) {
                if (attempt >= MAX_ATTEMPTS) {
                    throw new GraphException("Graph request to " + uri + " failed after " + attempt
                            + " attempt(s)", e);
                }
                log.log(Level.WARNING, "Graph request to " + uri + " failed on attempt " + attempt
                        + "; retrying", e);
                sleepMillis(backoffMillis(attempt));
                continue;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new GraphException("Interrupted calling " + uri, e);
            }

            int status = response.statusCode();
            if (status == 429 || status == 503) {
                closeQuietly(response.body());
                long waitSeconds = retryAfterSeconds(response).orElse(FALLBACK_RETRY_SECONDS);
                if (attempt >= MAX_ATTEMPTS) {
                    throw new GraphException("Graph is still throttling " + uri + " after " + attempt
                            + " attempt(s); the last Retry-After was " + waitSeconds + "s", status, null);
                }
                // Pauses every thread on this client, not just this one.
                pauseAll(waitSeconds);
                log.warning("Graph returned " + status + " for " + uri + "; every request is paused for "
                        + waitSeconds + "s (attempt " + attempt + " of " + MAX_ATTEMPTS + ")");
                continue;
            }
            if (status >= 500) {
                closeQuietly(response.body());
                if (attempt >= MAX_ATTEMPTS) {
                    throw new GraphException("Graph returned " + status + " for " + uri + " after "
                            + attempt + " attempt(s)", status, null);
                }
                sleepMillis(backoffMillis(attempt));
                continue;
            }
            if (status == 401 || status == 403) {
                // Not retried. With app-only auth this is a consent or Sites.Selected problem, and a
                // retry would turn a clear failure into a slow one.
                closeQuietly(response.body());
                throw new GraphException("Graph refused " + uri + " with " + status
                        + ". Check the application permissions granted to this app registration ("
                        + tokenProvider.describe() + ")", status, null);
            }
            if (status == 410) {
                // A stale delta token. Returned to the caller rather than thrown, because the SPI models
                // it as SourceChangePage.expired() and it is an ordinary event, not a failure.
                return response;
            }
            if (status >= 400) {
                String detail = readErrorBody(response);
                throw new GraphException("Graph returned " + status + " for " + uri
                        + (detail.isEmpty() ? "" : ": " + detail), status, null);
            }
            return response;
        }
    }

    /**
     * Resolves a path against the base URL, or passes an absolute URL through untouched.
     *
     * <p>Untouched matters: a {@code nextLink} carries an opaque skip token that must not be re-encoded.
     * A base-URL rewrite is applied to absolute links, though, because a mock answering on localhost
     * returns links to itself while a tenant returns links to {@code graph.microsoft.com}, and following
     * whichever it sent is correct in both cases.</p>
     */
    private URI resolve(String pathOrUrl) {
        if (pathOrUrl == null || pathOrUrl.isBlank()) {
            throw new GraphException("Empty Graph path");
        }
        String trimmed = pathOrUrl.trim();
        if (trimmed.startsWith("http://") || trimmed.startsWith("https://")) {
            return URI.create(trimmed);
        }
        return URI.create(baseUrl + (trimmed.startsWith("/") ? trimmed : "/" + trimmed));
    }

    /** Blocks until the tenant-wide pause has elapsed. */
    private void awaitResume() {
        while (true) {
            long resumeAt = resumeAtNanos.get();
            long remaining = resumeAt - System.nanoTime();
            if (remaining <= 0) {
                return;
            }
            sleepMillis(Math.max(1, remaining / 1_000_000L));
        }
    }

    /**
     * How much of the tenant-wide pause is left, in milliseconds, or zero when nothing is paused.
     *
     * <p>Package-private for tests: proving that a thread which was never refused still waits means
     * observing the pause being set, and the alternative -- waiting for the server to have sent a 429 --
     * races the client, which has not read the response yet at that point.</p>
     */
    long pauseRemainingMillis() {
        long remaining = resumeAtNanos.get() - System.nanoTime();
        return remaining <= 0 ? 0 : remaining / 1_000_000L;
    }

    /** Extends the pause; never shortens one another thread already set. */
    private void pauseAll(long seconds) {
        long until = System.nanoTime() + seconds * 1_000_000_000L;
        resumeAtNanos.updateAndGet(existing -> Math.max(existing, until));
    }

    /**
     * {@code Retry-After} as seconds, accepting both forms the HTTP spec allows.
     *
     * <p>SharePoint Online sends delta seconds, but a proxy in front of it may rewrite it as an HTTP date,
     * and reading a date as an integer would throw where the whole point of the header is to be obeyed.</p>
     */
    private static Optional<Long> retryAfterSeconds(HttpResponse<?> response) {
        Optional<String> header = response.headers().firstValue("Retry-After");
        if (header.isEmpty()) {
            return Optional.empty();
        }
        String value = header.get().trim();
        try {
            return Optional.of(Math.max(0, Long.parseLong(value)));
        } catch (NumberFormatException ignored) {
            // Not a number, so try the HTTP-date form.
        }
        try {
            Instant when = java.time.ZonedDateTime.parse(value,
                    java.time.format.DateTimeFormatter.RFC_1123_DATE_TIME).toInstant();
            long seconds = Duration.between(Instant.now(), when).getSeconds();
            return Optional.of(Math.max(0, seconds));
        } catch (DateTimeParseException e) {
            log.warning("Could not read Retry-After value '" + value + "'; using "
                    + FALLBACK_RETRY_SECONDS + "s");
            return Optional.empty();
        }
    }

    private static Set<String> parseAppliedPreferences(HttpResponse<?> response) {
        Optional<String> header = response.headers().firstValue("Preference-Applied");
        if (header.isEmpty()) {
            return Set.of();
        }
        Set<String> applied = new LinkedHashSet<>();
        for (String token : header.get().split(",")) {
            String cleaned = token.trim().toLowerCase(Locale.ROOT);
            if (!cleaned.isEmpty()) {
                applied.add(cleaned);
            }
        }
        return applied;
    }

    /**
     * Says so when a preference was not honoured, rather than letting the caller discover it as absent
     * data.
     *
     * <p>This is the one that matters for cost: {@code hierarchicalsharing} not being applied means every
     * document needs its own 5-unit permission read, which is a fivefold increase in the crawl's spend.
     * Whether to continue is the caller's decision, not this class's, so this only reports.</p>
     */
    private void warnAboutUnappliedPreferences(String pathOrUrl, GraphResponse response) {
        if (response.requestedPreferences().isEmpty() || response.allPreferencesApplied()) {
            return;
        }
        log.warning("Graph did not apply " + response.unappliedPreferences() + " for " + pathOrUrl
                + " (Preference-Applied reported " + response.appliedPreferences() + "). This changes what "
                + "the response contains and what the crawl costs; it is not a transport error.");
    }

    private static String readErrorBody(HttpResponse<InputStream> response) {
        try (InputStream body = response.body()) {
            if (body == null) {
                return "";
            }
            byte[] bytes = body.readNBytes(2048);
            return new String(bytes, java.nio.charset.StandardCharsets.UTF_8).replaceAll("\\s+", " ").trim();
        } catch (IOException e) {
            return "";
        }
    }

    private static long backoffMillis(int attempt) {
        return Math.min(30_000L, 500L * (1L << Math.min(attempt - 1, 5)));
    }

    private static void sleepMillis(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new GraphException("Interrupted while waiting to call Graph", e);
        }
    }

    private static void closeQuietly(InputStream stream) {
        if (stream == null) {
            return;
        }
        try {
            stream.close();
        } catch (IOException ignored) {
            // Draining a body we are discarding; nothing useful to do with a failure here.
        }
    }

    /** For the status endpoint and the startup log; never the token. */
    public String describe() {
        return "Graph at " + baseUrl + " using " + tokenProvider.describe();
    }

    /** The meter, so a run can report what it spent. */
    public ResourceUnitMeter meter() {
        return meter;
    }

    /** Convenience for the paging loops: the string at {@code field}, or null. */
    public static String text(JsonNode node, String field) {
        if (node == null) {
            return null;
        }
        JsonNode value = node.get(field);
        return value == null || value.isNull() ? null : value.asText();
    }

    /** Convenience for the paging loops: the array at {@code field}, never null. */
    public static List<JsonNode> array(JsonNode node, String field) {
        List<JsonNode> items = new ArrayList<>();
        if (node == null) {
            return items;
        }
        JsonNode value = node.get(field);
        if (value != null && value.isArray()) {
            value.forEach(items::add);
        }
        return items;
    }
}
