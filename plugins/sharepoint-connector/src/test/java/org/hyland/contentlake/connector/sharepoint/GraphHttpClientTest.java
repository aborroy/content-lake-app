package org.hyland.contentlake.connector.sharepoint;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Drives the client against a real HTTP server rather than a mocked one.
 *
 * <p>{@code com.sun.net.httpserver.HttpServer} on port 0 is the same technique
 * {@code MultiSourceAuthenticationProviderTest} uses in {@code rag-service}: it needs no dependency, and
 * what is under test here is protocol behaviour -- headers on the wire, a status code arriving, a redirect
 * being followed without credentials -- which a mocked {@code HttpClient} would assert nothing about.</p>
 */
class GraphHttpClientTest {

    private HttpServer server;
    private String baseUrl;
    private final List<String> requestLog = new CopyOnWriteArrayList<>();
    private final List<Long> requestTimes = new CopyOnWriteArrayList<>();

    @BeforeEach
    void startServer() throws IOException {
        server = HttpServer.create(new InetSocketAddress(0), 0);
        baseUrl = "http://localhost:" + server.getAddress().getPort() + "/v1.0";
    }

    @AfterEach
    void stopServer() {
        if (server != null) {
            server.stop(0);
        }
    }

    private GraphHttpClient clientWith(ResourceUnitMeter meter) {
        server.start();
        return new GraphHttpClient(baseUrl, new StaticTokenProvider("test-token"), meter);
    }

    private void handle(String path, Handler handler) {
        server.createContext(path, exchange -> {
            requestLog.add(exchange.getRequestMethod() + " " + exchange.getRequestURI());
            requestTimes.add(System.nanoTime());
            try {
                handler.handle(exchange);
            } finally {
                exchange.close();
            }
        });
    }

    private static void respond(HttpExchange exchange, int status, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.sendResponseHeaders(status, bytes.length);
        exchange.getResponseBody().write(bytes);
    }

    @Test
    void decoratesEveryRequestAndParsesTheResponse() {
        List<String> authorization = new CopyOnWriteArrayList<>();
        List<String> userAgent = new CopyOnWriteArrayList<>();
        handle("/v1.0/me/drive", exchange -> {
            authorization.add(String.valueOf(exchange.getRequestHeaders().getFirst("Authorization")));
            userAgent.add(String.valueOf(exchange.getRequestHeaders().getFirst("User-Agent")));
            respond(exchange, 200, "{\"id\":\"drive-1\",\"driveType\":\"business\"}");
        });

        GraphHttpClient client = clientWith(ResourceUnitMeter.unmetered());
        GraphHttpClient.GraphResponse response =
                client.getJson("/me/drive", ResourceUnitMeter.SINGLE_ITEM, List.of());

        assertThat(response.body().get("id").asText()).isEqualTo("drive-1");
        assertThat(authorization).containsExactly("Bearer test-token");
        // Microsoft prioritises decorated traffic and names undecorated traffic as a deprioritisation
        // candidate, so the format is asserted rather than merely present.
        assertThat(userAgent).allSatisfy(value -> assertThat(value).startsWith("NONISV|Hyland|"));
    }

    @Test
    void followsAnAbsoluteNextLinkVerbatimRatherThanRebuildingIt() {
        handle("/v1.0/page2", exchange -> respond(exchange, 200, "{\"value\":[]}"));

        GraphHttpClient client = clientWith(ResourceUnitMeter.unmetered());
        // An opaque skip token is exactly the sort of value that must not be re-encoded.
        client.getJson(baseUrl + "/page2?$skiptoken=abc%2Fdef", ResourceUnitMeter.MULTI_ITEM_QUERY, List.of());

        assertThat(requestLog).containsExactly("GET /v1.0/page2?$skiptoken=abc%2Fdef");
    }

    @Test
    void obeysRetryAfterAndPausesEveryThreadNotOnlyTheOneThatWasRefused() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        CountDownLatch refused = new CountDownLatch(1);
        handle("/v1.0/throttled", exchange -> {
            if (calls.incrementAndGet() == 1) {
                exchange.getResponseHeaders().add("Retry-After", "1");
                respond(exchange, 429, "{\"error\":{\"code\":\"activityLimitReached\"}}");
                refused.countDown();
            } else {
                respond(exchange, 200, "{\"ok\":true}");
            }
        });

        GraphHttpClient client = clientWith(ResourceUnitMeter.unmetered());

        // One thread is refused, and only once that has happened does a second thread start. Firing both
        // at once would prove nothing: a sibling request already in flight when the 429 arrives is
        // legitimately not paused, so the assertion would depend on which won the race.
        CountDownLatch done = new CountDownLatch(2);
        Thread.ofPlatform().start(() -> {
            try {
                client.getJson("/throttled", ResourceUnitMeter.SINGLE_ITEM, List.of());
            } finally {
                done.countDown();
            }
        });
        assertThat(refused.await(10, TimeUnit.SECONDS)).isTrue();
        // The server having sent the 429 is not enough: the refused thread has not read it yet, so the
        // pause may not exist for another few hundred microseconds. Wait for the pause itself.
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (client.pauseRemainingMillis() == 0 && System.nanoTime() < deadline) {
            Thread.onSpinWait();
        }
        assertThat(client.pauseRemainingMillis()).isGreaterThan(0);
        long refusedAt = System.nanoTime();

        Thread.ofPlatform().start(() -> {
            try {
                client.getJson("/throttled", ResourceUnitMeter.SINGLE_ITEM, List.of());
            } finally {
                done.countDown();
            }
        });
        assertThat(done.await(30, TimeUnit.SECONDS)).isTrue();

        // The second thread was never refused itself, and still had to wait: the cap is per application
        // per tenant, so a sibling continuing at full rate spends the budget that just ran out.
        List<Long> afterRefusal = requestTimes.stream().filter(when -> when > refusedAt).toList();
        assertThat(afterRefusal).hasSizeGreaterThanOrEqualTo(2);
        assertThat(afterRefusal).allSatisfy(when ->
                assertThat(when - refusedAt).isGreaterThanOrEqualTo(TimeUnit.MILLISECONDS.toNanos(900)));
    }

    @Test
    void reportsAPreferenceThatWasNotApplied() {
        handle("/v1.0/delta", exchange -> {
            // No Preference-Applied header: the tenant declined, which is what happens without
            // Sites.FullControl.All.
            respond(exchange, 200, "{\"value\":[]}");
        });

        GraphHttpClient client = clientWith(ResourceUnitMeter.unmetered());
        GraphHttpClient.GraphResponse response = client.getJson("/delta", ResourceUnitMeter.DELTA_WITH_TOKEN,
                List.of("hierarchicalsharing", "deltashowremovedasdeleted"));

        assertThat(response.allPreferencesApplied()).isFalse();
        assertThat(response.unappliedPreferences())
                .containsExactly("hierarchicalsharing", "deltashowremovedasdeleted");
    }

    @Test
    void sendsPreferencesAsOneHeaderAndReadsWhatCameBack() {
        List<String> prefer = new CopyOnWriteArrayList<>();
        handle("/v1.0/delta", exchange -> {
            prefer.add(String.valueOf(exchange.getRequestHeaders().getFirst("Prefer")));
            exchange.getResponseHeaders().add("Preference-Applied", "hierarchicalsharing");
            respond(exchange, 200, "{\"value\":[]}");
        });

        GraphHttpClient client = clientWith(ResourceUnitMeter.unmetered());
        GraphHttpClient.GraphResponse response =
                client.getJson("/delta", ResourceUnitMeter.DELTA_WITH_TOKEN, List.of("hierarchicalsharing"));

        assertThat(prefer).containsExactly("hierarchicalsharing");
        assertThat(response.allPreferencesApplied()).isTrue();
        assertThat(response.unappliedPreferences()).isEmpty();
    }

    @Test
    void followsTheContentRedirectWithoutSendingOurBearerTokenOnward() throws Exception {
        List<String> storageAuthorization = new CopyOnWriteArrayList<>();
        handle("/v1.0/items/1/content", exchange -> {
            exchange.getResponseHeaders().add("Location", baseUrl + "/storage/blob");
            exchange.sendResponseHeaders(302, -1);
        });
        handle("/v1.0/storage/blob", exchange -> {
            // A signed storage URL rejects a request that also carries a bearer token, and the JDK client
            // does not strip Authorization across a redirect, which is why this is followed by hand.
            storageAuthorization.add(String.valueOf(
                    exchange.getRequestHeaders().getFirst("Authorization")));
            respond(exchange, 200, "file bytes");
        });

        GraphHttpClient client = clientWith(ResourceUnitMeter.unmetered());
        try (InputStream body = client.getStream("/items/1/content", ResourceUnitMeter.CONTENT_DOWNLOAD)) {
            assertThat(new String(body.readAllBytes(), StandardCharsets.UTF_8)).isEqualTo("file bytes");
        }

        assertThat(storageAuthorization).containsExactly("null");
    }

    @Test
    void servesAnInlineContentResponseWithoutARedirect() throws Exception {
        handle("/v1.0/items/2/content", exchange -> respond(exchange, 200, "small file"));

        GraphHttpClient client = clientWith(ResourceUnitMeter.unmetered());
        try (InputStream body = client.getStream("/items/2/content", ResourceUnitMeter.CONTENT_DOWNLOAD)) {
            assertThat(new String(body.readAllBytes(), StandardCharsets.UTF_8)).isEqualTo("small file");
        }
    }

    @Test
    void doesNotRetryAPermissionFailureAndSaysWhatToCheck() {
        handle("/v1.0/sites", exchange ->
                respond(exchange, 403, "{\"error\":{\"code\":\"accessDenied\"}}"));

        GraphHttpClient client = clientWith(ResourceUnitMeter.unmetered());

        assertThatThrownBy(() -> client.getJson("/sites", ResourceUnitMeter.MULTI_ITEM_QUERY, List.of()))
                .isInstanceOf(GraphException.class)
                .hasMessageContaining("403")
                .hasMessageContaining("application permissions");
        // Retrying a consent problem turns a clear failure into a slow one.
        assertThat(requestLog).hasSize(1);
    }

    @Test
    void returnsAGoneResponseToTheCallerInsteadOfThrowing() {
        handle("/v1.0/delta", exchange ->
                respond(exchange, 410, "{\"error\":{\"code\":\"resyncChangesApplyDifferences\"}}"));

        GraphHttpClient client = clientWith(ResourceUnitMeter.unmetered());
        GraphHttpClient.GraphResponse response =
                client.getJson("/delta", ResourceUnitMeter.DELTA_WITH_TOKEN, List.of());

        // A stale delta token is an ordinary event the SPI models as SourceChangePage.expired(), so it
        // must not arrive as an exception.
        assertThat(response.body().get("error").get("code").asText())
                .isEqualTo("resyncChangesApplyDifferences");
    }

    @Test
    void countsWhatEachCallCostsSoTheCrawlCanReportItRatherThanEstimateIt() {
        handle("/v1.0/items/1/permissions", exchange -> respond(exchange, 200, "{\"value\":[]}"));

        ResourceUnitMeter meter = new ResourceUnitMeter(10_000, 100);
        GraphHttpClient client = clientWith(meter);
        client.getJson("/items/1/permissions", ResourceUnitMeter.PERMISSIONS, List.of());

        // A permission operation is 5 units, which is why counting requests would measure the wrong thing.
        assertThat(meter.unitsSpent()).isEqualTo(5);
    }

    @Test
    void pacesCallsWhenTheBudgetRunsOut() {
        handle("/v1.0/items/1", exchange -> respond(exchange, 200, "{\"id\":\"1\"}"));

        // 60 units a minute is one a second, and a burst of one, so the second call has to wait.
        ResourceUnitMeter meter = new ResourceUnitMeter(60, 1);
        GraphHttpClient client = clientWith(meter);

        long start = System.nanoTime();
        client.getJson("/items/1", ResourceUnitMeter.SINGLE_ITEM, List.of());
        client.getJson("/items/1", ResourceUnitMeter.SINGLE_ITEM, List.of());
        long elapsedMillis = (System.nanoTime() - start) / 1_000_000L;

        assertThat(meter.enabled()).isTrue();
        assertThat(elapsedMillis).isGreaterThanOrEqualTo(500);
        assertThat(meter.waitedMillis()).isGreaterThan(0);
    }

    @Test
    void doesNotPauseBeforeTheFirstRequestOfACrawl() {
        handle("/v1.0/items/1", exchange -> respond(exchange, 200, "{\"id\":\"1\"}"));

        GraphHttpClient client = clientWith(ResourceUnitMeter.unmetered());

        // Regression: the tenant-wide pause was seeded with Long.MIN_VALUE, and nanoTime() is routinely a
        // large positive number, so the remaining-time subtraction overflowed positive and the first
        // request of every crawl slept effectively forever.
        long start = System.nanoTime();
        client.getJson("/items/1", ResourceUnitMeter.SINGLE_ITEM, List.of());
        long elapsedMillis = (System.nanoTime() - start) / 1_000_000L;

        assertThat(elapsedMillis).isLessThan(5_000);
    }

    @Test
    void doesNotStallOnACallThatCostsMoreThanTheBucketCanHold() {
        handle("/v1.0/items/1/permissions", exchange -> respond(exchange, 200, "{\"value\":[]}"));

        // A 5-unit permission read against a bucket that holds 2. Waiting for 5 to accumulate in a bucket
        // capped at 2 would never end.
        ResourceUnitMeter meter = new ResourceUnitMeter(6_000, 2);
        GraphHttpClient client = clientWith(meter);

        client.getJson("/items/1/permissions", ResourceUnitMeter.PERMISSIONS, List.of());

        assertThat(meter.unitsSpent()).isEqualTo(5);
    }

    @Test
    void refusesToBeBuiltWithoutABaseUrl() {
        assertThatThrownBy(() -> new GraphHttpClient("  ", new StaticTokenProvider("t"),
                ResourceUnitMeter.unmetered()))
                .isInstanceOf(GraphException.class)
                .hasMessageContaining("graph-base-url");
    }

    /** Lets a test body throw {@link IOException} the way an exchange handler needs to. */
    private interface Handler {
        void handle(HttpExchange exchange) throws IOException;
    }
}
