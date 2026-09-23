package org.hyland.contentlake.connector.sharepoint;

import com.microsoft.aad.msal4j.HttpMethod;
import com.microsoft.aad.msal4j.HttpRequest;
import com.microsoft.aad.msal4j.IHttpClient;
import com.microsoft.aad.msal4j.IHttpResponse;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Drives the real msal4j public-client flow over a loopback endpoint, in the manner of
 * {@link ClientCredentialsTokenProviderTest}.
 *
 * <p>msal4j rejects an authority that is not {@code https}, so the mock Graph service cannot stand in for
 * Entra ID and this is the only way to assert the wire form of a refresh. What these tests cannot prove is
 * a real device-code sign-in; that needs a tenant and an app registration, and it is the one part of this
 * mode a local run does not exercise.</p>
 */
class DeviceCodeTokenProviderTest {

    private static final String AUTHORITY = "https://login.microsoftonline.com/contoso.onmicrosoft.com/";

    private static final String INSTANCE_DISCOVERY = """
            {"tenant_discovery_endpoint":\
            "https://login.microsoftonline.com/contoso.onmicrosoft.com/v2.0/.well-known/openid-configuration",\
            "api-version":"1.1","metadata":[{"preferred_network":"login.microsoftonline.com",\
            "preferred_cache":"login.windows.net","aliases":["login.microsoftonline.com",\
            "login.windows.net","login.microsoft.com","sts.windows.net"]}]}""";

    /**
     * A token response. The id token is what msal4j reads the account's username out of, so a cache built
     * from this response has a usable account rather than an anonymous one.
     *
     * <p>The lifetime is a parameter because it is what makes the refresh path reachable. msal4j serves a
     * cached access token that is still valid without touching the network, which is correct and is the
     * behaviour the caching test relies on. To assert the refresh grant at all, the token the seeding step
     * stores has to be one msal4j considers spent.</p>
     */
    private static final String TOKEN_RESPONSE = """
            {"token_type":"Bearer","expires_in":%d,"ext_expires_in":%d,\
            "scope":"https://graph.microsoft.com/Sites.Read.All",\
            "access_token":"mock-delegated-token","refresh_token":"mock-refresh-token",\
            "id_token":"%s","client_info":"%s"}""";

    /** Seeding hands out a spent access token; every later response is normal. */
    private static final int SPENT = 1;
    private static final int FRESH = 3599;

    private final java.util.concurrent.atomic.AtomicBoolean seeding =
            new java.util.concurrent.atomic.AtomicBoolean(true);

    @TempDir
    Path cacheDirectory;

    private HttpServer server;
    private String serverBase;
    private final List<String> bodies = new CopyOnWriteArrayList<>();

    private static boolean isInstanceDiscovery(String path) {
        return path.contains("/discovery/instance");
    }

    @BeforeEach
    void startTokenEndpoint() throws IOException {
        server = HttpServer.create(new InetSocketAddress(0), 0);
        serverBase = "http://localhost:" + server.getAddress().getPort();
        server.createContext("/", exchange -> {
            try {
                String path = exchange.getRequestURI().getPath();
                String payload;
                if (isInstanceDiscovery(path)) {
                    payload = INSTANCE_DISCOVERY;
                } else {
                    bodies.add(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
                    int lifetime = seeding.compareAndSet(true, false) ? SPENT : FRESH;
                    payload = TOKEN_RESPONSE.formatted(lifetime, lifetime, idToken(), clientInfo());
                }
                byte[] bytes = payload.getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().add("Content-Type", "application/json");
                exchange.sendResponseHeaders(200, bytes.length);
                exchange.getResponseBody().write(bytes);
            } finally {
                exchange.close();
            }
        });
        server.start();
    }

    @AfterEach
    void stopTokenEndpoint() {
        if (server != null) {
            server.stop(0);
        }
    }

    @Test
    void refreshesSilentlyFromACachePopulatedByASignIn() throws Exception {
        Path cache = seededCache();

        DeviceCodeTokenProvider provider = new DeviceCodeTokenProvider(
                AUTHORITY, "client-id", List.of("https://graph.microsoft.com/Sites.Read.All"), cache,
                new LoopbackTransport());

        assertThat(provider.token()).isEqualTo("mock-delegated-token");

        // The grant is a refresh, never an interactive one: the container must not be able to prompt.
        assertThat(bodies).hasSize(1);
        assertThat(bodies.get(0)).contains("grant_type=refresh_token");
        assertThat(bodies.get(0)).contains("client_id=client-id");
        assertThat(bodies.get(0)).doesNotContain("device_code");
    }

    @Test
    void cachesTheTokenSoOneRefreshServesEveryGraphCall() throws Exception {
        // A provider that refreshed per request would spend the tenant's throttling budget on authentication,
        // which is what the GraphTokenProvider contract forbids. The first call refreshes because the seeded
        // access token is spent; the next two must be served from the cache.
        DeviceCodeTokenProvider provider = new DeviceCodeTokenProvider(
                AUTHORITY, "client-id", List.of("https://graph.microsoft.com/Sites.Read.All"), seededCache(),
                new LoopbackTransport());

        provider.token();
        provider.token();
        provider.token();

        assertThat(bodies).hasSize(1);
    }

    @Test
    void namesTheSignInCommandWhenTheCacheHoldsNoAccount() {
        Path empty = cacheDirectory.resolve("empty.json");

        DeviceCodeTokenProvider provider = new DeviceCodeTokenProvider(
                AUTHORITY, "client-id", List.of(), empty, new LoopbackTransport());

        assertThatThrownBy(provider::token)
                .isInstanceOf(GraphException.class)
                .hasMessageContaining("holds no signed-in account")
                .hasMessageContaining(SharePointDeviceLogin.COMMAND_HINT)
                .hasMessageContaining(empty.toString());
    }

    /**
     * The auth state a screen renders, answered from the cache without a network call.
     *
     * <p>{@code bodies} staying empty is the assertion that matters: a status endpoint may poll this, and a
     * provider that attempted a silent acquisition to answer it would make asking about the credential as
     * expensive as using it, and would fail whenever the directory was briefly unreachable.</p>
     */
    @Test
    void reportsItsStateFromTheCacheWithoutSpendingATokenRequest() throws Exception {
        DeviceCodeTokenProvider provider = new DeviceCodeTokenProvider(
                AUTHORITY, "client-id", List.of("https://graph.microsoft.com/Sites.Read.All"), seededCache(),
                new LoopbackTransport());

        assertThat(provider.mode()).isEqualTo("device-code");
        assertThat(provider.usable()).isTrue();
        assertThat(provider.identity()).isNotBlank();
        assertThat(provider.remedy()).isNull();

        // Nothing went to the token endpoint to answer any of the above.
        assertThat(bodies).isEmpty();
    }

    @Test
    void reportsAnEmptyCacheAsUnusableAndNamesTheCommandThatFixesIt() {
        DeviceCodeTokenProvider provider = new DeviceCodeTokenProvider(
                AUTHORITY, "client-id", List.of(), cacheDirectory.resolve("empty.json"),
                new LoopbackTransport());

        assertThat(provider.usable()).isFalse();
        assertThat(provider.identity()).isNull();
        assertThat(provider.remedy()).contains(SharePointDeviceLogin.COMMAND_HINT);
    }

    /**
     * The remedy reaches a browser, so it must not name the cache file.
     *
     * <p>The provider's own exception does name it, deliberately, because that goes to a log an engineer reads.
     * This is the same fact rendered for a different audience, and the path is a file worth attacking.</p>
     */
    @Test
    void keepsTheCachePathOutOfTheRemedyEvenThoughTheExceptionCarriesIt() {
        Path cache = cacheDirectory.resolve("secret-location.json");

        DeviceCodeTokenProvider provider = new DeviceCodeTokenProvider(
                AUTHORITY, "client-id", List.of(), cache, new LoopbackTransport());

        assertThat(provider.remedy())
                .isNotNull()
                .doesNotContain(cache.toString())
                .doesNotContain("secret-location");
        // And the log-facing message still does, so this is a difference in audience rather than a regression.
        assertThatThrownBy(provider::token).hasMessageContaining(cache.toString());
    }

    @Test
    void refusesToGuessAtACacheItCannotParse() throws IOException {
        Path corrupt = cacheDirectory.resolve("corrupt.json");
        Files.writeString(corrupt, "this is not a token cache");

        assertThatThrownBy(() -> new DeviceCodeTokenProvider(
                AUTHORITY, "client-id", List.of(), corrupt, new LoopbackTransport()).token())
                .isInstanceOf(GraphException.class)
                .hasMessageContaining(corrupt.toString());
    }

    @Test
    void refusesToBuildWithoutACachePath() {
        // Without one there is nowhere to read the refresh token a sign-in produced, and the failure should
        // name the setting rather than surface on the first Graph call.
        assertThatThrownBy(() -> new DeviceCodeTokenProvider(AUTHORITY, "client-id", List.of(), null))
                .isInstanceOf(GraphException.class)
                .hasMessageContaining(SharePointConnectorPlugin.TOKEN_CACHE_PATH_SETTING);
    }

    @Test
    void isNotOfferedAsAProductionMode() {
        // A revoked refresh token needs a human, so the host has to be able to say so at startup.
        DeviceCodeTokenProvider provider = new DeviceCodeTokenProvider(
                AUTHORITY, "client-id", List.of(), cacheDirectory.resolve("any.json"),
                new LoopbackTransport());

        assertThat(provider.supportedInProduction()).isFalse();
    }

    @Test
    void neverPutsTheTokenOrTheCacheContentsInItsDescription() throws Exception {
        // describe() goes into logs that get pasted into issues.
        Path cache = seededCache();
        String serialized = Files.readString(cache);

        DeviceCodeTokenProvider provider = new DeviceCodeTokenProvider(
                AUTHORITY, "client-id", List.of("https://graph.microsoft.com/Sites.Read.All"), cache,
                new LoopbackTransport());
        provider.token();

        assertThat(provider.describe())
                .doesNotContain("mock-delegated-token")
                .doesNotContain("mock-refresh-token")
                .doesNotContain(serialized)
                .contains("device-code");
    }

    @Test
    void defaultsToTheLeastPrivilegedScopeThatServesEveryCallPlusOfflineAccess() {
        // Sites.Read.All is what was measured working for delta, permission reads and content download;
        // offline_access is what yields the refresh token this whole mode depends on.
        assertThat(SharePointDeviceLogin.effectiveScopes(List.of()))
                .containsExactly("https://graph.microsoft.com/Sites.Read.All", "offline_access");
    }

    @Test
    void dropsBlankConfiguredScopesRatherThanSendingThemToEntra() {
        assertThat(SharePointDeviceLogin.effectiveScopes(List.of("  Files.Read  ", "", "   ")))
                .containsExactly("Files.Read");

        assertThatThrownBy(() -> SharePointDeviceLogin.effectiveScopes(List.of("", "  ")))
                .isInstanceOf(GraphException.class)
                .hasMessageContaining(SharePointConnectorPlugin.SCOPES_SETTING);
    }

    /**
     * A cache holding one account and a refresh token, as a sign-in would leave it.
     *
     * <p>Built by letting msal4j serialize a real refresh response rather than by hand-writing its cache
     * format, which is internal and would drift.</p>
     */
    private Path seededCache() throws Exception {
        Path cache = cacheDirectory.resolve("msal-cache.json");
        com.microsoft.aad.msal4j.PublicClientApplication seeding =
                SharePointDeviceLogin.application(AUTHORITY, "client-id", cache, new LoopbackTransport());
        seeding.acquireToken(com.microsoft.aad.msal4j.RefreshTokenParameters
                        .builder(java.util.Set.of("https://graph.microsoft.com/Sites.Read.All"),
                                "seed-refresh-token")
                        .build())
                .get();
        bodies.clear();
        assertThat(Files.exists(cache)).as("the sign-in wrote a cache").isTrue();
        return cache;
    }

    /** Base64url of a minimal id token payload, which is where msal4j reads the account username from. */
    private static String idToken() {
        String header = base64Url("{\"alg\":\"none\",\"typ\":\"JWT\"}");
        String payload = base64Url("{\"aud\":\"client-id\",\"iss\":\"https://login.microsoftonline.com/"
                + "tenant-guid/v2.0\",\"oid\":\"user-object-id\",\"tid\":\"tenant-guid\","
                + "\"preferred_username\":\"crawler@contoso.onmicrosoft.com\"}");
        return header + "." + payload + ".";
    }

    private static String clientInfo() {
        return base64Url("{\"uid\":\"user-object-id\",\"utid\":\"tenant-guid\"}");
    }

    private static String base64Url(String value) {
        return java.util.Base64.getUrlEncoder().withoutPadding()
                .encodeToString(value.getBytes(StandardCharsets.UTF_8));
    }

    /** Sends msal4j's requests to the loopback server, preserving the path so the grant type is assertable. */
    private final class LoopbackTransport implements IHttpClient {

        @Override
        public IHttpResponse send(HttpRequest request) throws Exception {
            URI target = URI.create(serverBase + request.url().getPath());

            java.net.http.HttpRequest.Builder builder = java.net.http.HttpRequest.newBuilder(target);
            request.headers().forEach((name, value) -> {
                // Restricted headers the JDK client sets itself; copying them throws.
                if (!name.equalsIgnoreCase("Host") && !name.equalsIgnoreCase("Content-Length")) {
                    builder.header(name, value);
                }
            });
            if (request.httpMethod() == HttpMethod.POST) {
                builder.POST(java.net.http.HttpRequest.BodyPublishers.ofString(
                        request.body() == null ? "" : request.body()));
            } else {
                builder.GET();
            }

            java.net.http.HttpResponse<String> response = java.net.http.HttpClient.newHttpClient()
                    .send(builder.build(), java.net.http.HttpResponse.BodyHandlers.ofString());

            com.microsoft.aad.msal4j.HttpResponse result = new com.microsoft.aad.msal4j.HttpResponse();
            result.statusCode(response.statusCode());
            result.body(response.body());
            result.addHeaders(Map.of("Content-Type", List.of("application/json")));
            return result;
        }
    }
}
