package org.hyland.contentlake.connector.sharepoint;

import com.microsoft.aad.msal4j.HttpMethod;
import com.microsoft.aad.msal4j.HttpRequest;
import com.microsoft.aad.msal4j.IHttpClient;
import com.microsoft.aad.msal4j.IHttpResponse;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Exercises the real msal4j, not a stand-in for it.
 *
 * <p>This matters because token acquisition is the one part of the connector a mock Graph service cannot
 * cover: msal4j rejects an authority that is not {@code https} -- measured, {@code build()} throws
 * {@code authority should use the 'https' scheme}, and {@code validateAuthority(false)} skips instance
 * discovery rather than that check -- so the mock cannot double as an Entra ID. Replacing msal4j's
 * transport instead leaves the library doing the real work: it builds the real client credentials grant,
 * posts it, parses the real token response shape and caches the result. Only the socket is local.</p>
 *
 * <p>The authority string below is therefore the production one, and the requests are answered from a
 * loopback server.</p>
 */
class ClientCredentialsTokenProviderTest {

    private static final String AUTHORITY = "https://login.microsoftonline.com/contoso.onmicrosoft.com/";

    /**
     * msal4j calls the instance discovery endpoint before the token endpoint, so the transport has to
     * answer both. Discovered by running this test: the first version answered every request with a token
     * response and msal4j failed with a bare {@code MsalServiceException} from
     * {@code AadInstanceDiscoveryProvider.validate}.
     *
     * <p>Worth knowing operationally, not just here: a deployment behind an egress allowlist needs
     * {@code login.microsoftonline.com} reachable for discovery as well as for the token itself.</p>
     */
    private static final String INSTANCE_DISCOVERY = """
            {"tenant_discovery_endpoint":\
            "https://login.microsoftonline.com/contoso.onmicrosoft.com/v2.0/.well-known/openid-configuration",\
            "api-version":"1.1","metadata":[{"preferred_network":"login.microsoftonline.com",\
            "preferred_cache":"login.windows.net","aliases":["login.microsoftonline.com",\
            "login.windows.net","login.microsoft.com","sts.windows.net"]}]}""";

    private static final String TOKEN_RESPONSE = """
            {"token_type":"Bearer","expires_in":3599,"ext_expires_in":3599,\
            "access_token":"mock-graph-token"}""";

    private static boolean isInstanceDiscovery(String path) {
        return path.contains("/discovery/instance");
    }

    private HttpServer server;
    private String serverBase;
    private final List<String> bodies = new CopyOnWriteArrayList<>();

    @BeforeEach
    void startTokenEndpoint() throws IOException {
        server = HttpServer.create(new InetSocketAddress(0), 0);
        serverBase = "http://localhost:" + server.getAddress().getPort();
        server.createContext("/", exchange -> {
            try {
                String path = exchange.getRequestURI().getPath();
                byte[] bytes = (isInstanceDiscovery(path) ? INSTANCE_DISCOVERY : TOKEN_RESPONSE)
                        .getBytes(StandardCharsets.UTF_8);
                if (!isInstanceDiscovery(path)) {
                    bodies.add(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
                }
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
    void acquiresAnAppOnlyTokenThroughTheRealClientCredentialsGrant() {
        LoopbackTransport transport = new LoopbackTransport();
        ClientCredentialsTokenProvider provider = new ClientCredentialsTokenProvider(
                AUTHORITY, "client-id", "client-secret", null, null, null, transport);

        assertThat(provider.token()).isEqualTo("mock-graph-token");

        // What msal4j actually put on the wire, which is the part worth asserting: the grant type and the
        // .default scope are what make this app-only rather than delegated.
        assertThat(bodies).hasSize(1);
        assertThat(bodies.get(0)).contains("grant_type=client_credentials");
        assertThat(bodies.get(0)).contains("client_id=client-id");
        assertThat(bodies.get(0)).contains("scope=https%3A%2F%2Fgraph.microsoft.com%2F.default");
        assertThat(transport.requests).allSatisfy(url ->
                assertThat(url).contains("/contoso.onmicrosoft.com/oauth2/v2.0/token"));
    }

    @Test
    void reusesTheCachedTokenRatherThanSpendingTheBudgetOnAuthentication() {
        ClientCredentialsTokenProvider provider = new ClientCredentialsTokenProvider(
                AUTHORITY, "client-id", "client-secret", null, null, null, new LoopbackTransport());

        String first = provider.token();
        String second = provider.token();
        String third = provider.token();

        assertThat(first).isEqualTo(second).isEqualTo(third);
        // token() is called once per Graph request, so a provider that re-authenticated each time would
        // triple the connector's traffic to Entra.
        assertThat(bodies).hasSize(1);
    }

    @Test
    void reportsAFailedAcquisitionWithTheConfigurationThatProducedIt() {
        server.stop(0);
        server = null;

        ClientCredentialsTokenProvider provider = new ClientCredentialsTokenProvider(
                AUTHORITY, "client-id", "client-secret", null, null, null, new LoopbackTransport());

        assertThatThrownBy(provider::token)
                .isInstanceOf(GraphException.class)
                .hasMessageContaining("client credentials with a secret")
                .hasMessageContaining("client-id");
    }

    @Test
    void refusesBothCredentialsAtOnceRatherThanPickingOne() {
        Path certificate = Path.of("/tmp/does-not-matter.pfx");

        assertThatThrownBy(() -> new ClientCredentialsTokenProvider(
                AUTHORITY, "client-id", "client-secret", certificate, "pw", null, null))
                .isInstanceOf(GraphException.class)
                // Silently preferring one would leave an operator convinced they had rotated a credential
                // that was never in use.
                .hasMessageContaining("supply exactly one");
    }

    @Test
    void refusesWithNoCredentialAtAll() {
        assertThatThrownBy(() -> new ClientCredentialsTokenProvider(
                AUTHORITY, "client-id", null, null, null, null, null))
                .isInstanceOf(GraphException.class)
                .hasMessageContaining("client-secret")
                .hasMessageContaining("certificate-path");
    }

    @Test
    void namesTheCertificatePathWhenItCannotBeRead() {
        Path missing = Path.of("/tmp/absent-" + System.nanoTime() + ".pfx");

        assertThatThrownBy(() -> new ClientCredentialsTokenProvider(
                AUTHORITY, "client-id", null, missing, "pw", null, null))
                .isInstanceOf(GraphException.class)
                .hasMessageContaining(missing.toString());
    }

    @Test
    void acquiresATokenWithACertificateCredential() throws Exception {
        Path pkcs12 = generatePkcs12("secret-password");
        try {
            LoopbackTransport transport = new LoopbackTransport();
            ClientCredentialsTokenProvider provider = new ClientCredentialsTokenProvider(
                    AUTHORITY, "client-id", null, pkcs12, "secret-password", null, transport);

            assertThat(provider.token()).isEqualTo("mock-graph-token");
            // A certificate grant is a signed assertion rather than a shared secret, which is the whole
            // reason to prefer it: nothing that expires on a date nobody diarised.
            assertThat(bodies.get(0)).contains("client_assertion_type=urn%3Aietf%3Aparams%3Aoauth%3A"
                    + "client-assertion-type%3Ajwt-bearer");
            assertThat(bodies.get(0)).contains("client_assertion=");
            assertThat(provider.describe()).contains(pkcs12.toString()).doesNotContain("secret-password");
        } finally {
            Files.deleteIfExists(pkcs12);
        }
    }

    @Test
    void rejectsAWrongCertificatePasswordWithBothThingsToCheck() throws Exception {
        Path pkcs12 = generatePkcs12("right-password");
        try {
            assertThatThrownBy(() -> new ClientCredentialsTokenProvider(
                    AUTHORITY, "client-id", null, pkcs12, "wrong-password", null, null))
                    .isInstanceOf(GraphException.class)
                    .hasMessageContaining("PKCS#12")
                    .hasMessageContaining("certificate-password");
        } finally {
            Files.deleteIfExists(pkcs12);
        }
    }

    /**
     * Builds a throwaway PKCS#12 with {@code keytool}, which ships with every JDK this builds on.
     *
     * <p>Generating one in process would mean a certificate library this connector has no other use for.</p>
     */
    private static Path generatePkcs12(String password) throws Exception {
        Path file = Files.createTempFile("sharepoint-connector-test-", ".pfx");
        Files.delete(file);
        Path keytool = Path.of(System.getProperty("java.home"), "bin", "keytool");
        Process process = new ProcessBuilder(
                keytool.toString(),
                "-genkeypair",
                "-alias", "test",
                "-keyalg", "RSA",
                "-keysize", "2048",
                "-dname", "CN=content-lake-sharepoint-test",
                "-validity", "1",
                "-storetype", "PKCS12",
                "-keystore", file.toString(),
                "-storepass", password,
                "-keypass", password)
                .redirectErrorStream(true)
                .start();
        if (!process.waitFor(60, TimeUnit.SECONDS) || process.exitValue() != 0) {
            String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
            throw new IllegalStateException("keytool could not create a test certificate: " + output);
        }
        return file;
    }

    /**
     * msal4j's transport, pointed at the loopback server.
     *
     * <p>Only the host and port are replaced. The path, method, headers and body are the ones msal4j built,
     * so what the server sees is a real client credentials request.</p>
     */
    private final class LoopbackTransport implements IHttpClient {

        private final List<String> requests = new CopyOnWriteArrayList<>();

        @Override
        public IHttpResponse send(HttpRequest request) throws Exception {
            requests.add(request.url().toString());
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
