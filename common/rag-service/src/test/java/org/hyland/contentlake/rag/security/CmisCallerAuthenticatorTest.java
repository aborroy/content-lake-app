package org.hyland.contentlake.rag.security;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.hyland.contentlake.security.CallerCredentials;
import org.hyland.contentlake.security.CallerIdentities;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.web.client.RestClient;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * CMIS caller authentication, over a real HTTP server rather than a mocked client.
 *
 * <p>The server is what makes the response-shape assertions mean anything: the point of checking the body is
 * that a 2xx from something that is not a CMIS repository must not authenticate anyone, and a mocked client
 * would be asserting the check against itself.</p>
 */
class CmisCallerAuthenticatorTest {

    private static final String BROWSER_SERVICE_DOCUMENT = """
            {"-default-":{"repositoryId":"-default-","repositoryName":"Main Repository",\
            "cmisVersionSupported":"1.1"}}""";

    private static final String ATOMPUB_SERVICE_DOCUMENT = """
            <?xml version="1.0"?><service xmlns:cmisra="http://docs.oasis-open.org/ns/cmis/restatom/200908/">\
            <workspace><cmisra:repositoryInfo><cmisra:repositoryId>-default-</cmisra:repositoryId>\
            </cmisra:repositoryInfo></workspace></service>""";

    private HttpServer server;
    private final AtomicInteger requests = new AtomicInteger();

    @AfterEach
    void tearDown() {
        if (server != null) {
            server.stop(0);
        }
    }

    @Nested
    class Authentication {

        @Test
        void aBrowserBindingServiceDocumentAuthenticatesTheCaller() throws Exception {
            serveFor("alice", "secret", 200, BROWSER_SERVICE_DOCUMENT);

            CallerIdentities identities = authenticator("").authenticate(login("alice", "secret"));

            assertThat(identities).isNotNull();
            // One untyped identity: bare "alice", so nothing is re-bucketed and it answers for every source.
            assertThat(identities.describe()).isEqualTo("alice");
            assertThat(identities.usernameFor("cmis")).isEqualTo("alice");
            assertThat(identities.usernameFor("anything-else")).isEqualTo("alice");
        }

        @Test
        void anAtomPubServiceDocumentAlsoAnswers() throws Exception {
            serveFor("alice", "secret", 200, ATOMPUB_SERVICE_DOCUMENT);

            assertThat(authenticator("").authenticate(login("alice", "secret"))).isNotNull();
        }

        @Test
        void theUsernameSurvivesVerbatimIncludingItsCase() throws Exception {
            // A CMIS repository reports no canonical username, so whatever was presented is what lands in the
            // principal and is matched against the ACE. Pinned so nobody tidies it into lower case later:
            // some repositories are case-sensitive and normalising would stop the principal matching at all.
            serveFor("Alice.Smith@CORP.example", "secret", 200, BROWSER_SERVICE_DOCUMENT);

            CallerIdentities identities = authenticator("")
                    .authenticate(login("Alice.Smith@CORP.example", "secret"));

            assertThat(identities.describe()).isEqualTo("Alice.Smith@CORP.example");
        }

        @Test
        void aRejectedPasswordIsADeclineRatherThanAnException() throws Exception {
            serveFor("alice", "secret", 200, BROWSER_SERVICE_DOCUMENT);

            // A 401 says nothing about the other authorities, so it must not deny a caller one of them would
            // have accepted.
            assertThat(authenticator("").authenticate(login("alice", "wrong"))).isNull();
        }

        @Test
        void anUnreachableRepositoryDeclines() {
            // Port 0 is never listening, so this is an I/O failure rather than a status.
            CmisRepositoryDirectory directory = new CmisRepositoryDirectory(
                    clientFor("http://127.0.0.1:1"), "");

            assertThat(new CmisCallerAuthenticator(directory).authenticate(login("alice", "secret"))).isNull();
        }
    }

    @Nested
    class TheBodyIsCheckedNotJustTheStatus {

        @Test
        void aTwoHundredThatIsNotAServiceDocumentDeclines() throws Exception {
            // The defect this guards: pointed at any authenticated HTTP service, every one of its users
            // would otherwise become a caller here.
            serveFor("alice", "secret", 200, "{\"status\":\"UP\",\"version\":\"1.0\"}");

            assertThat(authenticator("").authenticate(login("alice", "secret"))).isNull();
        }

        @Test
        void anEmptyTwoHundredDeclines() throws Exception {
            serveFor("alice", "secret", 200, "");

            assertThat(authenticator("").authenticate(login("alice", "secret"))).isNull();
        }

        @Test
        void aConfiguredRepositoryIdMustBePresentInTheBody() throws Exception {
            serveFor("alice", "secret", 200, BROWSER_SERVICE_DOCUMENT);

            assertThat(authenticator("-default-").authenticate(login("alice", "secret"))).isNotNull();
            // Right shape, wrong repository: a URL pointed at another CMIS server is not this one.
            assertThat(authenticator("other-repo").authenticate(login("alice", "secret"))).isNull();
        }
    }

    @Nested
    class CredentialsThatNeverReachTheRepository {

        @Test
        void aBlankPasswordIsDeclinedWithoutACall() throws Exception {
            serveFor("alice", "", 200, BROWSER_SERVICE_DOCUMENT);
            CmisCallerAuthenticator authenticator = authenticator("");

            // Some repositories treat an empty password as an anonymous bind, which would accept any username.
            assertThat(authenticator.supports(login("alice", ""))).isFalse();
            assertThat(requests).hasValue(0);
        }

        @Test
        void aReservedPrincipalIsDeclinedWithoutACall() throws Exception {
            serveFor("x", "y", 200, BROWSER_SERVICE_DOCUMENT);
            CmisCallerAuthenticator authenticator = authenticator("");

            // Forwarding an Alfresco ticket would put it in a third repository's access log.
            assertThat(authenticator.supports(login("TICKET_abc", ""))).isFalse();
            assertThat(authenticator.supports(
                    login(MultiSourceAuthenticationProvider.NUXEO_TOKEN_PRINCIPAL_PREFIX + "t", ""))).isFalse();
            assertThat(requests).hasValue(0);
        }

        @Test
        void aBlankPrincipalIsDeclinedWithoutACall() throws Exception {
            serveFor("x", "y", 200, BROWSER_SERVICE_DOCUMENT);

            assertThat(authenticator("").supports(login("", "secret"))).isFalse();
            assertThat(requests).hasValue(0);
        }

        @Test
        void anOrdinaryPasswordLoginIsClaimed() throws Exception {
            serveFor("x", "y", 200, BROWSER_SERVICE_DOCUMENT);

            assertThat(authenticator("").supports(login("alice", "secret"))).isTrue();
        }
    }

    @Nested
    class Configuration {

        @Test
        void enabledWithNoUrlFailsAtStartupNamingTheSetting() {
            assertThatThrownBy(() -> new CmisRepositoryDirectory("", ""))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("rag.security.cmis.url");
            assertThatThrownBy(() -> new CmisRepositoryDirectory("   ", "x"))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("rag.security.cmis.url");
        }

        @Test
        void isConsultedAfterAlfrescoAndNuxeo() {
            CmisCallerAuthenticator authenticator =
                    new CmisCallerAuthenticator(new CmisRepositoryDirectory("http://cmis.example", ""));

            assertThat(authenticator.order()).isGreaterThan(CallerAuthenticatorOrder.ALFRESCO);
            assertThat(authenticator.order()).isGreaterThan(CallerAuthenticatorOrder.NUXEO);
            assertThat(authenticator.id()).isEqualTo("cmis-password");
        }
    }

    private CmisCallerAuthenticator authenticator(String repositoryId) {
        return new CmisCallerAuthenticator(new CmisRepositoryDirectory(clientFor(baseUrl()), repositoryId));
    }

    private static CallerCredentials login(String principal, String password) {
        return CallerCredentials.of(principal, password);
    }

    private static RestClient clientFor(String baseUrl) {
        SimpleClientHttpRequestFactory factory = new SimpleClientHttpRequestFactory();
        factory.setConnectTimeout(1_000);
        factory.setReadTimeout(2_000);
        return RestClient.builder().baseUrl(baseUrl).requestFactory(factory).build();
    }

    /** Serves {@code body} for exactly these credentials, and 401 for anything else. */
    private void serveFor(String username, String password, int status, String body) throws IOException {
        String expected = "Basic " + Base64.getEncoder().encodeToString(
                (username + ":" + password).getBytes(StandardCharsets.UTF_8));
        server = HttpServer.create(new InetSocketAddress(0), 0);
        server.createContext("/", exchange -> {
            try {
                requests.incrementAndGet();
                if (!expected.equals(exchange.getRequestHeaders().getFirst("Authorization"))) {
                    send(exchange, 401, "");
                    return;
                }
                send(exchange, status, body);
            } finally {
                exchange.close();
            }
        });
        server.start();
    }

    private String baseUrl() {
        return "http://127.0.0.1:" + server.getAddress().getPort();
    }

    private static void send(HttpExchange exchange, int status, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        if (bytes.length == 0) {
            exchange.sendResponseHeaders(status, -1);
            return;
        }
        exchange.sendResponseHeaders(status, bytes.length);
        exchange.getResponseBody().write(bytes);
    }
}
