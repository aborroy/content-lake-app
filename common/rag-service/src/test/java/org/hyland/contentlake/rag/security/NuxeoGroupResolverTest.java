package org.hyland.contentlake.rag.security;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class NuxeoGroupResolverTest {

    private HttpServer server;
    private final List<String> requestedUris = new ArrayList<>();

    @AfterEach
    void tearDown() {
        if (server != null) {
            server.stop(0);
        }
    }

    @Test
    void declaresTheNuxeoSourceType() {
        assertThat(resolverFor("http://localhost:1/nuxeo").sourceType()).isEqualTo("nuxeo");
    }

    @Test
    void readsTheDirectGroupsAndPrefixesThem() throws Exception {
        // Nuxeo names groups without a prefix while the ingested ACLs store them as GROUP_<name>.
        start(exchange -> respond(exchange, 200, "{\"id\":\"alice\",\"groups\":[\"members\",\"sales\"]}"));

        assertThat(resolver().resolveGroups("alice"))
                .containsExactly("GROUP_members", "GROUP_sales");
    }

    @Test
    void leavesAnAlreadyPrefixedGroupAlone() throws Exception {
        start(exchange -> respond(exchange, 200, "{\"groups\":[\"GROUP_members\"]}"));

        assertThat(resolver().resolveGroups("alice")).containsExactly("GROUP_members");
    }

    @Test
    void readsExtendedGroupsWhichArriveAsObjects() throws Exception {
        start(exchange -> respond(exchange, 200, """
                {"groups":["members"],
                 "extendedGroups":[{"name":"administrators","label":"Administrators"},
                                   {"groupname":"powerusers"},
                                   {"id":"auditors"}]}"""));

        assertThat(resolver().resolveGroups("alice")).containsExactly(
                "GROUP_members", "GROUP_administrators", "GROUP_powerusers", "GROUP_auditors");
    }

    @Test
    void readsTheGroupsHeldOnTheUserSchema() throws Exception {
        // Which of the three places carries groups depends on the instance's user directory, so reading
        // only one loses memberships on some deployments.
        start(exchange -> respond(exchange, 200,
                "{\"properties\":{\"username\":\"alice\",\"groups\":[\"schema-group\"]}}"));

        assertThat(resolver().resolveGroups("alice")).containsExactly("GROUP_schema-group");
    }

    @Test
    void deduplicatesAGroupNamedInMoreThanOnePlace() throws Exception {
        start(exchange -> respond(exchange, 200, """
                {"groups":["members"],
                 "extendedGroups":[{"name":"members"}],
                 "properties":{"groups":["members"]}}"""));

        assertThat(resolver().resolveGroups("alice")).containsExactly("GROUP_members");
    }

    @Test
    void ignoresBlankAndNullNames() throws Exception {
        start(exchange -> respond(exchange, 200,
                "{\"groups\":[\"members\",\"  \",null],\"extendedGroups\":[{\"label\":\"no name\"}]}"));

        assertThat(resolver().resolveGroups("alice")).containsExactly("GROUP_members");
    }

    @Test
    void aNotFoundMeansTheDirectoryHoldsNoSuchIdentity() throws Exception {
        // Not a failure: the caller keeps the source with their default authorities.
        start(exchange -> respond(exchange, 404, "{\"entity-type\":\"exception\",\"status\":404}"));

        assertThat(resolver().resolveGroups("alice")).isNull();
    }

    @Test
    void aUserInNoGroupsIsAKnownAnswer() throws Exception {
        start(exchange -> respond(exchange, 200, "{\"id\":\"alice\",\"groups\":[]}"));

        assertThat(resolver().resolveGroups("alice")).isEmpty();
    }

    @Test
    void aServerErrorPropagatesSoTheFailurePolicyApplies() throws Exception {
        start(exchange -> respond(exchange, 503, "{}"));

        assertThatThrownBy(() -> resolver().resolveGroups("alice"))
                .isInstanceOf(RuntimeException.class);
    }

    @Test
    void encodesAUsernameThatCarriesUrlSyntax() throws Exception {
        start(exchange -> respond(exchange, 200, "{\"groups\":[\"members\"]}"));

        assertThat(resolver().resolveGroups("a b")).containsExactly("GROUP_members");
        assertThat(requestedUris.getFirst()).contains("/user/a%20b");
    }

    @Test
    void authenticatesWithTheConfiguredAccount() throws Exception {
        List<String> authorizations = new ArrayList<>();
        start(exchange -> {
            authorizations.add(exchange.getRequestHeaders().getFirst("Authorization"));
            respond(exchange, 200, "{\"groups\":[]}");
        });

        resolver().resolveGroups("alice");

        assertThat(authorizations).containsExactly("Basic " + Base64.getEncoder()
                .encodeToString("Administrator:secret".getBytes(StandardCharsets.UTF_8)));
    }

    @Test
    void appendsTheApiRootToTheConfiguredBaseUrl() throws Exception {
        start(exchange -> respond(exchange, 200, "{\"groups\":[]}"));

        resolverFor(baseUrl() + "/nuxeo").resolveGroups("alice");

        assertThat(requestedUris.getFirst()).isEqualTo("/nuxeo/api/v1/user/alice");
    }

    @Test
    void toleratesABaseUrlThatAlreadyNamesTheApiRoot() throws Exception {
        start(exchange -> respond(exchange, 200, "{\"groups\":[]}"));

        resolverFor(baseUrl() + "/nuxeo/api/v1").resolveGroups("alice");

        assertThat(requestedUris.getFirst()).isEqualTo("/nuxeo/api/v1/user/alice");
    }

    @Test
    void toleratesABaseUrlWithATrailingSlash() throws Exception {
        start(exchange -> respond(exchange, 200, "{\"groups\":[]}"));

        resolverFor(baseUrl() + "/nuxeo/").resolveGroups("alice");

        assertThat(requestedUris.getFirst()).isEqualTo("/nuxeo/api/v1/user/alice");
    }

    private NuxeoGroupResolver resolver() {
        return resolverFor(baseUrl() + "/nuxeo");
    }

    private static NuxeoGroupResolver resolverFor(String baseUrl) {
        return new NuxeoGroupResolver(baseUrl, "Administrator", "secret");
    }

    private void start(ExchangeHandler handler) throws IOException {
        server = HttpServer.create(new InetSocketAddress(0), 0);
        server.createContext("/", exchange -> {
            try {
                requestedUris.add(exchange.getRequestURI().toString());
                handler.handle(exchange);
            } finally {
                exchange.close();
            }
        });
        server.start();
    }

    private String baseUrl() {
        return "http://127.0.0.1:" + server.getAddress().getPort();
    }

    private static void respond(HttpExchange exchange, int statusCode, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(statusCode, bytes.length);
        exchange.getResponseBody().write(bytes);
    }

    @FunctionalInterface
    private interface ExchangeHandler {
        void handle(HttpExchange exchange) throws IOException;
    }
}
