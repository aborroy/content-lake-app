package org.hyland.contentlake.rag.security;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.MediaType;
import org.springframework.web.client.RestClient;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The resolver against a real HTTP server, because what matters here is the three answers the registry
 * distinguishes and none of them is observable through a mocked client.
 *
 * <p>{@code com.sun.net.httpserver.HttpServer} on port 0, the same technique
 * {@code MultiSourceAuthenticationProviderTest} uses next door.</p>
 */
class EntraGroupResolverTest {

    private HttpServer server;
    private String baseUrl;
    private final List<String> requestLog = new CopyOnWriteArrayList<>();
    private final List<String> authorization = new CopyOnWriteArrayList<>();

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

    private EntraGroupResolver resolverWith(String usernameSuffix) {
        server.start();
        RestClient client = RestClient.builder()
                .baseUrl(baseUrl)
                .defaultHeaders(headers -> headers.setAccept(List.of(MediaType.APPLICATION_JSON)))
                .build();
        return new EntraGroupResolver(client, () -> "test-token", "sharepoint", usernameSuffix);
    }

    private void handle(String path, Handler handler) {
        server.createContext(path, exchange -> {
            requestLog.add(exchange.getRequestURI().toString());
            authorization.add(String.valueOf(exchange.getRequestHeaders().getFirst("Authorization")));
            try {
                handler.handle(exchange);
            } finally {
                exchange.close();
            }
        });
    }

    private static void respond(HttpExchange exchange, int status, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().add("Content-Type", "application/json");
        exchange.sendResponseHeaders(status, bytes.length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(bytes);
        }
    }

    @Test
    void resolvesGroupsAsIdsPrefixedTheWayTheConnectorWritesThem() {
        handle("/v1.0/users/bob@contoso.com/transitiveMemberOf", exchange -> respond(exchange, 200, """
                {"value":[{"id":"group-guid-finance"},{"id":"group-guid-all-staff"}]}"""));

        List<String> groups = resolverWith("").resolveGroups("bob@contoso.com");

        // GROUP_<objectId>, which is exactly what SharePointAclMapper writes into the ACL. Anything else and
        // the filter matches nothing.
        assertThat(groups).containsExactly("GROUP_group-guid-finance", "GROUP_group-guid-all-staff");
        assertThat(authorization).allSatisfy(value -> assertThat(value).isEqualTo("Bearer test-token"));
    }

    @Test
    void asksForTransitiveMembershipSoNestedGroupsAreNotSilentlyLost() {
        handle("/v1.0/users/bob@contoso.com/transitiveMemberOf", exchange -> respond(exchange, 200,
                """
                {"value":[{"id":"outer-group"},{"id":"group-nested-inside-outer"}]}"""));

        List<String> groups = resolverWith("").resolveGroups("bob@contoso.com");

        // A SharePoint grant to a group whose members are other groups is ordinary. A direct memberOf read
        // would under-resolve it, and the documents would quietly not appear.
        assertThat(requestLog).allSatisfy(uri -> assertThat(uri).contains("/transitiveMemberOf"));
        assertThat(groups).contains("GROUP_group-nested-inside-outer");
    }

    @Test
    void followsEveryPageOfTheMembershipCollection() {
        handle("/v1.0/users/bob@contoso.com/transitiveMemberOf", exchange -> {
            if (exchange.getRequestURI().getQuery() != null
                    && exchange.getRequestURI().getQuery().contains("$skiptoken=page2")) {
                respond(exchange, 200, "{\"value\":[{\"id\":\"group-from-page-two\"}]}");
            } else {
                respond(exchange, 200, """
                        {"value":[{"id":"group-from-page-one"}],
                         "@odata.nextLink":"https://graph.microsoft.com/v1.0/users/bob@contoso.com/\
                        transitiveMemberOf/microsoft.graph.group?$skiptoken=page2"}""");
            }
        });

        List<String> groups = resolverWith("").resolveGroups("bob@contoso.com");

        // A membership collection read only as far as its first page under-resolves the caller, which looks
        // exactly like a document that was never ingested.
        assertThat(groups).containsExactly("GROUP_group-from-page-one", "GROUP_group-from-page-two");
    }

    @Test
    void returnsNullWhenEntraHasNoSuchIdentity() {
        handle("/v1.0/users/nobody@contoso.com/transitiveMemberOf", exchange ->
                respond(exchange, 404, """
                        {"error":{"code":"Request_ResourceNotFound","message":"Resource not found"}}"""));

        List<String> groups = resolverWith("").resolveGroups("nobody@contoso.com");

        // Null, not empty and not an exception. A user of another repository who is absent from Entra is the
        // ordinary case in a multi-source deployment, and it must cost them nothing but this source's group
        // grants.
        assertThat(groups).isNull();
    }

    @Test
    void throwsWhenTheDirectoryCouldNotBeAsked() {
        handle("/v1.0/users/bob@contoso.com/transitiveMemberOf", exchange ->
                respond(exchange, 503, "{\"error\":{\"code\":\"serviceNotAvailable\"}}"));

        // Distinct from "unknown here" on purpose: returning null for an unreachable directory would
        // silently downgrade a fail-closed deployment to a degraded one. The failure policy decides.
        assertThatThrownBy(() -> resolverWith("").resolveGroups("bob@contoso.com"))
                .isInstanceOf(RuntimeException.class);
    }

    @Test
    void throwsWhenATokenCannotBeAcquired() {
        server.start();
        RestClient client = RestClient.builder().baseUrl(baseUrl).build();
        EntraGroupResolver resolver = new EntraGroupResolver(client,
                () -> { throw new IllegalStateException("no token"); }, "sharepoint", "");

        // Also a directory that could not be asked, so also an exception rather than a null.
        assertThatThrownBy(() -> resolver.resolveGroups("bob@contoso.com"))
                .isInstanceOf(RuntimeException.class);
    }

    @Test
    void appendsTheConfiguredDomainToABareUsername() {
        handle("/v1.0/users/user-a@contoso.com/transitiveMemberOf", exchange ->
                respond(exchange, 200, "{\"value\":[{\"id\":\"group-guid-finance\"}]}"));

        // The caller authenticated against Alfresco as "user-a", which is not an Entra identity. This is the
        // common case of one tenant whose UPNs are <username>@<domain>.
        List<String> groups = resolverWith("@contoso.com").resolveGroups("user-a");

        assertThat(groups).containsExactly("GROUP_group-guid-finance");
        assertThat(requestLog).allSatisfy(uri -> assertThat(uri).contains("user-a@contoso.com"));
    }

    @Test
    void leavesAUsernameThatAlreadyLooksLikeAnIdentityAlone() {
        handle("/v1.0/users/bob@other.example/transitiveMemberOf", exchange ->
                respond(exchange, 200, "{\"value\":[]}"));

        List<String> groups = resolverWith("@contoso.com").resolveGroups("bob@other.example");

        // Appending a second domain would produce an identity nobody has.
        assertThat(groups).isEmpty();
        assertThat(requestLog).allSatisfy(uri -> assertThat(uri).doesNotContain("@contoso.com"));
    }

    @Test
    void leavesAnObjectIdAloneEvenThoughItContainsNoAtSign() {
        String objectId = "8f14e45f-ceea-467a-9c1e-1b2c3d4e5f60";
        handle("/v1.0/users/" + objectId + "/transitiveMemberOf", exchange ->
                respond(exchange, 200, "{\"value\":[{\"id\":\"group-guid-finance\"}]}"));

        List<String> groups = resolverWith("@contoso.com").resolveGroups(objectId);

        // Once a caller is identified by their oid, the username reaching here is a GUID. Testing only for an
        // "@" appended the domain to it, Graph answered 404 for <guid>@contoso.com, and that reported "no such
        // identity": the caller kept the source with default authorities and their group grants silently
        // stopped resolving. Graph accepts an object id directly, so it must be passed through untouched.
        assertThat(groups).containsExactly("GROUP_group-guid-finance");
        assertThat(requestLog).allSatisfy(uri -> assertThat(uri).doesNotContain("@contoso.com"));
    }

    @Test
    void treatsAKnownUserInNoGroupsAsAnEmptyListRatherThanNull() {
        handle("/v1.0/users/loner@contoso.com/transitiveMemberOf", exchange ->
                respond(exchange, 200, "{\"value\":[]}"));

        List<String> groups = resolverWith("").resolveGroups("loner@contoso.com");

        // Empty and null mean different things to the registry: this caller is known here and is in no
        // groups, which is not the same as being absent from the directory.
        assertThat(groups).isNotNull().isEmpty();
    }

    @Test
    void ignoresAGroupEntryWithNoId() {
        handle("/v1.0/users/bob@contoso.com/transitiveMemberOf", exchange -> respond(exchange, 200, """
                {"value":[{"displayName":"Finance"},{"id":"group-guid-finance"}]}"""));

        List<String> groups = resolverWith("").resolveGroups("bob@contoso.com");

        // A display name is not unique, so a principal built from one would match documents granted to a
        // different group of the same name. Dropping the entry under-resolves; using the name would leak.
        assertThat(groups).containsExactly("GROUP_group-guid-finance");
    }

    @Test
    void answersForTheSharePointSourceTypeByDefault() {
        assertThat(resolverWith("").sourceType()).isEqualTo("sharepoint");
    }

    @Test
    void resolvesNothingForABlankUsername() {
        EntraGroupResolver resolver = resolverWith("");

        assertThat(resolver.resolveGroups(null)).isNull();
        assertThat(resolver.resolveGroups("  ")).isNull();
        // No request should have been made for an identity that cannot exist.
        assertThat(requestLog).isEmpty();
    }

    @Test
    void namesExactlyOneConstructorForSpringToUse() {
        long autowired = java.util.Arrays.stream(EntraGroupResolver.class.getDeclaredConstructors())
                .filter(constructor -> constructor.isAnnotationPresent(
                        org.springframework.beans.factory.annotation.Autowired.class))
                .count();

        // Regression: this class has a second constructor for these tests, and Spring only infers one when
        // there is exactly one. Without the annotation the bean failed to build, which took the resolver
        // registry and the whole service down at startup. Nothing in this file could have caught that, since
        // every test here calls the test constructor directly.
        assertThat(EntraGroupResolver.class.getDeclaredConstructors()).hasSize(2);
        assertThat(autowired).isEqualTo(1);
    }

    private interface Handler {
        void handle(HttpExchange exchange) throws IOException;
    }
}
