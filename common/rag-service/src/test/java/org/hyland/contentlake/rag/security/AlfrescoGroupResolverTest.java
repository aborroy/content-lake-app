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
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AlfrescoGroupResolverTest {

    private static final String GROUPS_PATH =
            "/alfresco/api/-default-/public/alfresco/versions/1/people/alice/groups";

    private HttpServer server;
    private final List<String> requestedUris = new ArrayList<>();

    @AfterEach
    void tearDown() {
        if (server != null) {
            server.stop(0);
        }
    }

    @Test
    void declaresTheAlfrescoSourceType() {
        assertThat(new AlfrescoGroupResolver("http://localhost:1", "admin", "admin").sourceType())
                .isEqualTo("alfresco");
    }

    @Test
    void readsTheGroupIdsOfOnePage() throws Exception {
        start(exchange -> respond(exchange, 200, page(false, "GROUP_A", "GROUP_B")));

        assertThat(resolver().resolveGroups("alice")).containsExactly("GROUP_A", "GROUP_B");
    }

    @Test
    void pagesUntilTheRepositoryReportsNoMoreItems() throws Exception {
        // The un-paged single call this replaced asked for maxItems=1000 and dropped everything past it
        // without saying so, which is a document the caller can no longer retrieve.
        start(exchange -> {
            String query = exchange.getRequestURI().getQuery();
            if (query.contains("skipCount=0")) {
                respond(exchange, 200, page(true, "GROUP_A"));
            } else if (query.contains("skipCount=100")) {
                respond(exchange, 200, page(true, "GROUP_B"));
            } else {
                respond(exchange, 200, page(false, "GROUP_C"));
            }
        });

        assertThat(resolver().resolveGroups("alice"))
                .containsExactly("GROUP_A", "GROUP_B", "GROUP_C");
        assertThat(requestedUris).hasSize(3);
    }

    @Test
    void asksForOneHundredPerPage() throws Exception {
        start(exchange -> respond(exchange, 200, page(false, "GROUP_A")));

        resolver().resolveGroups("alice");

        assertThat(requestedUris.getFirst()).contains("skipCount=0").contains("maxItems=100");
    }

    @Test
    void stopsAtThePageBoundRatherThanPagingForever() throws Exception {
        // hasMoreItems that never turns false is a repository fault, not a reason to loop.
        start(exchange -> respond(exchange, 200, page(true, "GROUP_" + requestedUris.size())));

        assertThat(resolver().resolveGroups("alice")).hasSize(100);
        assertThat(requestedUris).hasSize(100);
    }

    @Test
    void encodesAUsernameThatCarriesUrlSyntax() throws Exception {
        // Concatenating this into the path reaches a different URL, or none at all.
        start(exchange -> respond(exchange, 200, page(false, "GROUP_A")));

        assertThat(resolver().resolveGroups("do/main u&r")).containsExactly("GROUP_A");
        assertThat(requestedUris.getFirst())
                .contains("/people/do%2Fmain%20u%26r/groups")
                .doesNotContain("/people/do/main");
    }

    @Test
    void aFirstPageNotFoundMeansTheRepositoryHoldsNoSuchPerson() throws Exception {
        // Distinct from an empty group list, and not a failure: the caller keeps the source.
        start(exchange -> respond(exchange, 404,
                "{\"error\":{\"statusCode\":404,\"briefSummary\":\"alice was not found\"}}"));

        assertThat(resolver().resolveGroups("alice")).isNull();
    }

    @Test
    void aLaterPageNotFoundKeepsWhatWasAlreadyRead() throws Exception {
        start(exchange -> {
            if (exchange.getRequestURI().getQuery().contains("skipCount=0")) {
                respond(exchange, 200, page(true, "GROUP_A"));
            } else {
                respond(exchange, 404, "{}");
            }
        });

        assertThat(resolver().resolveGroups("alice")).containsExactly("GROUP_A");
    }

    @Test
    void anEmptyListIsAKnownAnswer() throws Exception {
        start(exchange -> respond(exchange, 200, page(false)));

        assertThat(resolver().resolveGroups("alice")).isEmpty();
    }

    @Test
    void aServerErrorPropagatesSoTheFailurePolicyApplies() throws Exception {
        // The registry turns this into fail-closed or degrade; swallowing it here would hide the outage.
        start(exchange -> respond(exchange, 500, "{}"));

        assertThatThrownBy(() -> resolver().resolveGroups("alice"))
                .isInstanceOf(RuntimeException.class);
    }

    @Test
    void authenticatesWithTheServiceAccount() throws Exception {
        // A caller is not necessarily allowed to read their own group list, and the answer is
        // authorization input rather than something they asked for.
        List<String> authorizations = new ArrayList<>();
        start(exchange -> {
            authorizations.add(exchange.getRequestHeaders().getFirst("Authorization"));
            respond(exchange, 200, page(false, "GROUP_A"));
        });

        resolver().resolveGroups("alice");

        assertThat(authorizations).containsExactly("Basic " + Base64.getEncoder()
                .encodeToString("svc:secret".getBytes(StandardCharsets.UTF_8)));
    }

    @Test
    void toleratesABaseUrlWithATrailingSlash() throws Exception {
        start(exchange -> respond(exchange, 200, page(false, "GROUP_A")));

        AlfrescoGroupResolver resolver =
                new AlfrescoGroupResolver(baseUrl() + "/", "svc", "secret");

        assertThat(resolver.resolveGroups("alice")).containsExactly("GROUP_A");
        assertThat(requestedUris.getFirst()).startsWith(GROUPS_PATH).doesNotContain("//alfresco");
    }

    @Test
    void ignoresAnEntryWithNoId() throws Exception {
        start(exchange -> respond(exchange, 200, """
                {"list":{"pagination":{"hasMoreItems":false},"entries":[
                  {"entry":{"id":"GROUP_A"}},
                  {"entry":{"displayName":"nameless"}},
                  {"nothing":true}
                ]}}"""));

        assertThat(resolver().resolveGroups("alice")).containsExactly("GROUP_A");
    }

    private AlfrescoGroupResolver resolver() {
        return new AlfrescoGroupResolver(baseUrl(), "svc", "secret");
    }

    private static String page(boolean hasMoreItems, String... groupIds) {
        String entries = IntStream.range(0, groupIds.length)
                .mapToObj(index -> "{\"entry\":{\"id\":\"" + groupIds[index] + "\"}}")
                .reduce((left, right) -> left + "," + right)
                .orElse("");
        return "{\"list\":{\"pagination\":{\"hasMoreItems\":" + hasMoreItems + "},"
                + "\"entries\":[" + entries + "]}}";
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
