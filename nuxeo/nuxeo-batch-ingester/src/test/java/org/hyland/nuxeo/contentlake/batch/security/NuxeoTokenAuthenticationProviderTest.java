package org.hyland.nuxeo.contentlake.batch.security;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.hyland.nuxeo.contentlake.config.NuxeoProperties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

class NuxeoTokenAuthenticationProviderTest {

    private HttpServer server;

    @AfterEach
    void tearDown() {
        if (server != null) {
            server.stop(0);
        }
    }

    @Test
    void authenticatesNuxeoTokenAndResolvesUsername() throws Exception {
        String token = "nuxeo-token-demo";
        server = startServer(exchange -> {
            if (!"GET".equals(exchange.getRequestMethod())
                    || !"/nuxeo/api/v1/me".equals(exchange.getRequestURI().getPath())) {
                sendResponse(exchange, 404, "");
                return;
            }

            if (!token.equals(exchange.getRequestHeaders().getFirst("X-Authentication-Token"))) {
                sendResponse(exchange, 401, "");
                return;
            }

            sendResponse(exchange, 200, "{\"id\":\"nuxeo-user\"}");
        });

        NuxeoProperties props = new NuxeoProperties();
        props.setBaseUrl(baseUrl() + "/nuxeo");
        NuxeoTokenAuthenticationProvider provider = new NuxeoTokenAuthenticationProvider(props);

        var authentication = provider.authenticate(new UsernamePasswordAuthenticationToken(
                NuxeoTokenAuthenticationProvider.NUXEO_TOKEN_PRINCIPAL_PREFIX + token,
                ""
        ));

        assertThat(authentication).isNotNull();
        assertThat(authentication.getName()).isEqualTo("nuxeo-user");
    }

    private HttpServer startServer(ExchangeHandler handler) throws IOException {
        HttpServer httpServer = HttpServer.create(new InetSocketAddress(0), 0);
        httpServer.createContext("/", exchange -> {
            try {
                handler.handle(exchange);
            } finally {
                exchange.close();
            }
        });
        httpServer.start();
        return httpServer;
    }

    private String baseUrl() {
        return "http://127.0.0.1:" + server.getAddress().getPort();
    }

    private static void sendResponse(HttpExchange exchange, int statusCode, String body) throws IOException {
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
