package org.hyland.contentlake.rag.security;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.test.util.ReflectionTestUtils;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

class MultiSourceAuthenticationProviderTest {

    private HttpServer server;

    @AfterEach
    void tearDown() {
        if (server != null) {
            server.stop(0);
        }
    }

    @Test
    void authenticatesAlfrescoTicketAndResolvesUsername() throws Exception {
        String ticket = "TICKET_demo-ticket";
        server = startServer(exchange -> {
            if (!"GET".equals(exchange.getRequestMethod())
                    || !"/alfresco/api/-default-/public/authentication/versions/1/tickets/-me-"
                    .equals(exchange.getRequestURI().getPath())) {
                sendResponse(exchange, 404, "");
                return;
            }

            String expectedHeader = "Basic " + Base64.getEncoder()
                    .encodeToString((ticket + ":").getBytes(StandardCharsets.UTF_8));
            if (!expectedHeader.equals(exchange.getRequestHeaders().getFirst("Authorization"))) {
                sendResponse(exchange, 401, "");
                return;
            }

            sendResponse(exchange, 200, "{\"entry\":{\"id\":\"rag-user\"}}");
        });

        MultiSourceAuthenticationProvider provider = new MultiSourceAuthenticationProvider();
        ReflectionTestUtils.setField(provider, "alfrescoUrl", baseUrl());
        ReflectionTestUtils.setField(provider, "nuxeoUrl", "");

        var authentication = provider.authenticate(new UsernamePasswordAuthenticationToken(ticket, ""));

        assertThat(authentication.getName()).isEqualTo("rag-user");
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

            sendResponse(exchange, 200, "{\"id\":\"rag-user\"}");
        });

        MultiSourceAuthenticationProvider provider = new MultiSourceAuthenticationProvider();
        ReflectionTestUtils.setField(provider, "alfrescoUrl", "");
        ReflectionTestUtils.setField(provider, "nuxeoUrl", baseUrl() + "/nuxeo");

        var authentication = provider.authenticate(new UsernamePasswordAuthenticationToken(
                MultiSourceAuthenticationProvider.NUXEO_TOKEN_PRINCIPAL_PREFIX + token,
                ""
        ));

        assertThat(authentication.getName()).isEqualTo("rag-user");
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
