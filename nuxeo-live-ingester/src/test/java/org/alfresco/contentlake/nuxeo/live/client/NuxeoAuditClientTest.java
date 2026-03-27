package org.alfresco.contentlake.nuxeo.live.client;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.alfresco.contentlake.auth.BasicNuxeoAuthentication;
import org.alfresco.contentlake.nuxeo.live.model.AuditCursor;
import org.alfresco.contentlake.nuxeo.live.model.NuxeoAuditPage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.OffsetDateTime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class NuxeoAuditClientTest {

    private HttpServer server;

    @BeforeEach
    void setUp() throws IOException {
        server = HttpServer.create(new InetSocketAddress(0), 0);
    }

    @AfterEach
    void tearDown() {
        server.stop(0);
    }

    @Test
    void fetchPage_queriesAutomationEndpointWithStrictWindowedCursorPredicate() throws IOException {
        RequestCapture capture = new RequestCapture();
        server.createContext("/nuxeo/api/v1" + NuxeoAuditClient.AUDIT_QUERY_OPERATION, exchange -> {
            capture.method = exchange.getRequestMethod();
            capture.contentType = exchange.getRequestHeaders().getFirst("Content-Type");
            capture.authorization = exchange.getRequestHeaders().getFirst("Authorization");
            capture.body = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
            writeJson(exchange, """
                    {
                      "entity-type": "logEntries",
                      "entries": [
                        {
                          "entity-type": "logEntry",
                          "id": 48,
                          "eventId": "documentRemoved",
                          "docUUID": "doc-48",
                          "docPath": "/default-domain/workspaces/doc-48.trashed",
                          "docType": "Note",
                          "repositoryId": "default",
                          "eventDate": "2026-03-26T16:48:45.902Z",
                          "logDate": "2026-03-26T16:48:45.939Z"
                        }
                      ]
                    }
                    """);
        });
        server.start();

        NuxeoAuditClient client = new NuxeoAuditClient(
                "http://localhost:" + server.getAddress().getPort() + "/nuxeo",
                new BasicNuxeoAuthentication("Administrator", "Administrator")
        );

        AuditCursor cursor = new AuditCursor(OffsetDateTime.parse("2026-03-26T16:48:41.235Z"), 46);
        OffsetDateTime windowEnd = OffsetDateTime.parse("2026-03-26T16:49:00.000Z");
        NuxeoAuditPage page = client.fetchPage(cursor, windowEnd, 5);

        assertThat(page.getEntries()).hasSize(1);
        assertThat(page.getEntries().getFirst().id()).isEqualTo(48);
        assertThat(page.getEntries().getFirst().eventId()).isEqualTo("documentRemoved");
        assertThat(capture.method).isEqualTo("POST");
        assertThat(capture.contentType).startsWith("application/json");
        assertThat(capture.authorization).startsWith("Basic ");
        assertThat(capture.body).contains("documentSecurityUpdated");
        assertThat(capture.body).contains("logDate > TIMESTAMP \\\"2026-03-26 16:48:41.235\\\"");
        assertThat(capture.body).contains("logDate = TIMESTAMP \\\"2026-03-26 16:48:41.235\\\" AND id > 46");
        assertThat(capture.body).contains("logDate <= TIMESTAMP \\\"2026-03-26 16:49:00.000\\\"");
        assertThat(capture.body).contains("\"pageSize\":5");
    }

    @Test
    void fetchPage_acceptsBaseUrlThatAlreadyEndsWithApiV1() throws IOException {
        RequestCapture capture = new RequestCapture();
        server.createContext("/nuxeo/api/v1" + NuxeoAuditClient.AUDIT_QUERY_OPERATION, exchange -> {
            capture.path = exchange.getRequestURI().getPath();
            writeJson(exchange, """
                    {
                      "entity-type": "logEntries",
                      "entries": []
                    }
                    """);
        });
        server.start();

        NuxeoAuditClient client = new NuxeoAuditClient(
                "http://localhost:" + server.getAddress().getPort() + "/nuxeo/api/v1",
                new BasicNuxeoAuthentication("Administrator", "Administrator")
        );

        client.fetchPage(new AuditCursor(OffsetDateTime.parse("2026-03-26T16:48:41.235Z"), 46),
                OffsetDateTime.parse("2026-03-26T16:49:00.000Z"),
                5);

        assertThat(capture.path).isEqualTo("/nuxeo/api/v1" + NuxeoAuditClient.AUDIT_QUERY_OPERATION);
    }

    @Test
    void fetchPage_reportsMissingAutomationOperationExplicitly() throws IOException {
        server.createContext("/nuxeo/api/v1" + NuxeoAuditClient.AUDIT_QUERY_OPERATION, exchange -> {
            byte[] payload = """
                    {"entity-type":"exception","status":404,"message":"Operation not found"}
                    """.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(404, payload.length);
            try (OutputStream stream = exchange.getResponseBody()) {
                stream.write(payload);
            }
        });
        server.start();

        NuxeoAuditClient client = new NuxeoAuditClient(
                "http://localhost:" + server.getAddress().getPort() + "/nuxeo",
                new BasicNuxeoAuthentication("Administrator", "Administrator")
        );

        assertThatThrownBy(() -> client.fetchPage(
                new AuditCursor(OffsetDateTime.parse("2026-03-26T16:48:41.235Z"), 46),
                OffsetDateTime.parse("2026-03-26T16:49:00.000Z"),
                5
        ))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(NuxeoAuditClient.AUDIT_QUERY_OPERATION)
                .hasMessageContaining("GET /api/v1/audit")
                .hasMessageContaining("@audit");
    }

    private static void writeJson(HttpExchange exchange, String body) throws IOException {
        byte[] payload = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(200, payload.length);
        try (OutputStream stream = exchange.getResponseBody()) {
            stream.write(payload);
        }
    }

    private static final class RequestCapture {
        private String method;
        private String contentType;
        private String authorization;
        private String body;
        private String path;
    }
}
