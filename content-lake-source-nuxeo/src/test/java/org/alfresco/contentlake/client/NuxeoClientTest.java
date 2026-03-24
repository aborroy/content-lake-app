package org.alfresco.contentlake.client;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.alfresco.contentlake.auth.BasicNuxeoAuthentication;
import org.alfresco.contentlake.spi.SourceNode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.Resource;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class NuxeoClientTest {

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
    void getNode_mapsDocumentAndUsesAclHeader() throws IOException {
        RequestCapture capture = new RequestCapture();
        server.createContext("/nuxeo/api/v1/id/doc-123", exchange -> {
            capture.authorization = exchange.getRequestHeaders().getFirst("Authorization");
            capture.enricher = exchange.getRequestHeaders().getFirst("enrichers-document");
            writeJson(exchange, """
                    {
                      "entity-type": "document",
                      "uid": "doc-123",
                      "path": "/default-domain/workspaces/finance/q1-report.pdf",
                      "type": "File",
                      "title": "Quarterly Report",
                      "state": "project",
                      "properties": {
                        "dc:modified": "2026-03-24T09:15:30Z",
                        "file:content": {
                          "name": "q1-report.pdf",
                          "mime-type": "application/pdf"
                        }
                      },
                      "contextParameters": {
                        "acls": [
                          {
                            "name": "local",
                            "aces": [
                              { "username": "Administrator", "permission": "Everything", "granted": true }
                            ]
                          }
                        ]
                      }
                    }
                    """);
        });
        server.start();

        NuxeoClient client = client("file:content");

        SourceNode node = client.getNode("doc-123");

        assertThat(node).isNotNull();
        assertThat(node.nodeId()).isEqualTo("doc-123");
        assertThat(node.name()).isEqualTo("Quarterly Report");
        assertThat(node.path()).isEqualTo("/default-domain/workspaces/finance");
        assertThat(node.sourceProperties())
                .containsEntry("nuxeo_path", "/default-domain/workspaces/finance/q1-report.pdf");
        assertThat(capture.authorization).startsWith("Basic ");
        assertThat(capture.enricher).isEqualTo("acls");
    }

    @Test
    void getChildren_honorsSkipAndMaxItemsAcrossPages() throws IOException {
        server.createContext("/nuxeo/api/v1/id/folder-1/@children", exchange -> {
            String query = exchange.getRequestURI().getQuery();
            if ("currentPageIndex=0&pageSize=2".equals(query)) {
                writeJson(exchange, """
                        {
                          "entries": [
                            { "uid": "doc-1", "path": "/default-domain/workspaces/ws/doc-1", "type": "File", "title": "Doc 1", "state": "project", "properties": { "dc:modified": "2026-03-24T10:00:00Z", "file:content": { "mime-type": "application/pdf" } } },
                            { "uid": "doc-2", "path": "/default-domain/workspaces/ws/doc-2", "type": "File", "title": "Doc 2", "state": "project", "properties": { "dc:modified": "2026-03-24T10:01:00Z", "file:content": { "mime-type": "application/pdf" } } }
                          ]
                        }
                        """);
                return;
            }
            if ("currentPageIndex=1&pageSize=2".equals(query)) {
                writeJson(exchange, """
                        {
                          "entries": [
                            { "uid": "doc-3", "path": "/default-domain/workspaces/ws/doc-3", "type": "File", "title": "Doc 3", "state": "project", "properties": { "dc:modified": "2026-03-24T10:02:00Z", "file:content": { "mime-type": "application/pdf" } } },
                            { "uid": "doc-4", "path": "/default-domain/workspaces/ws/doc-4", "type": "File", "title": "Doc 4", "state": "project", "properties": { "dc:modified": "2026-03-24T10:03:00Z", "file:content": { "mime-type": "application/pdf" } } }
                          ]
                        }
                        """);
                return;
            }
            exchange.sendResponseHeaders(404, -1);
            exchange.close();
        });
        server.start();

        NuxeoClient client = client("file:content");

        List<SourceNode> children = client.getChildren("folder-1", 1, 2);

        assertThat(children).extracting(SourceNode::nodeId).containsExactly("doc-2", "doc-3");
    }

    @Test
    void getContentAndDownloadContent_useConfiguredBlobXpath() throws IOException {
        List<URI> requestedUris = new ArrayList<>();
        byte[] content = "nuxeo-binary".getBytes(StandardCharsets.UTF_8);

        server.createContext("/nuxeo/api/v1/id/doc-123/@blob/files:files/0/file", exchange -> {
            requestedUris.add(exchange.getRequestURI());
            writeBytes(exchange, content, "application/octet-stream");
        });
        server.start();

        NuxeoClient client = client("files:files/0/file");

        assertThat(client.getContent("doc-123")).containsExactly(content);

        Resource resource = client.downloadContent("doc-123", "sample.bin");
        try {
            assertThat(Files.readAllBytes(resource.getFile().toPath())).containsExactly(content);
        } finally {
            Files.deleteIfExists(resource.getFile().toPath());
        }

        assertThat(requestedUris)
                .extracting(URI::getPath)
                .containsOnly("/nuxeo/api/v1/id/doc-123/@blob/files:files/0/file");
    }

    private NuxeoClient client(String blobXpath) {
        return new NuxeoClient(
                "http://localhost:" + server.getAddress().getPort() + "/nuxeo",
                "nuxeo-dev",
                blobXpath,
                new BasicNuxeoAuthentication("Administrator", "Administrator")
        );
    }

    private static void writeJson(HttpExchange exchange, String body) throws IOException {
        writeBytes(exchange, body.getBytes(StandardCharsets.UTF_8), "application/json");
    }

    private static void writeBytes(HttpExchange exchange, byte[] body, String contentType) throws IOException {
        exchange.getResponseHeaders().set("Content-Type", contentType);
        exchange.sendResponseHeaders(200, body.length);
        try (OutputStream outputStream = exchange.getResponseBody()) {
            outputStream.write(body);
        }
    }

    private static final class RequestCapture {
        private String authorization;
        private String enricher;
    }
}
