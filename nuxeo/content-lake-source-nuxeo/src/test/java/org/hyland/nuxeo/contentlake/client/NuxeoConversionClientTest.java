package org.hyland.contentlake.client;

import com.sun.net.httpserver.HttpServer;
import org.hyland.nuxeo.contentlake.auth.BasicNuxeoAuthentication;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.web.client.HttpClientErrorException;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class NuxeoConversionClientTest {

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
    void convertToText_callsSynchronousConversionAdapterWithTypeQueryParam() throws IOException {
        RequestCapture capture = new RequestCapture();
        server.createContext("/nuxeo/api/v1/id/doc-123/@blob/file:content/@convert", exchange -> {
            capture.method = exchange.getRequestMethod();
            capture.requestUri = exchange.getRequestURI();
            capture.authorization = exchange.getRequestHeaders().getFirst("Authorization");
            writeResponse(exchange, 200, "Converted by Nuxeo".getBytes(StandardCharsets.UTF_8), "text/plain");
        });
        server.start();

        NuxeoConversionClient client = client("file:content");

        String text = client.convertToText("doc-123", "application/pdf");

        assertThat(text).isEqualTo("Converted by Nuxeo");
        assertThat(capture.method).isEqualTo("GET");
        assertThat(capture.authorization).startsWith("Basic ");
        assertThat(capture.requestUri.getQuery()).isEqualTo("type=text/plain");
    }

    @Test
    void convertSync_usesConfiguredBlobXpath() throws IOException {
        RequestCapture capture = new RequestCapture();
        server.createContext("/nuxeo/api/v1/id/doc-123/@blob/files:files/0/file/@convert", exchange -> {
            capture.requestUri = exchange.getRequestURI();
            writeResponse(exchange, 200, "nested-blob".getBytes(StandardCharsets.UTF_8), "text/plain");
        });
        server.start();

        NuxeoConversionClient client = client("files:files/0/file");

        byte[] result = client.convertSync("doc-123", "text/plain");

        assertThat(result).isEqualTo("nested-blob".getBytes(StandardCharsets.UTF_8));
        assertThat(capture.requestUri.getQuery()).isEqualTo("type=text/plain");
    }

    @Test
    void convertToText_throwsWhenConversionFails() throws IOException {
        server.createContext("/nuxeo/api/v1/id/doc-404/@blob/file:content/@convert", exchange ->
                writeResponse(exchange, 404, "missing".getBytes(StandardCharsets.UTF_8), "text/plain"));
        server.start();

        NuxeoConversionClient client = client("file:content");

        assertThatThrownBy(() -> client.convertToText("doc-404", "application/pdf"))
                .isInstanceOf(HttpClientErrorException.NotFound.class);
    }

    @Test
    void convertToText_wrapsUnsupportedConverterResponses() throws IOException {
        server.createContext("/nuxeo/api/v1/id/doc-unsupported/@blob/file:content/@convert", exchange ->
                writeResponse(exchange, 400,
                        "No converter for application/pdf to text/plain".getBytes(StandardCharsets.UTF_8),
                        "text/plain"));
        server.start();

        NuxeoConversionClient client = client("file:content");

        assertThatThrownBy(() -> client.convertToText("doc-unsupported", "application/pdf"))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("application/pdf -> text/plain");
    }

    private NuxeoConversionClient client(String blobXpath) {
        return new NuxeoConversionClient(
                "http://localhost:" + server.getAddress().getPort() + "/nuxeo",
                blobXpath,
                5000,
                new BasicNuxeoAuthentication("Administrator", "Administrator")
        );
    }

    private static void writeResponse(com.sun.net.httpserver.HttpExchange exchange,
                                      int status,
                                      byte[] body,
                                      String contentType) throws IOException {
        if (contentType != null) {
            exchange.getResponseHeaders().set("Content-Type", contentType);
        }
        exchange.sendResponseHeaders(status, body.length);
        try (OutputStream outputStream = exchange.getResponseBody()) {
            outputStream.write(body);
        } finally {
            exchange.close();
        }
    }

    private static final class RequestCapture {
        private String method;
        private URI requestUri;
        private String authorization;
    }
}
