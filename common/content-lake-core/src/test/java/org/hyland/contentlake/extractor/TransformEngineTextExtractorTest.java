package org.hyland.contentlake.extractor;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.hyland.contentlake.spi.ExtractedText;
import org.hyland.contentlake.spi.TextFormat;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.core.io.Resource;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Exercised against a real {@link HttpServer} on an ephemeral port rather than a mocked
 * {@code RestClient}, so the multipart body, the query string and the status handling are all the
 * real thing. This is the idiom {@code NuxeoConversionClientTest} established.
 */
class TransformEngineTextExtractorTest {

    private static final String PDF = "application/pdf";

    /** What liteparse advertises: markdown at a higher priority than plaintext, for PDF and Office. */
    private static final String CONFIG_WITH_MARKDOWN = """
            {"transformers":[{"transformerName":"liteparse","supportedSourceAndTargetList":[
              {"sourceMediaType":"application/pdf","targetMediaType":"text/markdown","priority":50},
              {"sourceMediaType":"application/pdf","targetMediaType":"text/plain","priority":45}
            ]}]}
            """;

    /** What the official Transform Core AIO advertises: plaintext only. */
    private static final String CONFIG_PLAIN_ONLY = """
            {"transformers":[{"transformerName":"tika","supportedSourceAndTargetList":[
              {"sourceMediaType":"application/pdf","targetMediaType":"text/plain","priority":50}
            ]}]}
            """;

    private HttpServer server;

    @BeforeEach
    void setUp() throws IOException {
        server = HttpServer.create(new InetSocketAddress(0), 0);
    }

    @AfterEach
    void tearDown() {
        server.stop(0);
    }

    // ──────────────────────────────────────────────────────────────────────
    // Target selection
    // ──────────────────────────────────────────────────────────────────────

    @Test
    void requestsMarkdownWhenTheEngineAdvertisesItAndTheFormatAllowsIt() throws IOException {
        Capture capture = new Capture();
        serveConfig(CONFIG_WITH_MARKDOWN);
        serveTransform(capture, 200, "| Tier | Coverage |\n| --- | --- |\n| Sovereign | 24x7 |");
        server.start();

        ExtractedText extracted = extractor(ExtractionFormat.AUTO).extract(pdf(), PDF);

        assertThat(extracted).isNotNull();
        assertThat(extracted.format()).isEqualTo(TextFormat.MARKDOWN);
        assertThat(extracted.text()).contains("| Sovereign | 24x7 |");
        assertThat(capture.body).contains("name=\"targetMimetype\"").contains("text/markdown");
        assertThat(capture.body).contains("name=\"targetExtension\"").contains("md");
        assertThat(capture.body).contains("name=\"sourceMimetype\"").contains(PDF);
        assertThat(capture.query).contains("timeout=");
        assertThat(capture.method).isEqualTo("POST");
    }

    /**
     * The default. An existing deployment must keep asking for exactly what it asked for before, even
     * when the engine in front of it happens to be able to produce markdown.
     */
    @Test
    void defaultFormatAsksForPlaintextEvenWhenMarkdownIsAvailable() throws IOException {
        Capture capture = new Capture();
        serveConfig(CONFIG_WITH_MARKDOWN);
        serveTransform(capture, 200, "Sovereign 24x7");
        server.start();

        ExtractedText extracted = extractor(ExtractionFormat.PLAINTEXT).extract(pdf(), PDF);

        assertThat(extracted).isNotNull();
        assertThat(extracted.format()).isEqualTo(TextFormat.PLAIN);
        assertThat(capture.body).contains("text/plain").doesNotContain("text/markdown");
    }

    @Test
    void fallsBackToPlaintextWhenTheEngineAdvertisesNoMarkdown() throws IOException {
        Capture capture = new Capture();
        serveConfig(CONFIG_PLAIN_ONLY);
        serveTransform(capture, 200, "Sovereign 24x7");
        server.start();

        ExtractedText extracted = extractor(ExtractionFormat.MARKDOWN).extract(pdf(), PDF);

        assertThat(extracted).isNotNull();
        assertThat(extracted.format()).isEqualTo(TextFormat.PLAIN);
        assertThat(capture.body).contains("text/plain").doesNotContain("text/markdown");
    }

    @Test
    void preferredFormatAnswersWithoutRunningATransform() throws IOException {
        Capture capture = new Capture();
        serveConfig(CONFIG_WITH_MARKDOWN);
        serveTransform(capture, 200, "unused");
        server.start();

        TransformEngineTextExtractor extractor = extractor(ExtractionFormat.AUTO);

        assertThat(extractor.preferredFormat(PDF)).isEqualTo(TextFormat.MARKDOWN);
        assertThat(extractor.supports(PDF)).isTrue();
        assertThat(capture.body).isNull();
    }

    @Test
    void unsupportedSourceTypeIsNotSupported() throws IOException {
        serveConfig(CONFIG_PLAIN_ONLY);
        server.start();

        assertThat(extractor(ExtractionFormat.AUTO).supports("video/mp4")).isFalse();
        assertThat(extractor(ExtractionFormat.AUTO).supports(null)).isFalse();
        assertThat(extractor(ExtractionFormat.AUTO).supports("  ")).isFalse();
    }

    @Test
    void engineConfigIsCachedAcrossCalls() throws IOException {
        AtomicInteger configCalls = new AtomicInteger();
        server.createContext("/transform/config", exchange -> {
            configCalls.incrementAndGet();
            respond(exchange, 200, CONFIG_WITH_MARKDOWN.getBytes(StandardCharsets.UTF_8), "application/json");
        });
        server.start();

        TransformEngineTextExtractor extractor = extractor(ExtractionFormat.AUTO);
        extractor.supports(PDF);
        extractor.supports(PDF);
        extractor.preferredFormat(PDF);

        assertThat(configCalls.get()).isEqualTo(1);
    }

    // ──────────────────────────────────────────────────────────────────────
    // Degradation: none of these may throw
    // ──────────────────────────────────────────────────────────────────────

    @Test
    void unsupportedTransformResponseYieldsNullRatherThanThrowing() throws IOException {
        serveConfig(CONFIG_PLAIN_ONLY);
        server.createContext("/transform", exchange ->
                respond(exchange, 400, "No transforms for: application/pdf -> text/plain"
                        .getBytes(StandardCharsets.UTF_8), "text/plain"));
        server.start();

        assertThat(extractor(ExtractionFormat.AUTO).extract(pdf(), PDF)).isNull();
    }

    @Test
    void serverErrorYieldsNullRatherThanThrowing() throws IOException {
        serveConfig(CONFIG_PLAIN_ONLY);
        server.createContext("/transform", exchange ->
                respond(exchange, 500, "boom".getBytes(StandardCharsets.UTF_8), "text/plain"));
        server.start();

        assertThat(extractor(ExtractionFormat.AUTO).extract(pdf(), PDF)).isNull();
    }

    @Test
    void emptyResponseBodyYieldsNull() throws IOException {
        serveConfig(CONFIG_PLAIN_ONLY);
        server.createContext("/transform", exchange ->
                respond(exchange, 200, new byte[0], "text/plain"));
        server.start();

        assertThat(extractor(ExtractionFormat.AUTO).extract(pdf(), PDF)).isNull();
    }

    @Test
    void blankResponseBodyYieldsNull() throws IOException {
        serveConfig(CONFIG_PLAIN_ONLY);
        server.createContext("/transform", exchange ->
                respond(exchange, 200, "   \n  ".getBytes(StandardCharsets.UTF_8), "text/plain"));
        server.start();

        assertThat(extractor(ExtractionFormat.AUTO).extract(pdf(), PDF)).isNull();
    }

    /** A read timeout is the realistic shape of a cold start on a model-loading engine. */
    @Test
    void readTimeoutYieldsNullRatherThanThrowing() throws IOException {
        serveConfig(CONFIG_PLAIN_ONLY);
        server.createContext("/transform", exchange -> {
            try {
                Thread.sleep(2_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            respond(exchange, 200, "too late".getBytes(StandardCharsets.UTF_8), "text/plain");
        });
        server.start();

        TransformEngineTextExtractor extractor = new TransformEngineTextExtractor(
                baseUrl(), 250L, ExtractionFormat.AUTO, Duration.ofMinutes(5));

        assertThat(extractor.extract(pdf(), PDF)).isNull();
    }

    /**
     * An absent engine must not make the extractor claim it can produce markdown: every document would
     * then take a path that 400s. Plaintext still fails open, so a slow-starting engine gets a real
     * attempt rather than being treated as supporting nothing.
     */
    @Test
    void unreachableEngineFailsOpenForPlaintextOnly() {
        TransformEngineTextExtractor extractor = new TransformEngineTextExtractor(
                "http://127.0.0.1:1", 250L, ExtractionFormat.AUTO, Duration.ofMinutes(5));

        assertThat(extractor.isTransformSupported(PDF, "text/plain")).isTrue();
        assertThat(extractor.isTransformSupported(PDF, "text/markdown")).isFalse();
        assertThat(extractor.preferredFormat(PDF)).isEqualTo(TextFormat.PLAIN);
        assertThat(extractor.extract(pdf(), PDF)).isNull();
    }

    @Test
    void extractTextDelegatesToExtractAndUnwrapsTheText() throws IOException {
        serveConfig(CONFIG_PLAIN_ONLY);
        serveTransform(new Capture(), 200, "flattened text");
        server.start();

        assertThat(extractor(ExtractionFormat.PLAINTEXT).extractText(pdf(), PDF))
                .isEqualTo("flattened text");
    }

    // ──────────────────────────────────────────────────────────────────────

    private TransformEngineTextExtractor extractor(ExtractionFormat format) {
        return new TransformEngineTextExtractor(baseUrl(), 5_000L, format, Duration.ofMinutes(5));
    }

    private String baseUrl() {
        return "http://localhost:" + server.getAddress().getPort();
    }

    private static Resource pdf() {
        return new ByteArrayResource("%PDF-1.4 fake".getBytes(StandardCharsets.UTF_8)) {
            @Override
            public String getFilename() {
                return "content.pdf";
            }
        };
    }

    private void serveConfig(String json) {
        server.createContext("/transform/config", exchange ->
                respond(exchange, 200, json.getBytes(StandardCharsets.UTF_8), "application/json"));
    }

    private void serveTransform(Capture capture, int status, String body) {
        server.createContext("/transform", exchange -> {
            capture.method = exchange.getRequestMethod();
            capture.query = exchange.getRequestURI().getQuery();
            capture.contentType = exchange.getRequestHeaders().getFirst("Content-Type");
            capture.body = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
            respond(exchange, status, body.getBytes(StandardCharsets.UTF_8), "text/plain");
        });
    }

    private static void respond(HttpExchange exchange, int status, byte[] body, String contentType)
            throws IOException {
        exchange.getResponseHeaders().add("Content-Type", contentType);
        exchange.sendResponseHeaders(status, body.length == 0 ? -1 : body.length);
        if (body.length > 0) {
            try (OutputStream out = exchange.getResponseBody()) {
                out.write(body);
            }
        }
        exchange.close();
    }

    private static final class Capture {
        private String method;
        private String query;
        private String contentType;
        private String body;
    }
}
