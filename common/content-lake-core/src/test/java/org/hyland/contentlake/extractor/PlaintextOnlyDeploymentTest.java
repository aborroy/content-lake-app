package org.hyland.contentlake.extractor;

import com.sun.net.httpserver.HttpServer;
import org.hyland.contentlake.spi.ExtractedText;
import org.hyland.contentlake.spi.TextExtractor;
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

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Plain text stays a first-class option, with no transform-extras engine required.
 *
 * <p>`extraction.format` defaults to `plaintext`, and these tests pin what that guarantees: markdown is
 * never requested, an engine that happens to be able to produce it is still asked for `text/plain`, and
 * a deployment with no engine at all works on in-process Tika alone. Anyone who does not want markdown,
 * or does not want to run another container, keeps exactly the behaviour they had.</p>
 */
class PlaintextOnlyDeploymentTest {

    private static final String PDF = "application/pdf";

    /** An engine advertising both targets, i.e. the tempting case where markdown is available. */
    private static final String CONFIG_BOTH = """
            {"transformers":[{"transformerName":"liteparse","supportedSourceAndTargetList":[
              {"sourceMediaType":"application/pdf","targetMediaType":"text/markdown","priority":50},
              {"sourceMediaType":"application/pdf","targetMediaType":"text/plain","priority":45}
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

    /** The documented default: parse() of an unset, blank or unknown value is PLAINTEXT. */
    @Test
    void extractionFormatDefaultsToPlaintext() {
        assertThat(ExtractionFormat.parse(null)).isEqualTo(ExtractionFormat.PLAINTEXT);
        assertThat(ExtractionFormat.parse("")).isEqualTo(ExtractionFormat.PLAINTEXT);
        assertThat(ExtractionFormat.parse("   ")).isEqualTo(ExtractionFormat.PLAINTEXT);
        assertThat(ExtractionFormat.parse("nonsense")).isEqualTo(ExtractionFormat.PLAINTEXT);
        assertThat(ExtractionFormat.parse("plaintext")).isEqualTo(ExtractionFormat.PLAINTEXT);
        assertThat(ExtractionFormat.PLAINTEXT.allowsMarkdown()).isFalse();
        assertThat(ExtractionFormat.parse("AUTO")).isEqualTo(ExtractionFormat.AUTO);
        assertThat(ExtractionFormat.parse(" Markdown ")).isEqualTo(ExtractionFormat.MARKDOWN);
    }

    /**
     * The decisive one: even pointed at a markdown-capable engine, the default never asks for markdown
     * and never even queries whether markdown is available.
     */
    @Test
    void plaintextNeverRequestsMarkdownEvenFromAMarkdownCapableEngine() throws IOException {
        StringBuilder body = new StringBuilder();
        server.createContext("/transform/config", exchange ->
                respond(exchange, 200, CONFIG_BOTH.getBytes(StandardCharsets.UTF_8), "application/json"));
        server.createContext("/transform", exchange -> {
            body.append(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
            respond(exchange, 200, "flattened text".getBytes(StandardCharsets.UTF_8), "text/plain");
        });
        server.start();

        TextExtractor extractor = new TransformEngineTextExtractor(
                baseUrl(), 5_000L, ExtractionFormat.PLAINTEXT, Duration.ofMinutes(5));

        assertThat(extractor.preferredFormat(PDF)).isEqualTo(TextFormat.PLAIN);

        ExtractedText extracted = extractor.extract(pdf(), PDF);
        assertThat(extracted).isNotNull();
        assertThat(extracted.format()).isEqualTo(TextFormat.PLAIN);
        assertThat(extracted.text()).isEqualTo("flattened text");
        assertThat(body.toString()).contains("text/plain").doesNotContain("text/markdown");
    }

    /**
     * A deployment that installs nothing: no transform-extras, no engine URL configured. This is the
     * default shape of the Nuxeo and filesystem chains, and it must extract text perfectly well.
     */
    @Test
    void aTikaOnlyChainNeedsNoEngineAtAll() {
        TextExtractor chain = new ChainingTextExtractor(new TikaTextExtractor());

        assertThat(chain.supports("text/plain")).isTrue();
        assertThat(chain.preferredFormat("text/plain")).isEqualTo(TextFormat.PLAIN);

        Resource html = new ByteArrayResource(
                "<html><body><h1>Heading</h1><p>Body text.</p></body></html>"
                        .getBytes(StandardCharsets.UTF_8));

        ExtractedText extracted = chain.extract(html, "text/html");

        assertThat(extracted).isNotNull();
        assertThat(extracted.format()).isEqualTo(TextFormat.PLAIN);
        assertThat(extracted.text()).contains("Heading").contains("Body text.");
        // Plain text, so nothing downstream has any markdown to strip.
        assertThat(extracted.text()).doesNotContain("<h1>").doesNotContain("|");
    }

    /**
     * Asking for markdown while the only engine is plaintext-only (the official Transform Core AIO)
     * degrades silently to plaintext rather than refusing to extract.
     */
    @Test
    void requestingMarkdownAgainstAPlaintextOnlyEngineStillExtracts() throws IOException {
        String plainOnly = """
                {"transformers":[{"transformerName":"tika","supportedSourceAndTargetList":[
                  {"sourceMediaType":"application/pdf","targetMediaType":"text/plain","priority":50}
                ]}]}
                """;
        server.createContext("/transform/config", exchange ->
                respond(exchange, 200, plainOnly.getBytes(StandardCharsets.UTF_8), "application/json"));
        server.createContext("/transform", exchange ->
                respond(exchange, 200, "flattened".getBytes(StandardCharsets.UTF_8), "text/plain"));
        server.start();

        TextExtractor extractor = new TransformEngineTextExtractor(
                baseUrl(), 5_000L, ExtractionFormat.MARKDOWN, Duration.ofMinutes(5));

        ExtractedText extracted = extractor.extract(pdf(), PDF);

        assertThat(extracted).isNotNull();
        assertThat(extracted.format()).isEqualTo(TextFormat.PLAIN);
        assertThat(extracted.text()).isEqualTo("flattened");
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

    private static void respond(com.sun.net.httpserver.HttpExchange exchange, int status, byte[] body,
                                String contentType) throws IOException {
        exchange.getResponseHeaders().add("Content-Type", contentType);
        exchange.sendResponseHeaders(status, body.length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(body);
        }
        exchange.close();
    }
}
