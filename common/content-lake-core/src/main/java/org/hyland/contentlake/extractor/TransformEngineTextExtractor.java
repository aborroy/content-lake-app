package org.hyland.contentlake.extractor;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.hyland.contentlake.spi.ExtractedText;
import org.hyland.contentlake.spi.TextExtractor;
import org.hyland.contentlake.spi.TextFormat;
import org.springframework.core.io.Resource;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.http.client.MultipartBodyBuilder;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;

/**
 * Extracts text by speaking the {@code alfresco-transform-core} HTTP protocol to a transform engine.
 *
 * <p>Talks the protocol rather than going through a repository, so it serves every content source:
 * the Alfresco Transform Core AIO that the Alfresco stack already runs, and equally an engine placed
 * alongside the Nuxeo or filesystem connectors, neither of which has any structure-aware extraction
 * otherwise.</p>
 *
 * <p>Two endpoints, both standard for an engine inheriting {@code alfresco-transform-core}:</p>
 * <ul>
 *   <li>{@code POST /transform} -- multipart {@code file}, {@code sourceMimetype},
 *       {@code targetMimetype}, {@code targetExtension}; the response body is the transformed bytes</li>
 *   <li>{@code GET /transform/config} -- the engine's advertised source/target pairs</li>
 * </ul>
 *
 * <p>Target selection is <em>discovered</em>, not configured: the engine's own config says whether it
 * can turn a given MIME type into markdown, so there is no routing table to keep in step with which
 * engines are deployed. When {@link ExtractionFormat} allows markdown and the engine advertises it,
 * markdown is requested; otherwise plaintext is, exactly as before.</p>
 *
 * <p>Degrades, never fails. An unsupported transform, an unreachable engine and a malformed response
 * all return {@code null}, which is the {@link TextExtractor} contract for "no text" and lets a
 * {@link ChainingTextExtractor} fall through to the next extractor.</p>
 */
@Slf4j
public class TransformEngineTextExtractor implements TextExtractor {

    private static final String TARGET_PLAIN = "text/plain";
    private static final String TARGET_MARKDOWN = "text/markdown";
    private static final String EXTENSION_PLAIN = "txt";
    private static final String EXTENSION_MARKDOWN = "md";

    private static final long DEFAULT_TIMEOUT_MS = 60_000;
    private static final Duration DEFAULT_CONFIG_CACHE_TTL = Duration.ofMinutes(5);

    private final RestClient restClient;
    private final long timeoutMs;
    private final ExtractionFormat extractionFormat;
    private final Duration configCacheTtl;

    private volatile EngineConfig cachedConfig;
    private volatile Instant cachedConfigAt;

    public TransformEngineTextExtractor(String baseUrl) {
        this(baseUrl, DEFAULT_TIMEOUT_MS, ExtractionFormat.PLAINTEXT, DEFAULT_CONFIG_CACHE_TTL);
    }

    public TransformEngineTextExtractor(String baseUrl, long timeoutMs, ExtractionFormat extractionFormat) {
        this(baseUrl, timeoutMs, extractionFormat, DEFAULT_CONFIG_CACHE_TTL);
    }

    /**
     * @param baseUrl          engine base URL, with or without a trailing slash
     * @param timeoutMs        read timeout for a transform request
     * @param extractionFormat which representation to ask for
     * @param configCacheTtl   how long an engine's advertised capabilities are trusted
     */
    public TransformEngineTextExtractor(String baseUrl, long timeoutMs,
                                        ExtractionFormat extractionFormat, Duration configCacheTtl) {
        this.timeoutMs = timeoutMs;
        this.extractionFormat = extractionFormat == null ? ExtractionFormat.PLAINTEXT : extractionFormat;
        this.configCacheTtl = configCacheTtl == null ? DEFAULT_CONFIG_CACHE_TTL : configCacheTtl;

        // Without an explicit request factory the default total timeout is 10s, which is fine for a
        // small text file and aborts anything slower: a large PDF takes tens of seconds, and a
        // markdown engine loading a layout model takes longer still on its first call after start.
        JdkClientHttpRequestFactory requestFactory = new JdkClientHttpRequestFactory();
        requestFactory.setReadTimeout(Duration.ofMillis(timeoutMs));
        this.restClient = RestClient.builder()
                .baseUrl(baseUrl)
                .requestFactory(requestFactory)
                .build();

        log.info("TransformEngineTextExtractor initialized: baseUrl={}, timeout={}ms, format={}, "
                + "configCacheTtl={}", baseUrl, timeoutMs, this.extractionFormat, this.configCacheTtl);
    }

    // ──────────────────────────────────────────────────────────────────────
    // TextExtractor
    // ──────────────────────────────────────────────────────────────────────

    @Override
    public boolean supports(String mimeType) {
        return resolveTarget(mimeType) != null;
    }

    @Override
    public TextFormat preferredFormat(String mimeType) {
        Target target = resolveTarget(mimeType);
        return target == null ? TextFormat.PLAIN : target.format();
    }

    @Override
    public String extractText(Resource content, String mimeType) {
        ExtractedText extracted = extract(content, mimeType);
        return extracted == null ? null : extracted.text();
    }

    @Override
    public ExtractedText extract(Resource content, String mimeType) {
        Target target = resolveTarget(mimeType);
        if (target == null) {
            log.debug("No transform available for {}", mimeType);
            return null;
        }
        if (target.format() == TextFormat.PLAIN && extractionFormat.reportsFallback()) {
            log.info("Extraction format is markdown but the engine advertises no {} -> {} transform; "
                    + "falling back to plaintext", mimeType, TARGET_MARKDOWN);
        }

        try {
            byte[] result = transform(content, mimeType, target);
            if (result == null || result.length == 0) {
                log.info("Transform of {} -> {} returned no content", mimeType, target.mimeType());
                return null;
            }
            String text = new String(result, StandardCharsets.UTF_8);
            log.debug("Extracted {} characters of {} from {}", text.length(), target.format(), mimeType);
            return ExtractedText.of(text, target.format());

        } catch (HttpClientErrorException e) {
            if (isUnsupportedTransformError(e)) {
                log.info("Engine does not support {} -> {}", mimeType, target.mimeType());
                return null;
            }
            log.warn("Transform of {} -> {} failed with {}: extraction degrades to the next extractor",
                    mimeType, target.mimeType(), e.getStatusCode());
            return null;
        } catch (RestClientException e) {
            // Unreachable engine, connection reset or read timeout. A cold start on a markdown engine
            // can exceed a short timeout, and that must not fail the ingest.
            log.warn("Transform engine unavailable for {} -> {} ({}): extraction degrades to the "
                    + "next extractor", mimeType, target.mimeType(), e.getMessage());
            return null;
        }
    }

    // ──────────────────────────────────────────────────────────────────────
    // Target selection
    // ──────────────────────────────────────────────────────────────────────

    /** What to ask the engine for, or {@code null} when it advertises no usable transform. */
    private Target resolveTarget(String mimeType) {
        if (mimeType == null || mimeType.isBlank()) {
            return null;
        }
        if (extractionFormat.allowsMarkdown() && isTransformSupported(mimeType, TARGET_MARKDOWN)) {
            return new Target(TARGET_MARKDOWN, EXTENSION_MARKDOWN, TextFormat.MARKDOWN);
        }
        if (isTransformSupported(mimeType, TARGET_PLAIN)) {
            return new Target(TARGET_PLAIN, EXTENSION_PLAIN, TextFormat.PLAIN);
        }
        return null;
    }

    /**
     * Whether the engine advertises {@code sourceMimeType -> targetMimeType}.
     *
     * <p>Fails <em>open</em> when the config cannot be read, letting the transform itself decide. That
     * is deliberate: an engine that is slow to start would otherwise be treated as supporting nothing
     * for its first five minutes, and a real attempt returning 400 is cheap and unambiguous.</p>
     */
    boolean isTransformSupported(String sourceMimeType, String targetMimeType) {
        if (sourceMimeType == null || sourceMimeType.isBlank()
                || targetMimeType == null || targetMimeType.isBlank()) {
            return false;
        }

        EngineConfig config = engineConfig();
        if (config == null || config.getTransformers() == null) {
            // Only fail open for plaintext. Guessing that an unknown engine can produce markdown
            // would send every document down a path that 400s, when plaintext is the safe default.
            return TARGET_PLAIN.equals(targetMimeType);
        }

        return config.getTransformers().stream()
                .filter(t -> t.getSupportedSourceAndTargetList() != null)
                .flatMap(t -> t.getSupportedSourceAndTargetList().stream())
                .anyMatch(s -> targetMimeType.equals(s.getTargetMediaType())
                        && sourceMimeType.equals(s.getSourceMediaType()));
    }

    private EngineConfig engineConfig() {
        Instant now = Instant.now();
        EngineConfig local = cachedConfig;
        Instant localAt = cachedConfigAt;

        if (local != null && localAt != null
                && Duration.between(localAt, now).compareTo(configCacheTtl) < 0) {
            return local;
        }

        try {
            EngineConfig fetched = restClient.get()
                    .uri("/transform/config")
                    .retrieve()
                    .body(EngineConfig.class);
            cachedConfig = fetched;
            cachedConfigAt = now;
            return fetched;
        } catch (Exception e) {
            log.debug("Could not read engine config from /transform/config: {}", e.getMessage());
            return cachedConfig; // possibly stale, still better than nothing
        }
    }

    // ──────────────────────────────────────────────────────────────────────
    // Transform request
    // ──────────────────────────────────────────────────────────────────────

    private byte[] transform(Resource content, String sourceMimeType, Target target) {
        MultipartBodyBuilder builder = new MultipartBodyBuilder();
        builder.part("file", content).contentType(MediaType.parseMediaType(sourceMimeType));
        builder.part("sourceMimetype", sourceMimeType);
        builder.part("targetMimetype", target.mimeType());
        builder.part("targetExtension", target.extension());

        MultiValueMap<String, HttpEntity<?>> body = builder.build();

        return restClient.post()
                .uri(uriBuilder -> uriBuilder
                        .path("/transform")
                        .queryParam("timeout", timeoutMs)
                        .build())
                .contentType(MediaType.MULTIPART_FORM_DATA)
                .body(body)
                .retrieve()
                .body(byte[].class);
    }

    /** The engine's own wording for "I have no transformer for this pair". */
    private boolean isUnsupportedTransformError(HttpClientErrorException e) {
        return e.getStatusCode() == HttpStatus.BAD_REQUEST
                && e.getResponseBodyAsString() != null
                && e.getResponseBodyAsString().contains("No transforms for:");
    }

    private record Target(String mimeType, String extension, TextFormat format) {}

    // ──────────────────────────────────────────────────────────────────────
    // GET /transform/config response
    // ──────────────────────────────────────────────────────────────────────

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    static class EngineConfig {
        @JsonProperty("transformers")
        private List<TransformerDef> transformers;
    }

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    static class TransformerDef {
        @JsonProperty("transformerName")
        private String transformerName;

        @JsonProperty("supportedSourceAndTargetList")
        private List<SupportedPair> supportedSourceAndTargetList;
    }

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    static class SupportedPair {
        @JsonProperty("sourceMediaType")
        private String sourceMediaType;

        @JsonProperty("targetMediaType")
        private String targetMediaType;

        @JsonProperty("maxSourceSizeBytes")
        private Long maxSourceSizeBytes;
    }
}
