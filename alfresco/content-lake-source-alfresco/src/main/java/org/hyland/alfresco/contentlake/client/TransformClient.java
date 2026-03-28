package org.hyland.alfresco.contentlake.client;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.hyland.contentlake.spi.TextExtractor;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.core.io.Resource;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.client.MultipartBodyBuilder;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.HttpClientErrorException;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;

/**
 * Client for Alfresco Transform Core AIO (Local Transform, /transform endpoint).
 *
 * This client performs synchronous transformations by sending the source content to a Transform Core AIO
 * instance (typically running as "transform-core-aio" in the ACS Docker Compose deployment) and returning
 * the transformed bytes as the HTTP response body.
 *
 * Goal: extract plain text (text/plain) from any source mimetype, whenever Transform Core AIO supports it.
 */
@Slf4j
public class TransformClient implements TextExtractor {

    private static final String TARGET_MIMETYPE = "text/plain";
    private static final String TARGET_EXTENSION = "txt";

    private static final long DEFAULT_TIMEOUT_MS = 60000;
    private static final Duration DEFAULT_CONFIG_CACHE_TTL = Duration.ofMinutes(5);

    private final RestClient restClient;
    private final long timeoutMs;

    private volatile EngineConfig cachedConfig;
    private volatile Instant cachedConfigAt;

    private final Duration configCacheTtl;

    /**
     * Create a TransformClient with custom timeout.
     *
     * @param baseUrl Base URL of Transform Core AIO
     * @param timeoutMs Timeout in milliseconds for transformation requests
     */
    public TransformClient(String baseUrl, long timeoutMs) {
        this(baseUrl, timeoutMs, DEFAULT_CONFIG_CACHE_TTL);
    }

    public TransformClient(String baseUrl, long timeoutMs, Duration configCacheTtl) {
        this.timeoutMs = timeoutMs;
        this.configCacheTtl = configCacheTtl;

        // RestClient baseUrl accepts both with and without trailing slash; keep as-is.
        this.restClient = RestClient.builder()
                .baseUrl(baseUrl)
                .build();

        log.info("TransformClient initialized: baseUrl={}, timeout={}ms, configCacheTtl={}",
                baseUrl, timeoutMs, configCacheTtl);
    }

    // ──────────────────────────────────────────────────────────────────────
    // TextExtractor — SPI implementation
    // ──────────────────────────────────────────────────────────────────────

    /** Returns {@code true} when Transform Core AIO supports extracting text from this MIME type. */
    @Override
    public boolean supports(String mimeType) {
        return isTransformSupported(mimeType, TARGET_MIMETYPE);
    }

    /**
     * Extracts plain text by submitting the content to Transform Core AIO.
     *
     * @return extracted text, or {@code null} when the transform is unsupported or yields no output
     */
    @Override
    public String extractText(Resource content, String mimeType) {
        if (!supports(mimeType)) {
            return null;
        }
        try {
            byte[] result = transformSync(content, mimeType, TARGET_MIMETYPE);
            if (result == null) {
                return null;
            }
            String text = new String(result, java.nio.charset.StandardCharsets.UTF_8);
            log.debug("Text extraction successful for {}: {} characters", mimeType, text.length());
            return text;
        } catch (org.springframework.web.client.HttpClientErrorException e) {
            if (isUnsupportedTransformError(e)) {
                log.info("Transform Core AIO does not support {} -> {}", mimeType, TARGET_MIMETYPE);
                return null;
            }
            throw e;
        }
    }

    // ──────────────────────────────────────────────────────────────────────
    // Alfresco-specific transform helpers
    // ──────────────────────────────────────────────────────────────────────

    public String transformToText(Resource content, String sourceMimeType) {
        if (sourceMimeType == null || sourceMimeType.isBlank()) {
            throw new IllegalArgumentException("Source mime type is required");
        }

        if (!isTransformSupported(sourceMimeType, TARGET_MIMETYPE)) {
            throw new UnsupportedOperationException(
                    "Transform Core AIO does not support " + sourceMimeType + " -> " + TARGET_MIMETYPE);
        }

        log.debug("Requesting transformation: {} -> {}", sourceMimeType, TARGET_MIMETYPE);

        byte[] result = transformSync(content, sourceMimeType, TARGET_MIMETYPE);

        if (result == null) {
            throw new RestClientException("Transform returned null response");
        }

        String text = new String(result, StandardCharsets.UTF_8);
        log.debug("Transformation successful, extracted {} characters", text.length());
        return text;
    }

    public byte[] transformSync(byte[] content, String sourceMimeType, String targetMimeType) {
        MultipartBodyBuilder builder = new MultipartBodyBuilder();

        builder.part("file", new ByteArrayResource(content) {
                    @Override
                    public String getFilename() {
                        return "content" + getExtensionForMimeType(sourceMimeType);
                    }
                })
                .contentType(MediaType.parseMediaType(sourceMimeType));

        builder.part("sourceMimetype", sourceMimeType);
        builder.part("targetMimetype", targetMimeType);
        builder.part("targetExtension", TARGET_EXTENSION);

        MultiValueMap<String, HttpEntity<?>> body = builder.build();

        try {
            return restClient.post()
                    .uri(uriBuilder -> uriBuilder
                            .path("/transform")
                            .queryParam("timeout", timeoutMs)
                            .build())
                    .contentType(MediaType.MULTIPART_FORM_DATA)
                    .body(body)
                    .retrieve()
                    .body(byte[].class);

        } catch (RestClientException e) {
            logTransformFailure(sourceMimeType, targetMimeType, e);
            throw e;
        }
    }

    /**
     * Streaming-friendly variant when content comes from a FileSystemResource (temp file).
     */
    public byte[] transformSync(Resource content, String sourceMimeType, String targetMimeType) {
        MultipartBodyBuilder builder = new MultipartBodyBuilder();

        builder.part("file", content)
                .contentType(MediaType.parseMediaType(sourceMimeType));

        builder.part("sourceMimetype", sourceMimeType);
        builder.part("targetMimetype", targetMimeType);
        builder.part("targetExtension", TARGET_EXTENSION);

        MultiValueMap<String, HttpEntity<?>> body = builder.build();

        try {
            return restClient.post()
                    .uri(uriBuilder -> uriBuilder
                            .path("/transform")
                            .queryParam("timeout", timeoutMs)
                            .build())
                    .contentType(MediaType.MULTIPART_FORM_DATA)
                    .body(body)
                    .retrieve()
                    .body(byte[].class);

        } catch (RestClientException e) {
            logTransformFailure(sourceMimeType, targetMimeType, e);
            throw e;
        }
    }

    /**
     * Check if a transformation is supported by Transform Core AIO.
     *
     * Transform Core AIO returns its full engine configuration from GET /transform/config.
     * This method caches that response and answers support checks locally.
     */
    public boolean isTransformSupported(String sourceMimeType, String targetMimeType) {
        if (sourceMimeType == null || sourceMimeType.isBlank()) {
            return false;
        }
        if (targetMimeType == null || targetMimeType.isBlank()) {
            return false;
        }

        EngineConfig config = getEngineConfigCached();

        // If config cannot be read, fail open (let the actual transform decide).
        if (config == null || config.getTransformers() == null) {
            return true;
        }

        return config.getTransformers().stream()
                .filter(t -> t.getSupportedSourceAndTargetList() != null)
                .flatMap(t -> t.getSupportedSourceAndTargetList().stream())
                .anyMatch(s -> targetMimeType.equals(s.getTargetMediaType())
                        && sourceMimeType.equals(s.getSourceMediaType()));
    }

    private EngineConfig getEngineConfigCached() {
        Instant now = Instant.now();
        EngineConfig local = cachedConfig;
        Instant localAt = cachedConfigAt;

        if (local != null && localAt != null && Duration.between(localAt, now).compareTo(configCacheTtl) < 0) {
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
            log.debug("Could not read Transform Core AIO config from /transform/config: {}", e.getMessage());
            return cachedConfig; // maybe stale, but better than nothing
        }
    }

    private void logTransformFailure(String sourceMimeType, String targetMimeType, RestClientException e) {
        if (isUnsupportedTransformError(e)) {
            log.info("Transform unavailable for {} -> {}: {}", sourceMimeType, targetMimeType, e.getMessage());
            return;
        }
        log.error("Transform request failed for {} -> {}: {}", sourceMimeType, targetMimeType, e.getMessage());
    }

    private boolean isUnsupportedTransformError(RestClientException e) {
        if (!(e instanceof HttpClientErrorException httpError)) {
            return false;
        }
        return httpError.getStatusCode() == HttpStatus.BAD_REQUEST
                && httpError.getResponseBodyAsString() != null
                && httpError.getResponseBodyAsString().contains("No transforms for:");
    }

    /**
     * Get file extension for a mime type (used for temp file naming).
     */
    private String getExtensionForMimeType(String mimeType) {
        return switch (mimeType) {
            case "application/pdf" -> ".pdf";
            case "application/msword" -> ".doc";
            case "application/vnd.openxmlformats-officedocument.wordprocessingml.document" -> ".docx";
            case "application/vnd.ms-excel" -> ".xls";
            case "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" -> ".xlsx";
            case "application/vnd.ms-powerpoint" -> ".ppt";
            case "application/vnd.openxmlformats-officedocument.presentationml.presentation" -> ".pptx";
            case "text/html" -> ".html";
            case "text/xml", "application/xml" -> ".xml";
            case "application/json" -> ".json";
            case "text/plain" -> ".txt";
            case "text/csv" -> ".csv";
            case "image/png" -> ".png";
            case "image/jpeg" -> ".jpg";
            case "image/tiff" -> ".tiff";
            default -> "";
        };
    }

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
