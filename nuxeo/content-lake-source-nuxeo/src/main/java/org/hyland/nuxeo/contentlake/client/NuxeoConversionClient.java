package org.hyland.contentlake.client;

import lombok.extern.slf4j.Slf4j;
import org.hyland.nuxeo.contentlake.auth.BasicNuxeoAuthentication;
import org.hyland.nuxeo.contentlake.auth.NuxeoAuthentication;
import org.hyland.nuxeo.contentlake.config.NuxeoProperties;
import org.hyland.contentlake.spi.TextExtractor;
import org.springframework.core.io.Resource;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestClientResponseException;
import org.springframework.web.util.UriUtils;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * Client for synchronous Nuxeo server-side blob conversion through the REST {@code @convert} adapter.
 *
 * <p>This mirrors the role of Alfresco's {@link TransformClient}: it is a dedicated client for
 * extracting plain text from non-textual blobs, but Nuxeo performs the conversion on the repository
 * side through {@code ConversionService} instead of receiving uploaded content.</p>
 *
 * <p>Concrete contract used here:
 * {@code GET /nuxeo/api/v1/id/{uid}/@blob/{blobXpath}/@convert?type=text/plain}</p>
 */
@Slf4j
public class NuxeoConversionClient implements TextExtractor {

    private static final String TARGET_MIMETYPE = "text/plain";

    private final RestClient restClient;
    private final String apiBaseUrl;
    private final String blobXpath;
    private final boolean enabled;

    public NuxeoConversionClient(NuxeoProperties properties) {
        this(properties, new BasicNuxeoAuthentication(properties.getUsername(), properties.getPassword()));
    }

    public NuxeoConversionClient(NuxeoProperties properties, NuxeoAuthentication authentication) {
        this(
                properties.getBaseUrl(),
                properties.getBlobXpath(),
                properties.getConversion().getTimeoutMs(),
                properties.getConversion().isEnabled(),
                authentication
        );
    }

    public NuxeoConversionClient(String baseUrl, String blobXpath, long timeoutMs, NuxeoAuthentication authentication) {
        this(baseUrl, blobXpath, timeoutMs, true, authentication);
    }

    public NuxeoConversionClient(String baseUrl,
                                 String blobXpath,
                                 long timeoutMs,
                                 boolean enabled,
                                 NuxeoAuthentication authentication) {
        this.apiBaseUrl = buildApiBaseUrl(baseUrl);
        this.blobXpath = (blobXpath == null || blobXpath.isBlank()) ? "file:content" : blobXpath;
        this.enabled = enabled;

        SimpleClientHttpRequestFactory requestFactory = new SimpleClientHttpRequestFactory();
        int timeout = Math.toIntExact(timeoutMs);
        requestFactory.setConnectTimeout(timeout);
        requestFactory.setReadTimeout(timeout);

        this.restClient = RestClient.builder()
                .baseUrl(apiBaseUrl)
                .requestFactory(requestFactory)
                .requestInterceptor(authentication.asInterceptor())
                .build();

        log.info("NuxeoConversionClient initialized: baseUrl={}, blobXpath={}, timeout={}ms",
                apiBaseUrl, this.blobXpath, timeoutMs);
    }

    @Override
    public boolean supports(String mimeType) {
        return enabled && mimeType != null && !mimeType.isBlank() && !mimeType.startsWith("text/");
    }

    @Override
    public boolean supportsSourceReference(String mimeType) {
        return supports(mimeType);
    }

    @Override
    public String extractText(String nodeId, String mimeType) {
        if (!supportsSourceReference(mimeType)) {
            return null;
        }
        return convertToText(nodeId, mimeType);
    }

    @Override
    public String extractText(Resource content, String mimeType) {
        throw new UnsupportedOperationException("Nuxeo conversion requires source document/blob identity");
    }

    public String convertToText(String nodeId, String sourceMimeType) {
        if (!enabled) {
            throw new UnsupportedOperationException("Nuxeo conversion is disabled");
        }
        if (nodeId == null || nodeId.isBlank()) {
            throw new IllegalArgumentException("Nuxeo nodeId is required");
        }
        if (sourceMimeType == null || sourceMimeType.isBlank()) {
            throw new IllegalArgumentException("Source mime type is required");
        }

        log.debug("Requesting Nuxeo ConversionService text extraction for node {}: {} -> {}",
                nodeId, sourceMimeType, TARGET_MIMETYPE);

        try {
            byte[] result = convertSync(nodeId, TARGET_MIMETYPE);
            if (result == null || result.length == 0) {
                throw new RestClientException("Nuxeo conversion returned no response body");
            }
            return new String(result, StandardCharsets.UTF_8);
        } catch (RestClientException e) {
            if (isUnsupportedConversionError(e)) {
                throw new UnsupportedOperationException(
                        "Nuxeo ConversionService does not support " + sourceMimeType + " -> " + TARGET_MIMETYPE, e);
            }
            throw e;
        }
    }

    public byte[] convertSync(String nodeId, String targetMimeType) {
        if (nodeId == null || nodeId.isBlank()) {
            throw new IllegalArgumentException("Nuxeo nodeId is required");
        }
        if (targetMimeType == null || targetMimeType.isBlank()) {
            throw new IllegalArgumentException("Target mime type is required");
        }

        try {
            return restClient.get()
                    .uri(conversionUri(nodeId, targetMimeType))
                    .retrieve()
                    .body(byte[].class);
        } catch (RestClientException e) {
            logConversionFailure(nodeId, targetMimeType, e);
            throw e;
        }
    }

    private void logConversionFailure(String nodeId, String targetMimeType, RestClientException e) {
        if (e instanceof HttpClientErrorException httpError) {
            log.warn("Nuxeo conversion failed for node {} -> {}: HTTP {} {}",
                    nodeId, targetMimeType, httpError.getStatusCode().value(), httpError.getStatusText());
            return;
        }
        log.error("Nuxeo conversion request failed for node {} -> {}: {}",
                nodeId, targetMimeType, e.getMessage());
    }

    private boolean isUnsupportedConversionError(RestClientException e) {
        if (!(e instanceof RestClientResponseException response)) {
            return false;
        }

        String body = response.getResponseBodyAsString();
        if (body == null || body.isBlank()) {
            return false;
        }

        String normalized = body.toLowerCase(Locale.ROOT);
        return normalized.contains("no converter")
                || normalized.contains("converter not found")
                || normalized.contains("no conversion")
                || normalized.contains("cannot convert")
                || normalized.contains("conversionexception");
    }

    private URI conversionUri(String nodeId, String targetMimeType) {
        String encodedNodeId = UriUtils.encodePathSegment(nodeId, StandardCharsets.UTF_8);
        String encodedBlobXpath = encodePathPreservingSlashes(blobXpath);
        String encodedTargetMimeType = UriUtils.encodeQueryParam(targetMimeType, StandardCharsets.UTF_8);
        return URI.create(apiBaseUrl + "/id/" + encodedNodeId + "/@blob/" + encodedBlobXpath
                + "/@convert?type=" + encodedTargetMimeType);
    }

    private static String buildApiBaseUrl(String baseUrl) {
        String normalized = trimTrailingSlash(baseUrl);
        return normalized.endsWith("/api/v1") ? normalized : normalized + "/api/v1";
    }

    private static String trimTrailingSlash(String value) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("Nuxeo baseUrl is required");
        }
        return value.endsWith("/") ? value.substring(0, value.length() - 1) : value;
    }

    private static String encodePathPreservingSlashes(String value) {
        String[] segments = value.split("/", -1);
        List<String> encoded = new ArrayList<>(segments.length);
        for (String segment : segments) {
            encoded.add(UriUtils.encodePathSegment(segment, StandardCharsets.UTF_8));
        }
        return String.join("/", encoded);
    }
}
