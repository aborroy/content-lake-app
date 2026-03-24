package org.alfresco.contentlake.client;

import lombok.extern.slf4j.Slf4j;
import org.alfresco.contentlake.adapter.NuxeoSourceNodeAdapter;
import org.alfresco.contentlake.auth.BasicNuxeoAuthentication;
import org.alfresco.contentlake.auth.NuxeoAuthentication;
import org.alfresco.contentlake.config.NuxeoProperties;
import org.alfresco.contentlake.model.NuxeoDocument;
import org.alfresco.contentlake.spi.ContentSourceClient;
import org.alfresco.contentlake.spi.SourceNode;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.lang.Nullable;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientResponseException;
import org.springframework.web.util.UriUtils;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * Nuxeo REST API client implementing the source SPI.
 */
@Slf4j
public class NuxeoClient implements ContentSourceClient {

    private static final String SOURCE_TYPE = "nuxeo";
    private static final String ACL_ENRICHER = "acls";

    private final RestClient restClient;
    private final String apiBaseUrl;
    private final String sourceId;
    private final String blobXpath;

    public NuxeoClient(NuxeoProperties properties) {
        this(properties, new BasicNuxeoAuthentication(properties.getUsername(), properties.getPassword()));
    }

    public NuxeoClient(NuxeoProperties properties, NuxeoAuthentication authentication) {
        this(
                buildApiBaseUrl(properties.getBaseUrl()),
                properties.getSourceId(),
                properties.getBlobXpath(),
                authentication
        );
    }

    public NuxeoClient(String baseUrl, String sourceId, String blobXpath, NuxeoAuthentication authentication) {
        this.apiBaseUrl = buildApiBaseUrl(baseUrl);
        this.sourceId = sourceId;
        this.blobXpath = (blobXpath == null || blobXpath.isBlank()) ? "file:content" : blobXpath;
        this.restClient = RestClient.builder()
                .baseUrl(apiBaseUrl)
                .requestInterceptor(authentication.asInterceptor())
                .build();
    }

    @Override
    public String getSourceId() {
        return sourceId;
    }

    @Override
    public String getSourceType() {
        return SOURCE_TYPE;
    }

    @Override
    public @Nullable SourceNode getNode(String nodeId) {
        try {
            NuxeoDocument document = restClient.get()
                    .uri("/id/{uid}", nodeId)
                    .accept(MediaType.APPLICATION_JSON)
                    .header("enrichers-document", ACL_ENRICHER)
                    .retrieve()
                    .body(NuxeoDocument.class);
            return document != null ? NuxeoSourceNodeAdapter.toSourceNode(document, sourceId, blobXpath) : null;
        } catch (RestClientResponseException e) {
            if (e.getStatusCode().value() == 404) {
                return null;
            }
            throw new IllegalStateException("Failed to fetch Nuxeo document " + nodeId, e);
        }
    }

    @Override
    public List<SourceNode> getChildren(String containerId, int skip, int maxItems) {
        if (maxItems <= 0) {
            return List.of();
        }

        List<SourceNode> results = new ArrayList<>(maxItems);
        int pageSize = maxItems;
        int currentSkip = Math.max(skip, 0);

        while (results.size() < maxItems) {
            int pageIndex = currentSkip / pageSize;
            int startIndex = currentSkip % pageSize;

            List<NuxeoDocument> page = fetchChildrenPage(containerId, pageIndex, pageSize);
            if (page.isEmpty() || startIndex >= page.size()) {
                break;
            }

            int remaining = maxItems - results.size();
            int endIndex = Math.min(page.size(), startIndex + remaining);
            for (NuxeoDocument document : page.subList(startIndex, endIndex)) {
                results.add(NuxeoSourceNodeAdapter.toSourceNode(document, sourceId, blobXpath));
            }

            if (page.size() < pageSize) {
                break;
            }
            currentSkip = (pageIndex + 1) * pageSize;
        }

        return results;
    }

    @Override
    public Resource downloadContent(String nodeId, String fileName) {
        byte[] content = getContent(nodeId);
        String suffix = sanitizeTempSuffix(fileName);
        try {
            Path tempFile = Files.createTempFile("nuxeo-", suffix);
            Files.write(tempFile, content);
            return new FileSystemResource(tempFile);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to write Nuxeo content to temp file for " + nodeId, e);
        }
    }

    @Override
    public byte[] getContent(String nodeId) {
        try {
            byte[] content = restClient.get()
                    .uri(blobUri(nodeId))
                    .retrieve()
                    .body(byte[].class);
            return content != null ? content : new byte[0];
        } catch (RestClientResponseException e) {
            throw new IllegalStateException("Failed to download Nuxeo content for " + nodeId, e);
        }
    }

    private List<NuxeoDocument> fetchChildrenPage(String containerId, int pageIndex, int pageSize) {
        try {
            NuxeoDocument.Page response = restClient.get()
                    .uri(uriBuilder -> uriBuilder
                            .path("/id/{uid}/@children")
                            .queryParam("currentPageIndex", pageIndex)
                            .queryParam("pageSize", pageSize)
                            .build(containerId))
                    .accept(MediaType.APPLICATION_JSON)
                    .retrieve()
                    .body(NuxeoDocument.Page.class);
            if (response == null || response.getEntries() == null) {
                return List.of();
            }
            return response.getEntries();
        } catch (RestClientResponseException e) {
            if (e.getStatusCode().value() == 404) {
                return List.of();
            }
            throw new IllegalStateException("Failed to list Nuxeo children for " + containerId, e);
        }
    }

    private URI blobUri(String nodeId) {
        String encodedNodeId = UriUtils.encodePathSegment(nodeId, StandardCharsets.UTF_8);
        String encodedBlobXpath = encodePathPreservingSlashes(blobXpath);
        return URI.create(apiBaseUrl + "/id/" + encodedNodeId + "/@blob/" + encodedBlobXpath);
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

    private static String sanitizeTempSuffix(String fileName) {
        String cleaned = (fileName == null || fileName.isBlank()) ? "content.bin" : fileName;
        cleaned = cleaned.replaceAll("[^A-Za-z0-9._-]", "_");
        return cleaned.startsWith(".") ? cleaned : "-" + cleaned;
    }
}
