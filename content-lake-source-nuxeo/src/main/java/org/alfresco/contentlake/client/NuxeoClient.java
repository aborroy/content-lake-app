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
import java.io.InputStream;
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
    private static final String DOCUMENT_PROPERTIES_HEADER = "X-NXDocumentProperties";
    private static final String ALL_DOCUMENT_PROPERTIES = "*";

    private final RestClient restClient;
    private final String apiBaseUrl;
    private final String sourceId;
    private final String blobXpath;

    public NuxeoClient(NuxeoProperties properties) {
        this(properties, new BasicNuxeoAuthentication(properties.getUsername(), properties.getPassword()));
    }

    public NuxeoClient(NuxeoProperties properties, NuxeoAuthentication authentication) {
        this(
                properties.getBaseUrl(),
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
            return toSourceNode(fetchDocument("/id/{uid}", nodeId, true));
        } catch (RestClientResponseException e) {
            if (e.getStatusCode().value() == 404) {
                return null;
            }
            throw new IllegalStateException("Failed to fetch Nuxeo document " + nodeId, e);
        }
    }

    public @Nullable SourceNode getNodeByPath(String repositoryPath) {
        try {
            String encodedPath = encodePathPreservingSlashes(trimLeadingSlash(repositoryPath));
            return toSourceNode(fetchDocument("/path/" + encodedPath, null, true));
        } catch (RestClientResponseException e) {
            if (e.getStatusCode().value() == 404 || e.getStatusCode().value() == 204) {
                return null;
            }
            throw new IllegalStateException("Failed to fetch Nuxeo document at path " + repositoryPath, e);
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

            NuxeoDocument.Page page = fetchChildrenPage(containerId, pageIndex, pageSize);
            List<NuxeoDocument> entries = page.getEntries();
            if (entries.isEmpty() || startIndex >= entries.size()) {
                break;
            }

            int remaining = maxItems - results.size();
            int endIndex = Math.min(entries.size(), startIndex + remaining);
            for (NuxeoDocument document : entries.subList(startIndex, endIndex)) {
                results.add(NuxeoSourceNodeAdapter.toSourceNode(document, sourceId, blobXpath));
            }

            if (entries.size() < pageSize || !page.hasMore()) {
                break;
            }
            currentSkip = (pageIndex + 1) * pageSize;
        }

        return results;
    }

    @Override
    public Resource downloadContent(String nodeId, String fileName) {
        String suffix = sanitizeTempSuffix(fileName);
        Path tempFile = null;
        try {
            tempFile = Files.createTempFile("nuxeo-", suffix);
            Path targetFile = tempFile;
            restClient.get()
                    .uri(blobUri(nodeId))
                    .exchange((request, response) -> {
                        if (response.getStatusCode().isError()) {
                            throw new IllegalStateException("Failed to download Nuxeo content for " + nodeId
                                    + " (HTTP " + response.getStatusCode().value() + ")");
                        }
                        try (InputStream stream = response.getBody()) {
                            if (stream == null) {
                                throw new IllegalStateException("Received empty response body for Nuxeo content " + nodeId);
                            }
                            Files.copy(stream, targetFile, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
                        }
                        return null;
                    });
            return new FileSystemResource(tempFile);
        } catch (IOException e) {
            deleteQuietly(tempFile);
            throw new IllegalStateException("Failed to write Nuxeo content to temp file for " + nodeId, e);
        } catch (RuntimeException e) {
            deleteQuietly(tempFile);
            throw e;
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

    public List<SourceNode> searchByNxql(String nxql, int pageIndex, int pageSize) {
        NuxeoDocument.Page page = searchPageByNxql(nxql, pageIndex, pageSize);
        return page.getEntries().stream().map(this::toSourceNode).toList();
    }

    public NuxeoDocument.Page searchPageByNxql(String nxql, int pageIndex, int pageSize) {
        try {
            NuxeoDocument.Page response = restClient.get()
                    .uri(uriBuilder -> uriBuilder
                            .path("/search/lang/NXQL/execute")
                            .queryParam("query", nxql)
                            .queryParam("pageSize", pageSize)
                            .queryParam("currentPageIndex", pageIndex)
                            .build())
                    .header(DOCUMENT_PROPERTIES_HEADER, ALL_DOCUMENT_PROPERTIES)
                    .accept(MediaType.APPLICATION_JSON)
                    .retrieve()
                    .body(NuxeoDocument.Page.class);
            return response != null ? response : new NuxeoDocument.Page();
        } catch (RestClientResponseException e) {
            if (isUnsupportedSearchStatus(e.getStatusCode().value())) {
                throw new UnsupportedOperationException("Nuxeo NXQL search endpoint is not available", e);
            }
            throw new IllegalStateException("Failed to execute NXQL search", e);
        }
    }

    private NuxeoDocument.Page fetchChildrenPage(String containerId, int pageIndex, int pageSize) {
        try {
            NuxeoDocument.Page response = restClient.get()
                    .uri(uriBuilder -> uriBuilder
                            .path("/id/{uid}/@children")
                            .queryParam("currentPageIndex", pageIndex)
                            .queryParam("pageSize", pageSize)
                            .build(containerId))
                    .header(DOCUMENT_PROPERTIES_HEADER, ALL_DOCUMENT_PROPERTIES)
                    .accept(MediaType.APPLICATION_JSON)
                    .retrieve()
                    .body(NuxeoDocument.Page.class);
            return response != null ? response : new NuxeoDocument.Page();
        } catch (RestClientResponseException e) {
            if (e.getStatusCode().value() == 404) {
                return new NuxeoDocument.Page();
            }
            throw new IllegalStateException("Failed to list Nuxeo children for " + containerId, e);
        }
    }

    private @Nullable NuxeoDocument fetchDocument(String pathTemplate, @Nullable String pathVariable, boolean enrichAcl) {
        RestClient.RequestHeadersSpec<?> request = restClient.get()
                .uri(pathVariable == null ? pathTemplate : pathTemplate, pathVariable)
                .header(DOCUMENT_PROPERTIES_HEADER, ALL_DOCUMENT_PROPERTIES)
                .accept(MediaType.APPLICATION_JSON);
        if (enrichAcl) {
            request = request.header("enrichers-document", ACL_ENRICHER);
        }
        return request.retrieve().body(NuxeoDocument.class);
    }

    private @Nullable SourceNode toSourceNode(@Nullable NuxeoDocument document) {
        return document != null ? NuxeoSourceNodeAdapter.toSourceNode(document, sourceId, blobXpath) : null;
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

    private static void deleteQuietly(@Nullable Path path) {
        if (path == null) {
            return;
        }
        try {
            Files.deleteIfExists(path);
        } catch (IOException ignored) {
            // Best-effort cleanup for failed temp-file downloads.
        }
    }

    private static String trimLeadingSlash(String value) {
        if (value == null || value.isBlank() || "/".equals(value)) {
            return "";
        }
        return value.startsWith("/") ? value.substring(1) : value;
    }

    private static boolean isUnsupportedSearchStatus(int status) {
        return status == 404 || status == 405 || status == 501;
    }
}
