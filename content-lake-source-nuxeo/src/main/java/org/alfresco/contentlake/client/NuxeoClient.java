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
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Nuxeo REST API client implementing the source SPI.
 */
@Slf4j
public class NuxeoClient implements ContentSourceClient {

    private static final String SOURCE_TYPE = "nuxeo";
    private static final String ACL_HEADER = "enrichers-document";
    private static final String ACL_ENRICHER = "acls";
    private static final String DOCUMENT_PROPERTIES_HEADER = "X-NXDocumentProperties";
    private static final String ALL_DOCUMENT_PROPERTIES = "*";
    private static final String GROUP_PREFIX = "GROUP_";
    private static final String EVERYONE_PRINCIPAL = "Everyone";
    private static final String EVERYONE_GROUP = "GROUP_EVERYONE";
    private static final Set<String> READ_PERMISSIONS = Set.of("Read", "ReadVersion", "ReadWrite", "Everything");
    private static final Set<String> UNRECOGNIZED_ALLOWED_PERMISSION_NAMES = ConcurrentHashMap.newKeySet();

    private final RestClient restClient;
    private final String apiBaseUrl;
    private final String sourceId;
    private final String blobXpath;
    private final Map<String, Boolean> groupPrincipalCache = new ConcurrentHashMap<>();

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
            NuxeoDocument document = fetchDocument("/id/{uid}", nodeId, true);
            return document != null ? toSourceNode(document) : null;
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
            NuxeoDocument document = fetchDocument("/path/" + encodedPath, null, true);
            return document != null ? toSourceNode(document) : null;
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
                results.add(toSourceNode(document));
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
                    .header(ACL_HEADER, ACL_ENRICHER)
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
                    .header(ACL_HEADER, ACL_ENRICHER)
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
            request = request.header(ACL_HEADER, ACL_ENRICHER);
        }
        return request.retrieve().body(NuxeoDocument.class);
    }

    public SourceNode toSourceNode(NuxeoDocument document) {
        PermissionSnapshot permissions = extractPermissionSnapshot(document);
        return NuxeoSourceNodeAdapter.toSourceNode(
                document,
                sourceId,
                blobXpath,
                permissions.readers(),
                permissions.denies()
        );
    }

    private PermissionSnapshot extractPermissionSnapshot(NuxeoDocument document) {
        List<NormalizedAce> effectiveReadAces = extractEffectiveReadAces(document);
        LinkedHashSet<String> candidatePrincipals = new LinkedHashSet<>();
        LinkedHashSet<String> explicitDenies = new LinkedHashSet<>();

        for (NormalizedAce ace : effectiveReadAces) {
            candidatePrincipals.add(ace.principal());
            if (!ace.granted()) {
                explicitDenies.add(ace.principal());
            }
        }

        LinkedHashSet<String> readers = new LinkedHashSet<>();
        for (String principal : candidatePrincipals) {
            if (hasEffectiveReadGrant(principal, effectiveReadAces)) {
                readers.add(principal);
            }
        }

        return new PermissionSnapshot(readers, explicitDenies);
    }

    private List<NormalizedAce> extractEffectiveReadAces(NuxeoDocument document) {
        List<NormalizedAce> readAces = new ArrayList<>();
        NuxeoDocument.ContextParameters contextParameters = document.getContextParameters();
        if (contextParameters == null || contextParameters.getAcls() == null) {
            return readAces;
        }

        for (NuxeoDocument.Acl acl : contextParameters.getAcls()) {
            if (acl == null || acl.getAces() == null) {
                continue;
            }
            for (NuxeoDocument.Ace ace : acl.getAces()) {
                if (!hasEffectiveStatus(ace)) {
                    continue;
                }
                if (!isReadPermission(ace.getPermission())) {
                    logUnrecognizedReadPermission(ace);
                    continue;
                }
                String principal = normalizePrincipal(ace.getUsername());
                if (principal != null) {
                    readAces.add(new NormalizedAce(principal, Boolean.TRUE.equals(ace.getGranted())));
                }
            }
        }
        return readAces;
    }

    private boolean hasEffectiveStatus(@Nullable NuxeoDocument.Ace ace) {
        if (ace == null) {
            return false;
        }
        String status = ace.getStatus();
        return status == null || status.isBlank() || "effective".equalsIgnoreCase(status);
    }

    private boolean isReadPermission(@Nullable String permission) {
        return permission != null && READ_PERMISSIONS.contains(permission);
    }

    private void logUnrecognizedReadPermission(@Nullable NuxeoDocument.Ace ace) {
        if (ace == null || !Boolean.TRUE.equals(ace.getGranted())) {
            return;
        }
        String permission = ace.getPermission();
        if (permission != null
                && log.isDebugEnabled()
                && UNRECOGNIZED_ALLOWED_PERMISSION_NAMES.add(permission)) {
            log.debug("Ignoring granted Nuxeo permission '{}' when computing read principals", permission);
        }
    }

    private boolean hasEffectiveReadGrant(String principal, List<NormalizedAce> effectiveReadAces) {
        for (NormalizedAce ace : effectiveReadAces) {
            if (matchesPrincipal(ace.principal(), principal)) {
                return ace.granted();
            }
        }
        return false;
    }

    private boolean matchesPrincipal(String acePrincipal, String principal) {
        return EVERYONE_GROUP.equals(acePrincipal) || acePrincipal.equals(principal);
    }

    private @Nullable String normalizePrincipal(@Nullable String principal) {
        if (principal == null || principal.isBlank()) {
            return null;
        }
        if (EVERYONE_PRINCIPAL.equalsIgnoreCase(principal)) {
            return EVERYONE_GROUP;
        }
        if (principal.startsWith(GROUP_PREFIX)) {
            return principal;
        }
        return isGroupPrincipal(principal) ? GROUP_PREFIX + principal : principal;
    }

    private boolean isGroupPrincipal(String principal) {
        return groupPrincipalCache.computeIfAbsent(principal, this::lookupGroupPrincipal);
    }

    private boolean lookupGroupPrincipal(String principal) {
        try {
            restClient.get()
                    .uri(uriBuilder -> uriBuilder.path("/group/{groupId}").build(principal))
                    .accept(MediaType.APPLICATION_JSON)
                    .retrieve()
                    .body(String.class);
            return true;
        } catch (RestClientResponseException e) {
            if (e.getStatusCode().value() == 404) {
                return false;
            }
            log.warn("Failed to resolve Nuxeo principal type for '{}' (defaulting to user): HTTP {}",
                    principal, e.getStatusCode().value());
            return false;
        } catch (Exception e) {
            log.warn("Failed to resolve Nuxeo principal type for '{}' (defaulting to user): {}",
                    principal, e.getMessage());
            return false;
        }
    }

    private record NormalizedAce(String principal, boolean granted) {}

    private record PermissionSnapshot(Set<String> readers, Set<String> denies) {}

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
