package org.hyland.alfresco.contentlake.client;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.hyland.alfresco.contentlake.adapter.AlfrescoSourceNodeAdapter;
import org.hyland.contentlake.spi.ContentSourceClient;
import org.hyland.contentlake.spi.SourceNode;
import org.alfresco.core.handler.NodesApi;
import org.alfresco.core.model.Node;
import org.alfresco.core.model.NodeBodyUpdate;
import org.alfresco.core.model.NodeChildAssociationEntry;
import org.alfresco.core.model.NodeChildAssociationPaging;
import org.alfresco.core.model.PermissionElement;
import org.alfresco.discovery.handler.DiscoveryApi;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClientResponseException;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Alfresco REST client wrapper used by the ingestion pipeline.
 *
 * <p>Implements {@link ContentSourceClient} so the shared sync pipeline can work
 * through the source-agnostic SPI. Alfresco-specific callers (scope resolver,
 * live/batch ingesters) use the {@link #getAlfrescoNode} and
 * {@link #getAllChildren} methods directly.</p>
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class AlfrescoClient implements ContentSourceClient {

    private static final int DEFAULT_PAGE_SIZE = 100;

    private static final List<String> INCLUDE = List.of("properties", "path", "permissions");

    private static final Set<String> READ_ROLES = Set.of(
            "Consumer", "Contributor", "Collaborator", "Coordinator", "Manager"
    );

    /**
     * Tracks permission names that are ALLOWED but not recognized as read access.
     *
     * <p>Used only for debug logging to avoid noisy logs on large repositories.</p>
     */
    private static final Set<String> UNRECOGNIZED_ALLOWED_PERMISSION_NAMES = ConcurrentHashMap.newKeySet();

    private final NodesApi nodesApi;
    private final DiscoveryApi discoveryApi;

    private volatile @Nullable String cachedRepositoryId;

    // ──────────────────────────────────────────────────────────────────────
    // ContentSourceClient — SPI implementation
    // ──────────────────────────────────────────────────────────────────────

    @Override
    public String getSourceId() {
        return getRepositoryId();
    }

    @Override
    public String getSourceType() {
        return "alfresco";
    }

    /**
     * Fetches a node and converts it to a {@link SourceNode}.
     *
     * <p>Read principals are extracted from the node's permissions before
     * conversion so the shared pipeline does not need Alfresco SDK types.</p>
     */
    @Override
    public @Nullable SourceNode getNode(String nodeId) {
        Node node = getAlfrescoNode(nodeId);
        if (node == null) {
            return null;
        }
        Set<String> readers = extractReadAuthorities(node);
        return AlfrescoSourceNodeAdapter.toSourceNode(node, getSourceId(), readers);
    }

    /** Lists direct children as {@link SourceNode} instances. */
    @Override
    public List<SourceNode> getChildren(String containerId, int skip, int maxItems) {
        List<Node> nodes = listChildren(containerId, skip, maxItems);
        List<SourceNode> result = new ArrayList<>(nodes.size());
        String sourceId = getSourceId();
        for (Node node : nodes) {
            Set<String> readers = extractReadAuthorities(node);
            result.add(AlfrescoSourceNodeAdapter.toSourceNode(node, sourceId, readers));
        }
        return result;
    }

    /** Downloads content to a temporary file resource. */
    @Override
    public Resource downloadContent(String nodeId, String fileName) {
        return downloadContentToTempFile(nodeId, fileName);
    }

    /**
     * Downloads content as bytes.
     *
     * <p>Any {@link IOException} from the underlying Alfresco SDK call is
     * wrapped in an {@link IllegalStateException} so the SPI contract
     * (no checked exceptions) is satisfied.</p>
     */
    @Override
    public byte[] getContent(String nodeId) {
        try {
            return nodesApi.getNodeContent(nodeId, true, null, null)
                    .getBody()
                    .getContentAsByteArray();
        } catch (IOException e) {
            throw new IllegalStateException("Failed to download content for nodeId=" + nodeId, e);
        }
    }

    // ──────────────────────────────────────────────────────────────────────
    // Alfresco-specific API (used by scope resolver, ingesters, reconciler)
    // ──────────────────────────────────────────────────────────────────────

    /**
     * Lists direct children of a folder using pagination, returning Alfresco {@link Node} objects.
     *
     * <p>Used internally by {@link #getAllChildren} and by scope-aware traversal code
     * that needs Alfresco-specific aspect information.</p>
     *
     * @param folderId  folder node identifier
     * @param skipCount number of entries to skip
     * @param maxItems  maximum number of entries to return
     * @return list of children as Alfresco nodes
     */
    public List<Node> listChildren(String folderId, int skipCount, int maxItems) {
        NodeChildAssociationPaging response = nodesApi.listNodeChildren(
                folderId,
                skipCount,
                maxItems,
                null,
                null,
                INCLUDE,
                null,
                null,
                null
        ).getBody();

        if (response == null || response.getList() == null || response.getList().getEntries() == null) {
            return List.of();
        }

        List<Node> nodes = new ArrayList<>(response.getList().getEntries().size());
        for (NodeChildAssociationEntry entry : response.getList().getEntries()) {
            nodes.add(entry.getEntry());
        }
        return nodes;
    }

    /**
     * Lists all direct children of a folder by paging until exhaustion.
     *
     * @param folderId folder node identifier
     * @return list of children nodes
     */
    public List<Node> getAllChildren(String folderId) {
        List<Node> allNodes = new ArrayList<>();
        int skipCount = 0;

        while (true) {
            List<Node> batch = listChildren(folderId, skipCount, DEFAULT_PAGE_SIZE);
            allNodes.addAll(batch);

            if (batch.size() < DEFAULT_PAGE_SIZE) {
                break;
            }
            skipCount += DEFAULT_PAGE_SIZE;
        }

        return allNodes;
    }

    /**
     * Retrieves a single Alfresco node with properties, path, and permissions.
     *
     * <p>Use this when Alfresco-specific fields (aspects, permissions) are needed.
     * For source-agnostic access use {@link #getNode(String)} instead.</p>
     *
     * @param nodeId node identifier
     * @return the node, or {@code null} when it cannot be fetched
     */
    public @Nullable Node getAlfrescoNode(String nodeId) {
        try {
            var response = nodesApi.getNode(nodeId, INCLUDE, null, null);
            return response != null && response.getBody() != null
                    ? response.getBody().getEntry()
                    : null;
        } catch (RestClientResponseException e) {
            log.debug("Could not fetch node {}: status={} body={}", nodeId, e.getStatusCode(), e.getResponseBodyAsString());
            return null;
        } catch (Exception e) {
            log.debug("Could not fetch node {}: {}", nodeId, e.getMessage());
            return null;
        }
    }

    /**
     * Updates node aspects and optional properties.
     *
     * @param nodeId      node identifier
     * @param aspectNames complete aspect list to persist
     * @param properties  optional property map to persist
     * @return updated node entry
     */
    public Node updateNode(String nodeId, List<String> aspectNames, @Nullable Map<String, Object> properties) {
        try {
            NodeBodyUpdate body = new NodeBodyUpdate();
            body.setAspectNames(new ArrayList<>(aspectNames));
            if (properties != null) {
                body.setProperties(new LinkedHashMap<>(properties));
            }

            var response = nodesApi.updateNode(nodeId, body, INCLUDE, null);
            if (response == null || response.getBody() == null || response.getBody().getEntry() == null) {
                throw new IllegalStateException("Empty response while updating node " + nodeId);
            }

            return response.getBody().getEntry();
        } catch (RestClientResponseException e) {
            throw new IllegalStateException(
                    "Failed to update node " + nodeId + ": status=" + e.getStatusCode() + " body=" + e.getResponseBodyAsString(),
                    e
            );
        } catch (Exception e) {
            throw new IllegalStateException("Failed to update node " + nodeId, e);
        }
    }

    /**
     * Downloads node content to a temporary file.
     *
     * <p>Useful for large files and for clients that require content-length determination.</p>
     *
     * @param nodeId   node identifier
     * @param fileName preferred file name used as a suffix for the temp file
     * @return file system resource pointing to the temp file
     */
    public Resource downloadContentToTempFile(String nodeId, String fileName) {
        String safeName = sanitizeFileName(fileName);

        try (InputStream in = nodesApi.getNodeContent(nodeId, true, null, null)
                .getBody()
                .getInputStream()) {

            Path tmp = Files.createTempFile("alfresco-node-", "-" + safeName);
            Files.copy(in, tmp, StandardCopyOption.REPLACE_EXISTING);
            return new FileSystemResource(tmp.toFile());

        } catch (Exception e) {
            throw new IllegalStateException("Failed to download content for nodeId=" + nodeId, e);
        }
    }

    /**
     * Extracts authority identifiers that have read access on the node.
     *
     * @param node node with permissions included
     * @return set of authority identifiers
     */
    public Set<String> extractReadAuthorities(Node node) {
        Set<String> readers = new HashSet<>();

        if (node.getPermissions() == null) {
            return readers;
        }

        // Treat null as enabled; only explicit false disables inheritance.
        Boolean inheritanceEnabled = node.getPermissions().isIsInheritanceEnabled();
        addAllowedReaders(readers, node.getPermissions().getInherited(), !Boolean.FALSE.equals(inheritanceEnabled));
        addAllowedReaders(readers, node.getPermissions().getLocallySet(), true);

        return readers;
    }

    private void addAllowedReaders(Set<String> readers, List<PermissionElement> permissions, boolean enabled) {
        if (!enabled || permissions == null) {
            return;
        }

        for (PermissionElement perm : permissions) {
            if (!PermissionElement.AccessStatusEnum.ALLOWED.equals(perm.getAccessStatus())) {
                continue;
            }

            String permissionName = perm.getName();
            if (hasReadAccess(permissionName)) {
                readers.add(perm.getAuthorityId());
            } else {
                if (permissionName != null
                        && log.isDebugEnabled()
                        && UNRECOGNIZED_ALLOWED_PERMISSION_NAMES.add(permissionName)) {
                    log.debug("Ignoring ALLOWED permission name '{}' when computing read authorities", permissionName);
                }
            }
        }
    }

    private boolean hasReadAccess(String role) {
        if (role == null) {
            return false;
        }

        if (READ_ROLES.contains(role)) {
            return true;
        }

        if (role.startsWith("Site") && role.length() > 4) {
            String stripped = role.substring(4);
            if (READ_ROLES.contains(stripped)) {
                return true;
            }
        }

        return "ReadPermissions".equals(role) || "Read".equals(role);
    }

    /**
     * Returns the repository identifier and caches it after the first call.
     *
     * @return repository identifier
     */
    public String getRepositoryId() {
        String repoId = cachedRepositoryId;
        if (repoId != null) {
            return repoId;
        }

        synchronized (this) {
            repoId = cachedRepositoryId;
            if (repoId == null) {
                repoId = discoveryApi
                        .getRepositoryInformation()
                        .getBody()
                        .getEntry()
                        .getRepository()
                        .getId();
                cachedRepositoryId = repoId;
            }
        }

        return repoId;
    }

    private String sanitizeFileName(String fileName) {
        if (fileName == null || fileName.isBlank()) {
            return "content.bin";
        }

        String sanitized = fileName.replaceAll("[\\\\/:*?\"<>|\\p{Cntrl}]+", "_").trim();
        if (sanitized.isBlank()) {
            return "content.bin";
        }

        return sanitized.length() > 120 ? sanitized.substring(0, 120) : sanitized;
    }
}
