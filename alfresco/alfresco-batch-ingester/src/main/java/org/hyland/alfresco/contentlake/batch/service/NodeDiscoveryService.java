package org.hyland.alfresco.contentlake.batch.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.hyland.alfresco.contentlake.batch.config.IngestionProperties;
import org.hyland.alfresco.contentlake.batch.model.BatchSyncRequest;
import org.hyland.alfresco.contentlake.client.AlfrescoClient;
import org.hyland.alfresco.contentlake.client.AlfrescoSearchService;
import org.hyland.alfresco.contentlake.service.ContentLakeScopeResolver;
import org.alfresco.core.model.Node;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

/**
 * Discovers candidate Alfresco nodes for ingestion based on request parameters or configuration.
 *
 * <p>Before traversing each root folder, this service ensures the folder has the
 * {@code cl:indexed} aspect. If the aspect is missing it is added automatically, making
 * the batch request itself the act of onboarding a folder into Content Lake.</p>
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class NodeDiscoveryService {

    private final AlfrescoClient alfrescoClient;
    private final AlfrescoSearchService searchService;
    private final IngestionProperties props;
    private final ContentLakeScopeResolver scopeResolver;

    /**
     * Discovers nodes from the folders specified in the request.
     *
     * @param request discovery configuration
     * @return stream of nodes matching type and exclusion rules
     */
    public Stream<Node> discoverNodes(BatchSyncRequest request) {
        List<String> folders = request.getFolders();
        boolean recursive = request.isRecursive();
        List<String> types = request.getTypes();

        return folders.stream()
                .map(this::ensureIndexedAndResolve)
                .filter(folder -> folder != null)
                .flatMap(folder -> discoverFromFolder(folder.getId(), recursive, types));
    }

    /**
     * Discovers nodes using configured sources.
     *
     * @return stream of nodes matching source filters and exclusion rules
     */
    public Stream<Node> discoverFromConfig() {
        return props.getSources().stream()
                .flatMap(source -> {
                    Node folder = ensureIndexedAndResolve(source.getFolder());
                    if (folder == null) {
                        return Stream.empty();
                    }
                    return discoverFromFolder(folder.getId(), source.isRecursive(), source.getTypes());
                });
    }

    /**
     * Fetches the folder, ensures it has {@code cl:indexed}, and returns the resolved
     * {@link Node} with its canonical UUID. Accepts Alfresco aliases such as {@code -root-}.
     */
    private Node ensureIndexedAndResolve(String folderId) {
        Node folder = alfrescoClient.getAlfrescoNode(folderId);
        if (folder == null) {
            log.warn("Folder not found, skipping: {}", folderId);
            return null;
        }
        if (!Boolean.TRUE.equals(folder.isIsFolder())) {
            log.warn("Node {} is not a folder, skipping", folderId);
            return null;
        }

        List<String> aspects = folder.getAspectNames() != null
                ? new ArrayList<>(folder.getAspectNames())
                : new ArrayList<>();

        if (!aspects.contains(ContentLakeScopeResolver.INDEXED_ASPECT)) {
            aspects.add(ContentLakeScopeResolver.INDEXED_ASPECT);
            alfrescoClient.updateNode(folder.getId(), aspects, null);
            scopeResolver.invalidateFolderScope(folder.getId());
            log.info("Added cl:indexed to folder {}", folder.getId());
        }

        return folder;
    }

    private Stream<Node> discoverFromFolder(String folderId, boolean recursive, List<String> types) {
        log.info("Discovering nodes from folder: {}, recursive: {}", folderId, recursive);

        if (recursive) {
            List<Node> nodes = searchService.findDescendantFiles(
                    folderId, scopeResolver.getExcludedAspects());
            return nodes.stream()
                    .filter(node -> matchesType(node, types))
                    .filter(node -> !matchesExcludedPath(node));
        }

        // Non-recursive: single level only, no AFTS needed
        return alfrescoClient.getAllChildren(folderId).stream()
                .filter(node -> Boolean.FALSE.equals(node.isIsFolder()))
                .filter(node -> matchesType(node, types))
                .filter(node -> scopeResolver.isInScope(node));
    }

    private boolean matchesExcludedPath(Node node) {
        String path = node.getPath() != null ? node.getPath().getName() : null;
        return scopeResolver.matchesExcludedPath(path);
    }

    private boolean matchesType(Node node, List<String> types) {
        return types == null || types.isEmpty() || types.contains(node.getNodeType());
    }
}
