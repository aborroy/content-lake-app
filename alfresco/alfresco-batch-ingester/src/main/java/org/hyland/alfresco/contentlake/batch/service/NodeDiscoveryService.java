package org.hyland.alfresco.contentlake.batch.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.hyland.alfresco.contentlake.batch.config.IngestionProperties;
import org.hyland.alfresco.contentlake.batch.model.BatchSyncRequest;
import org.hyland.alfresco.contentlake.client.AlfrescoClient;
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
                .peek(this::ensureIndexed)
                .flatMap(folderId -> discoverFromFolder(folderId, recursive, types));
    }

    /**
     * Discovers nodes using configured sources.
     *
     * @return stream of nodes matching source filters and exclusion rules
     */
    public Stream<Node> discoverFromConfig() {
        return props.getSources().stream()
                .peek(source -> ensureIndexed(source.getFolder()))
                .flatMap(source -> discoverFromFolder(
                        source.getFolder(),
                        source.isRecursive(),
                        source.getTypes()
                ));
    }

    /**
     * Ensures the given folder has the {@code cl:indexed} aspect, adding it if absent.
     * This is idempotent — calling it on an already-indexed folder is a no-op.
     */
    private void ensureIndexed(String folderId) {
        Node folder = alfrescoClient.getAlfrescoNode(folderId);
        if (folder == null) {
            log.warn("Folder not found, skipping scope check: {}", folderId);
            return;
        }
        if (!Boolean.TRUE.equals(folder.isIsFolder())) {
            log.warn("Node {} is not a folder, skipping scope check", folderId);
            return;
        }

        List<String> aspects = folder.getAspectNames() != null
                ? new ArrayList<>(folder.getAspectNames())
                : new ArrayList<>();

        if (aspects.contains(ContentLakeScopeResolver.INDEXED_ASPECT)) {
            return;
        }

        aspects.add(ContentLakeScopeResolver.INDEXED_ASPECT);
        alfrescoClient.updateNode(folderId, aspects, null);
        scopeResolver.invalidateFolderScope(folderId);
        log.info("Added cl:indexed to folder {}", folderId);
    }

    private Stream<Node> discoverFromFolder(String folderId, boolean recursive, List<String> types) {
        log.info("Discovering nodes from folder: {}, recursive: {}", folderId, recursive);

        return alfrescoClient.getAllChildren(folderId).stream()
                .flatMap(node -> toCandidateStream(node, recursive, types));
    }

    private Stream<Node> toCandidateStream(Node node, boolean recursive, List<String> types) {
        if (Boolean.TRUE.equals(node.isIsFolder()) && recursive) {
            if (!scopeResolver.shouldTraverse(node)) {
                return Stream.empty();
            }
            if (scopeResolver.isExcludedBySelfOrAncestor(node)) {
                log.debug("Skipping excluded folder subtree {}", node.getId());
                return Stream.empty();
            }
            return discoverFromFolder(node.getId(), true, types);
        }

        if (Boolean.FALSE.equals(node.isIsFolder())
                && matchesType(node, types)
                && scopeResolver.isInScope(node)) {
            return Stream.of(node);
        }

        return Stream.empty();
    }

    private boolean matchesType(Node node, List<String> types) {
        return types == null || types.isEmpty() || types.contains(node.getNodeType());
    }
}
