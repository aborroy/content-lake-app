package org.hyland.filesystem.contentlake.batch.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.hyland.contentlake.service.DiscoveryOutcome;
import org.hyland.contentlake.spi.SourceNode;
import org.hyland.filesystem.contentlake.client.FileSystemSourceClient;
import org.hyland.filesystem.contentlake.config.FileSystemProperties;
import org.hyland.filesystem.contentlake.service.FileSystemScopeResolver;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

/**
 * Recursively walks the configured filesystem root, returning in-scope file {@link SourceNode}s.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class FileSystemDiscoveryService {

    private final FileSystemSourceClient client;
    private final FileSystemScopeResolver scopeResolver;
    private final FileSystemProperties properties;

    /**
     * A discovery pass and its own account of whether it covered its whole scope.
     *
     * @param nodes   the discovered nodes
     * @param outcome whether the configured root was walked in full, and the root covered
     */
    public record FileSystemDiscovery(List<SourceNode> nodes, DiscoveryOutcome outcome) {
    }

    public List<SourceNode> discover() {
        return discoverTallied().nodes();
    }

    /**
     * As {@link #discover}, but also reporting the pass's completeness.
     *
     * <p>Complete on a normal return, and each of the three ways it could fail to be propagates rather
     * than being swallowed: an unreadable or missing root makes {@code client.getNode} throw, a directory
     * that will not list makes {@code client.getChildren} throw, and an entry that is present but whose
     * attributes will not read does the same. The walk pages until it gets an empty page, so a directory
     * whose entry count is not a multiple of the page size is not cut short either.</p>
     */
    public FileSystemDiscovery discoverTallied() {
        List<SourceNode> discovered = new ArrayList<>();
        String rootNodeId = client.getRootNodeId();
        SourceNode root = client.getNode(rootNodeId);
        collect(root, discovered);
        log.info("Filesystem discovery from {} found {} in-scope files", rootNodeId, discovered.size());
        return new FileSystemDiscovery(discovered,
                DiscoveryOutcome.complete(List.of(root.path() != null ? root.path() : rootNodeId)));
    }

    private void collect(SourceNode node, List<SourceNode> discovered) {
        if (!node.folder()) {
            if (scopeResolver.isInScope(node)) {
                discovered.add(node);
            }
            return;
        }
        if (!scopeResolver.shouldTraverse(node)) {
            return;
        }
        int skip = 0;
        int pageSize = properties.getPageSize();
        while (true) {
            List<SourceNode> children = client.getChildren(node.nodeId(), skip, pageSize);
            if (children.isEmpty()) {
                break;
            }
            for (SourceNode child : children) {
                collect(child, discovered);
            }
            // Only an empty page ends a directory, and the cursor advances by what was asked for. A short
            // page is not exhaustion: the client applies the page window before converting entries.
            skip += pageSize;
        }
    }
}
