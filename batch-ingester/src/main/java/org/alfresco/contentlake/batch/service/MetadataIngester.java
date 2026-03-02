package org.alfresco.contentlake.batch.service;

import lombok.extern.slf4j.Slf4j;
import org.alfresco.contentlake.batch.model.TransformationTask;
import org.alfresco.contentlake.service.NodeSyncService;
import org.alfresco.core.model.Node;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class MetadataIngester {

    private final NodeSyncService nodeSyncService;

    public MetadataIngester(NodeSyncService nodeSyncService) {
        this.nodeSyncService = nodeSyncService;
    }

    public TransformationTask ingestMetadata(Node node) {
        log.debug("Ingesting metadata for node: {} ({})", node.getName(), node.getId());

        NodeSyncService.SyncResult result = nodeSyncService.ingestMetadata(node);
        if (result.skipped()) {
            log.debug("Skipping transformation enqueue for node {} — Content Lake version is already current", node.getId());
            return null;
        }

        return new TransformationTask(
                result.nodeId(),
                result.hxprDocId(),
                result.mimeType(),
                result.documentName(),
                result.documentPath()
        );
    }
}
