package org.alfresco.contentlake.batch.service;

import lombok.extern.slf4j.Slf4j;
import org.alfresco.contentlake.adapter.AlfrescoSourceNodeAdapter;
import org.alfresco.contentlake.client.AlfrescoClient;
import org.alfresco.contentlake.batch.model.TransformationTask;
import org.alfresco.contentlake.service.NodeSyncService;
import org.alfresco.contentlake.spi.SourceNode;
import org.alfresco.core.model.Node;
import org.springframework.stereotype.Service;

import java.util.Set;

@Slf4j
@Service
public class MetadataIngester {

    private final NodeSyncService nodeSyncService;
    private final AlfrescoClient alfrescoClient;

    public MetadataIngester(NodeSyncService nodeSyncService, AlfrescoClient alfrescoClient) {
        this.nodeSyncService = nodeSyncService;
        this.alfrescoClient = alfrescoClient;
    }

    public TransformationTask ingestMetadata(Node node) {
        log.debug("Ingesting metadata for node: {} ({})", node.getName(), node.getId());

        Set<String> readers = alfrescoClient.extractReadAuthorities(node);
        SourceNode sourceNode = AlfrescoSourceNodeAdapter.toSourceNode(
                node, alfrescoClient.getSourceId(), readers);

        NodeSyncService.SyncResult result = nodeSyncService.ingestMetadata(sourceNode);
        if (result.skipped()) {
            log.debug("Skipping transformation enqueue for node {} — Content Lake version is already current", node.getId());
            return null;
        }

        return new TransformationTask(
                result.nodeId(),
                result.hxprDocId(),
                result.mimeType(),
                result.documentName(),
                result.documentPath(),
                result.ingestProperties()
        );
    }
}
