package org.alfresco.contentlake.adapter;

import org.alfresco.contentlake.model.ContentLakeIngestProperties;
import org.alfresco.contentlake.spi.SourceNode;
import org.alfresco.core.model.Node;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Converts an Alfresco SDK {@link Node} into a {@link SourceNode}.
 *
 * <p>This adapter lives in {@code content-lake-common} (the Alfresco-specific layer)
 * and must be called at the Alfresco boundary — in the batch/live ingesters —
 * <em>before</em> the node is handed to the shared sync pipeline.</p>
 *
 * <p>The {@code sourceProperties} map is populated with the {@code alfresco_*} keys
 * defined in {@link ContentLakeIngestProperties} so that the pipeline can store them
 * in {@code cin_ingestProperties} without knowing anything about Alfresco.</p>
 */
public final class AlfrescoSourceNodeAdapter {

    private AlfrescoSourceNodeAdapter() {}

    /**
     * Converts an Alfresco {@link Node} to a {@link SourceNode}.
     *
     * @param node           Alfresco node (must include properties, path, and permissions)
     * @param sourceId       identifier of the Alfresco repository instance
     * @param readPrincipals authority identifiers that have read access on this node
     * @return source-agnostic representation of the node
     */
    public static SourceNode toSourceNode(Node node, String sourceId, Set<String> readPrincipals) {
        return new SourceNode(
                node.getId(),
                sourceId,
                "alfresco",
                node.getName(),
                node.getPath() != null ? node.getPath().getName() : null,
                node.getContent() != null ? node.getContent().getMimeType() : null,
                node.getModifiedAt(),
                Boolean.TRUE.equals(node.isIsFolder()),
                readPrincipals,
                buildSourceProperties(node, sourceId)
        );
    }

    private static Map<String, Object> buildSourceProperties(Node node, String sourceId) {
        Map<String, Object> props = new LinkedHashMap<>();
        props.put(ContentLakeIngestProperties.ALFRESCO_NODE_ID,       node.getId());
        props.put(ContentLakeIngestProperties.ALFRESCO_REPOSITORY_ID, sourceId);
        props.put(ContentLakeIngestProperties.ALFRESCO_NAME,          node.getName());
        props.put(ContentLakeIngestProperties.ALFRESCO_PATH,
                node.getPath() != null ? node.getPath().getName() : null);
        props.put(ContentLakeIngestProperties.ALFRESCO_MIME_TYPE,
                node.getContent() != null ? node.getContent().getMimeType() : null);
        props.put(ContentLakeIngestProperties.ALFRESCO_MODIFIED_AT,
                node.getModifiedAt() != null ? node.getModifiedAt().toString() : null);
        props.values().removeIf(Objects::isNull);
        return props;
    }
}
