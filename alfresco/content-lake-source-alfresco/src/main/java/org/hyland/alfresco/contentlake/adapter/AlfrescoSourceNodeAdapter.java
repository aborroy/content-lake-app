package org.hyland.contentlake.adapter;

import org.hyland.contentlake.model.ContentLakeIngestProperties;
import org.hyland.contentlake.spi.SourceNode;
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
                Set.of(),
                buildSourceProperties(node, sourceId)
        );
    }

    private static Map<String, Object> buildSourceProperties(Node node, String sourceId) {
        String path     = node.getPath()    != null ? node.getPath().getName()       : null;
        String mimeType = node.getContent() != null ? node.getContent().getMimeType() : null;
        String modified = node.getModifiedAt() != null ? node.getModifiedAt().toString() : null;

        Map<String, Object> props = new LinkedHashMap<>();

        // Generic source-agnostic keys (readable by any source-unaware consumer)
        props.put(ContentLakeIngestProperties.SOURCE_NODE_ID,     node.getId());
        props.put(ContentLakeIngestProperties.SOURCE_TYPE,        "alfresco");
        props.put(ContentLakeIngestProperties.SOURCE_NAME,        node.getName());
        props.put(ContentLakeIngestProperties.SOURCE_PATH,        path);
        props.put(ContentLakeIngestProperties.SOURCE_MIME_TYPE,   mimeType);
        props.put(ContentLakeIngestProperties.SOURCE_MODIFIED_AT, modified);

        // Alfresco-specific keys (preserved for adapter-aware consumers)
        props.put(ContentLakeIngestProperties.ALFRESCO_NODE_ID,       node.getId());
        props.put(ContentLakeIngestProperties.ALFRESCO_REPOSITORY_ID, sourceId);
        props.put(ContentLakeIngestProperties.ALFRESCO_NAME,          node.getName());
        props.put(ContentLakeIngestProperties.ALFRESCO_PATH,          path);
        props.put(ContentLakeIngestProperties.ALFRESCO_MIME_TYPE,     mimeType);
        props.put(ContentLakeIngestProperties.ALFRESCO_MODIFIED_AT,   modified);

        props.values().removeIf(Objects::isNull);
        return props;
    }
}
