package org.alfresco.contentlake.adapter;

import org.alfresco.contentlake.model.ContentLakeIngestProperties;
import org.alfresco.contentlake.model.NuxeoDocument;
import org.alfresco.contentlake.spi.SourceNode;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Converts a Nuxeo REST document payload into a source-agnostic {@link SourceNode}.
 *
 * <p>For file-like documents the shared sync pipeline still expects the parent path,
 * not the full document path, when constructing the Content Lake target path.
 * The adapter therefore stores the full Nuxeo repository path in
 * {@code nuxeo_path} while exposing the parent path through {@link SourceNode#path()}.
 * Folder-like documents keep their full path in both places because they are used
 * for traversal, not for content ingestion.</p>
 */
public final class NuxeoSourceNodeAdapter {

    private static final Set<String> CONTAINER_TYPES = Set.of(
            "Domain", "WorkspaceRoot", "Workspace", "Folder", "OrderedFolder", "Section", "Root"
    );

    private NuxeoSourceNodeAdapter() {}

    public static SourceNode toSourceNode(NuxeoDocument document, String sourceId, String blobXpath) {
        boolean folder = isContainerType(document.getType());
        String fullPath = document.getPath();
        String nodePath = folder ? fullPath : document.getParentPath();
        String mimeType = folder ? null : document.getBlobMimeType(blobXpath);
        String modifiedAt = document.getModifiedAt() != null ? document.getModifiedAt().toString() : null;

        Map<String, Object> props = new LinkedHashMap<>();
        props.put(ContentLakeIngestProperties.SOURCE_NODE_ID, document.getUid());
        props.put(ContentLakeIngestProperties.SOURCE_TYPE, "nuxeo");
        props.put(ContentLakeIngestProperties.SOURCE_PATH, nodePath);
        props.put(ContentLakeIngestProperties.SOURCE_NAME, document.getDisplayName());
        props.put(ContentLakeIngestProperties.SOURCE_MIME_TYPE, mimeType);
        props.put(ContentLakeIngestProperties.SOURCE_MODIFIED_AT, modifiedAt);
        props.put(ContentLakeIngestProperties.NUXEO_PATH, fullPath);
        props.put(ContentLakeIngestProperties.NUXEO_DOCUMENT_TYPE, document.getType());
        props.put(ContentLakeIngestProperties.NUXEO_LIFECYCLE_STATE, document.getState());
        props.put(ContentLakeIngestProperties.NUXEO_BLOB_XPATH, blobXpath);
        props.values().removeIf(Objects::isNull);

        return new SourceNode(
                document.getUid(),
                sourceId,
                "nuxeo",
                document.getDisplayName(),
                nodePath,
                mimeType,
                document.getModifiedAt(),
                folder,
                Set.of(),
                props
        );
    }

    public static boolean isContainerType(String type) {
        return type != null && CONTAINER_TYPES.contains(type);
    }
}
