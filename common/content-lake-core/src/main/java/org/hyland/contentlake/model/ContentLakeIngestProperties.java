package org.hyland.contentlake.model;

public final class ContentLakeIngestProperties {

    // Generic source-agnostic keys — written for every source type
    public static final String SOURCE_NODE_ID     = "source_nodeId";
    public static final String SOURCE_TYPE        = "source_type";
    public static final String SOURCE_PATH        = "source_path";
    public static final String SOURCE_NAME        = "source_name";
    public static final String SOURCE_MIME_TYPE   = "source_mimeType";
    public static final String SOURCE_MODIFIED_AT = "source_modifiedAt";

    // Alfresco-specific keys — written by the Alfresco adapter alongside the generic ones
    public static final String ALFRESCO_NODE_ID = "alfresco_nodeId";
    public static final String ALFRESCO_REPOSITORY_ID = "alfresco_repositoryId";
    public static final String ALFRESCO_PATH = "alfresco_path";
    public static final String ALFRESCO_NAME = "alfresco_name";
    public static final String ALFRESCO_MIME_TYPE = "alfresco_mimeType";
    public static final String ALFRESCO_MODIFIED_AT = "alfresco_modifiedAt";

    // Nuxeo-specific keys — written by the Nuxeo adapter alongside the generic ones
    public static final String NUXEO_PATH = "nuxeo_path";
    public static final String NUXEO_DOCUMENT_TYPE = "nuxeo_documentType";
    public static final String NUXEO_LIFECYCLE_STATE = "nuxeo_lifecycleState";
    public static final String NUXEO_BLOB_XPATH = "nuxeo_blobXpath";
    public static final String NUXEO_FACETS = "nuxeo_facets";
    public static final String NUXEO_EXCLUDE_FROM_SCOPE = "nuxeo_excludeFromScope";

    public static final String CONTENT_LAKE_SYNC_STATUS = "contentLake_syncStatus";
    public static final String CONTENT_LAKE_SYNC_ERROR = "contentLake_syncError";

    private ContentLakeIngestProperties() {
    }
}
