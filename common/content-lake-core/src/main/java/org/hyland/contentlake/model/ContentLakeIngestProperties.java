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

    /**
     * Extracted document text, mirrored here so the keyword leg of hybrid search can reach it.
     *
     * <p>{@code sys_fulltextBinary} is where extracted text naturally belongs and the sync writes it
     * there too, but hxpr does not expose that field to HXQL, so a query against it matches nothing.
     * hxpr does fold {@code cin_ingestProperties} into its analysed {@code sys_fulltext} index, so
     * populating this key is what makes document body text searchable by term at all.</p>
     *
     * <p>Query {@code sys_fulltext}, not this property directly: the property's own index truncates
     * at 256 characters, so {@code cin_ingestProperties.contentLake_extractedText LIKE '%term%'}
     * silently only ever matches a document's opening lines.</p>
     */
    public static final String CONTENT_LAKE_EXTRACTED_TEXT = "contentLake_extractedText";

    /**
     * Per-document section map (JSON) supporting small-to-big / parent-child retrieval.
     *
     * <p>Chunks are stored flat as rows in a single Parquet embeddings child, with no section-level
     * hxpr node. This property records, per chunk index, the section it came from and that section's
     * text, so retrieval can expand a matched small chunk back out to its parent section in-process
     * without walking an ancestors API or re-reading the Parquet file.</p>
     */
    public static final String CONTENT_LAKE_SECTION_MAP = "contentLake_sectionMap";

    /**
     * Fingerprint of everything that determines the document's stored chunks and vectors.
     *
     * <p>Held here rather than in a separate state store so there is no second source of truth to
     * reconcile: the fingerprint travels with the document it describes, and is visible to anything
     * that can read the document.</p>
     *
     * <p>A sync recomputes it after extraction and, on a match, skips chunking and embedding. It
     * covers the embedding type and the chunking parameters as well as the text, so changing either
     * forces a reprocess instead of stranding the corpus on vectors the new configuration would not
     * produce. See {@code ContentFingerprint}.</p>
     */
    public static final String CONTENT_LAKE_CONTENT_FINGERPRINT = "contentLake_contentFingerprint";

    private ContentLakeIngestProperties() {
    }
}
