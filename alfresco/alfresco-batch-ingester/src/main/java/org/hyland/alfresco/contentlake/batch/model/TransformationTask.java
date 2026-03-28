package org.hyland.alfresco.contentlake.batch.model;

import lombok.Data;

import java.time.Instant;
import java.util.Map;

@Data
public class TransformationTask {

    private String nodeId;
    private String hxprDocumentId;
    private String mimeType;
    private Instant createdAt;
    private int retryCount;

    /** Original document name (e.g. "Annual_Report_2025.pdf"). Used for metadata-enriched embedding. */
    private String documentName;

    /** Repository path (e.g. "/Company Home/Reports"). Used for metadata-enriched embedding. */
    private String documentPath;

    /**
     * {@code cin_ingestProperties} snapshot from the metadata phase.
     * Forwarded to {@code processContent} so the status patch does not need a prior GET.
     */
    private Map<String, Object> ingestProperties;

    public TransformationTask(String nodeId, String hxprDocumentId, String mimeType) {
        this.nodeId = nodeId;
        this.hxprDocumentId = hxprDocumentId;
        this.mimeType = mimeType;
        this.createdAt = Instant.now();
        this.retryCount = 0;
    }

    public TransformationTask(String nodeId, String hxprDocumentId, String mimeType,
                              String documentName, String documentPath,
                              Map<String, Object> ingestProperties) {
        this(nodeId, hxprDocumentId, mimeType);
        this.documentName = documentName;
        this.documentPath = documentPath;
        this.ingestProperties = ingestProperties;
    }
}