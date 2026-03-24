package org.alfresco.contentlake.model;

public record ContentLakeNodeStatus(
        String nodeId,
        Status status,
        boolean exists,
        boolean folder,
        boolean inScope,
        boolean excluded,
        String error,
        FolderStatusSummary folderSummary
) {

    public ContentLakeNodeStatus(
            String nodeId,
            Status status,
            boolean exists,
            boolean folder,
            boolean inScope,
            boolean excluded,
            String error
    ) {
        this(nodeId, status, exists, folder, inScope, excluded, error, null);
    }

    public enum Status {
        PENDING,
        INDEXED,
        FAILED
    }

    public record FolderStatusSummary(
            long totalDocuments,
            long indexedDocuments,
            long pendingDocuments,
            long failedDocuments
    ) {
    }
}
