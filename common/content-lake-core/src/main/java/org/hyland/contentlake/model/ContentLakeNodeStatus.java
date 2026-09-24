package org.hyland.contentlake.model;

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
        FAILED,
        /**
         * Nothing was attempted, because the document's type cannot contain text. Distinct from
         * {@code FAILED}, which means a pass ran and produced no usable text.
         *
         * <p>Scope exclusion deliberately has no status of its own: it is carried by the
         * {@code inScope} and {@code excluded} fields of the enclosing record.</p>
         */
        SKIPPED
    }

    public record FolderStatusSummary(
            long totalDocuments,
            long indexedDocuments,
            long pendingDocuments,
            long failedDocuments,
            long skippedDocuments
    ) {
    }
}
