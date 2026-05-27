package org.hyland.alfresco.contentlake.client;

/**
 * Aggregate counts of Content Lake sync statuses for all in-scope files under a folder.
 *
 * <p>Derived from a single AFTS facet query; files without {@code cl:syncStatus}
 * are treated as PENDING.</p>
 */
public record FolderStatusCounts(long total, long indexed, long failed) {

    public long pending() {
        return Math.max(0, total - indexed - failed);
    }
}
