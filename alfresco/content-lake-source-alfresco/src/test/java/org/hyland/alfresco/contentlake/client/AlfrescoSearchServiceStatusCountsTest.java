package org.hyland.alfresco.contentlake.client;

import org.alfresco.core.model.Node;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Folder status aggregation in {@link AlfrescoSearchService}.
 *
 * The counts come from the {@code cl:syncStatusValue} property carried by each descendant
 * node, so they stay correct on a search engine that does not index custom-model fields
 * (the ACS 26.2+ OpenSearch batch indexer drops any property whose namespace prefix it
 * cannot resolve, which made a facet-based aggregation report every document as pending).
 */
class AlfrescoSearchServiceStatusCountsTest {

    @Test
    void countsIndexedFailedSkippedAndPendingFromNodeProperties() {
        StubSearchService searchService = new StubSearchService(List.of(
                fileWithProperties("doc-1", Map.of("cl:syncStatusValue", "INDEXED")),
                fileWithProperties("doc-2", Map.of("cl:syncStatusValue", "INDEXED")),
                fileWithProperties("doc-3", Map.of("cl:syncStatusValue", "FAILED")),
                fileWithProperties("doc-4", Map.of("cl:syncStatusValue", "PENDING")),
                fileWithProperties("doc-5", Map.of()),
                fileWithProperties("doc-6", Map.of("cl:syncStatusValue", "SKIPPED"))
        ));

        FolderStatusCounts counts = searchService.getFolderStatusCounts("folder-1", List.of());

        assertThat(counts.total()).isEqualTo(6);
        assertThat(counts.indexed()).isEqualTo(2);
        assertThat(counts.failed()).isEqualTo(1);
        assertThat(counts.skipped()).isEqualTo(1);
        // Derived, so a skip must not be double-counted as pending: doc-4 and doc-5 only.
        assertThat(counts.pending()).isEqualTo(2);
    }

    @Test
    void skipsDocumentsExcludedFromLake() {
        StubSearchService searchService = new StubSearchService(List.of(
                fileWithProperties("doc-1", Map.of("cl:syncStatusValue", "INDEXED")),
                fileWithProperties("doc-2", Map.of("cl:excludeFromLake", true)),
                fileWithProperties("doc-3", Map.of("cl:excludeFromLake", "true"))
        ));

        FolderStatusCounts counts = searchService.getFolderStatusCounts("folder-1", List.of());

        assertThat(counts.total()).isEqualTo(1);
        assertThat(counts.indexed()).isEqualTo(1);
        assertThat(counts.pending()).isZero();
    }

    @Test
    void returnsZerosForAnEmptyFolder() {
        FolderStatusCounts counts = new StubSearchService(List.of())
                .getFolderStatusCounts("folder-1", List.of());

        assertThat(counts.total()).isZero();
        assertThat(counts.indexed()).isZero();
        assertThat(counts.failed()).isZero();
        assertThat(counts.skipped()).isZero();
        assertThat(counts.pending()).isZero();
    }

    private static Node fileWithProperties(String nodeId, Map<String, Object> properties) {
        return new Node()
                .id(nodeId)
                .isFile(true)
                .isFolder(false)
                .properties(properties);
    }

    private static final class StubSearchService extends AlfrescoSearchService {
        private final List<Node> descendants;

        private StubSearchService(List<Node> descendants) {
            super(null, null, null);
            this.descendants = descendants;
        }

        @Override
        public List<Node> findDescendantFiles(String folderId, Collection<String> excludedAspects) {
            return descendants;
        }
    }
}
