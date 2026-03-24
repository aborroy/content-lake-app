package org.alfresco.contentlake.service;

import org.alfresco.contentlake.spi.SourceNode;
import org.junit.jupiter.api.Test;

import java.time.OffsetDateTime;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class NuxeoScopeResolverTest {

    private final NuxeoScopeResolver resolver = new NuxeoScopeResolver(
            Set.of("/default-domain/workspaces"),
            Set.of("File", "Note")
    );

    @Test
    void isInScope_acceptsIncludedDocumentInsideConfiguredRoot() {
        SourceNode node = fileNode(
                "/default-domain/workspaces/finance/report.pdf",
                "File",
                "project"
        );

        assertThat(resolver.isInScope(node)).isTrue();
    }

    @Test
    void isInScope_rejectsDeletedDocumentsAndWrongTypes() {
        SourceNode deleted = fileNode(
                "/default-domain/workspaces/finance/report.pdf",
                "File",
                "deleted"
        );
        SourceNode wrongType = fileNode(
                "/default-domain/workspaces/finance/note.txt",
                "Picture",
                "project"
        );

        assertThat(resolver.isInScope(deleted)).isFalse();
        assertThat(resolver.isInScope(wrongType)).isFalse();
    }

    @Test
    void shouldTraverse_allowsFolderWithinIncludedRoots() {
        SourceNode folder = folderNode(
                "/default-domain/workspaces/finance",
                "Workspace",
                "project"
        );
        SourceNode outsideRoot = folderNode(
                "/default-domain/sections/archive",
                "Workspace",
                "project"
        );

        assertThat(resolver.shouldTraverse(folder)).isTrue();
        assertThat(resolver.shouldTraverse(outsideRoot)).isFalse();
    }

    private static SourceNode fileNode(String fullPath, String type, String lifecycleState) {
        return new SourceNode(
                "doc-123",
                "nuxeo-dev",
                "nuxeo",
                "Document",
                "/default-domain/workspaces/finance",
                "application/pdf",
                OffsetDateTime.parse("2026-03-24T09:15:30Z"),
                false,
                Set.of(),
                Map.of(
                        "nuxeo_path", fullPath,
                        "nuxeo_documentType", type,
                        "nuxeo_lifecycleState", lifecycleState
                )
        );
    }

    private static SourceNode folderNode(String fullPath, String type, String lifecycleState) {
        return new SourceNode(
                "folder-123",
                "nuxeo-dev",
                "nuxeo",
                "Folder",
                fullPath,
                null,
                OffsetDateTime.parse("2026-03-24T09:15:30Z"),
                true,
                Set.of(),
                Map.of(
                        "nuxeo_path", fullPath,
                        "nuxeo_documentType", type,
                        "nuxeo_lifecycleState", lifecycleState
                )
        );
    }
}
