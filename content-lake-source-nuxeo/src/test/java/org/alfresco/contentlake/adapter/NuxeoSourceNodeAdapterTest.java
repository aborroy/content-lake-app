package org.alfresco.contentlake.adapter;

import org.alfresco.contentlake.model.NuxeoDocument;
import org.alfresco.contentlake.spi.SourceNode;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class NuxeoSourceNodeAdapterTest {

    @Test
    void toSourceNode_filePreservesFullPathAndUsesParentPathForSync() {
        NuxeoDocument document = new NuxeoDocument();
        document.setUid("doc-123");
        document.setType("File");
        document.setTitle("Quarterly Report");
        document.setPath("/default-domain/workspaces/finance/q1-report.pdf");
        document.setState("project");
        document.setProperties(Map.of(
                "dc:modified", "2026-03-24T09:15:30Z",
                "file:content", Map.of("mime-type", "application/pdf")
        ));

        SourceNode node = NuxeoSourceNodeAdapter.toSourceNode(
                document,
                "nuxeo-dev",
                "file:content",
                Set.of("Administrator", "GROUP_members"),
                Set.of("GROUP_archived")
        );

        assertThat(node.nodeId()).isEqualTo("doc-123");
        assertThat(node.sourceType()).isEqualTo("nuxeo");
        assertThat(node.sourceId()).isEqualTo("nuxeo-dev");
        assertThat(node.name()).isEqualTo("Quarterly Report");
        assertThat(node.path()).isEqualTo("/default-domain/workspaces/finance");
        assertThat(node.mimeType()).isEqualTo("application/pdf");
        assertThat(node.folder()).isFalse();
        assertThat(node.readPrincipals()).containsExactlyInAnyOrder("Administrator", "GROUP_members");
        assertThat(node.denyPrincipals()).containsExactly("GROUP_archived");
        assertThat(node.sourceProperties())
                .containsEntry("source_nodeId", "doc-123")
                .containsEntry("source_type", "nuxeo")
                .containsEntry("source_path", "/default-domain/workspaces/finance")
                .containsEntry("nuxeo_path", "/default-domain/workspaces/finance/q1-report.pdf")
                .containsEntry("nuxeo_documentType", "File")
                .containsEntry("nuxeo_lifecycleState", "project")
                .containsEntry("nuxeo_blobXpath", "file:content");
    }

    @Test
    void toSourceNode_folderKeepsItsTraversalPath() {
        NuxeoDocument document = new NuxeoDocument();
        document.setUid("folder-1");
        document.setType("Workspace");
        document.setTitle("Finance Workspace");
        document.setPath("/default-domain/workspaces/finance");
        document.setState("project");

        SourceNode node = NuxeoSourceNodeAdapter.toSourceNode(
                document,
                "nuxeo-dev",
                "file:content",
                Set.of("GROUP_members"),
                Set.of()
        );

        assertThat(node.folder()).isTrue();
        assertThat(node.path()).isEqualTo("/default-domain/workspaces/finance");
        assertThat(node.mimeType()).isNull();
        assertThat(node.sourceProperties())
                .containsEntry("nuxeo_path", "/default-domain/workspaces/finance")
                .containsEntry("nuxeo_documentType", "Workspace");
    }
}
