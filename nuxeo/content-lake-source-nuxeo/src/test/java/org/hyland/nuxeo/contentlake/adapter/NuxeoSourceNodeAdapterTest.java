package org.hyland.nuxeo.contentlake.adapter;

import org.hyland.contentlake.model.ContentLakeIngestProperties;
import org.hyland.nuxeo.contentlake.model.NuxeoDocument;
import org.hyland.contentlake.spi.PermissionRule;
import org.hyland.contentlake.spi.SourceNode;
import org.junit.jupiter.api.Test;

import java.time.OffsetDateTime;
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

    @Test
    void toSourceNode_populatesStructuredSecurityConfigFromReadAndDenyPrincipals() {
        NuxeoDocument document = new NuxeoDocument();
        document.setUid("doc-123");
        document.setType("File");
        document.setTitle("Quarterly Report");
        document.setPath("/default-domain/workspaces/finance/q1-report.pdf");

        SourceNode node = NuxeoSourceNodeAdapter.toSourceNode(
                document,
                "nuxeo-dev",
                "file:content",
                Set.of("Administrator", "GROUP_members"),
                Set.of("GROUP_archived")
        );

        assertThat(node.security()).isNotNull();
        assertThat(node.security().inheritanceEnabled()).isTrue();
        assertThat(node.security().permissions())
                .containsExactlyInAnyOrder(
                        new PermissionRule("Administrator", "user", "Administrator", "READ"),
                        new PermissionRule("GROUP_members", "group", "GROUP_members", "READ"),
                        new PermissionRule("GROUP_archived", "group", "GROUP_archived", "READ_DENY"));
    }

    /**
     * The generic timestamp is core's to write, not this adapter's (#149).
     *
     * <p>Same defect as the Alfresco adapter had, from the same cause: {@code source_modifiedAt} is
     * compared as text by the {@code modifiedAfter} and {@code modifiedBefore} range predicates, and
     * {@code OffsetDateTime.toString()} elides zero seconds, so a document modified on a whole second was
     * stored in a form that sorts after any bound carrying seconds. Core seeds the key from the record in a
     * fixed-width form, so this adapter supplies nothing.</p>
     *
     * <p>There is no {@code nuxeo_modifiedAt} vendor key to keep the raw form in, and nothing wanted one:
     * {@code dc:modified} is already in the document's own properties if a consumer needs what Nuxeo
     * reported.</p>
     */
    @Test
    void toSourceNode_leavesTheGenericTimestampToCoreSoItIsFixedWidth() {
        NuxeoDocument document = new NuxeoDocument();
        document.setUid("doc-456");
        document.setType("File");
        document.setTitle("On A Whole Second");
        document.setPath("/default-domain/workspaces/finance/whole-second.pdf");
        document.setState("project");
        document.setProperties(Map.of(
                "dc:modified", "2026-03-24T09:15:00Z",
                "file:content", Map.of("mime-type", "application/pdf")
        ));

        SourceNode node = NuxeoSourceNodeAdapter.toSourceNode(
                document, "nuxeo-prod", "file:content", Set.of("Everyone"), Set.of());

        // The record carries it, which is what core formats.
        assertThat(node.modifiedAt()).isEqualTo(OffsetDateTime.parse("2026-03-24T09:15:00Z"));
        assertThat(node.sourceProperties())
                .doesNotContainKey(ContentLakeIngestProperties.SOURCE_MODIFIED_AT);
    }
}
