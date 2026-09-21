package org.hyland.alfresco.contentlake.adapter;

import org.alfresco.core.model.Node;
import org.alfresco.core.model.PermissionsInfo;
import org.hyland.contentlake.model.ContentLakeIngestProperties;
import org.hyland.contentlake.spi.PermissionRule;
import org.hyland.contentlake.spi.SourceNode;
import org.junit.jupiter.api.Test;

import java.time.OffsetDateTime;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class AlfrescoSourceNodeAdapterTest {

    @Test
    void toSourceNode_populatesStructuredSecurityConfigWithUserAndGroupRules() {
        Node node = new Node()
                .id("node-1")
                .name("report.pdf")
                .isFolder(false)
                .permissions(new PermissionsInfo().isInheritanceEnabled(true));

        SourceNode result = AlfrescoSourceNodeAdapter.toSourceNode(
                node, "alfresco-repo", Set.of("user-a", "GROUP_engineering"));

        assertThat(result.security()).isNotNull();
        assertThat(result.security().inheritanceEnabled()).isTrue();
        assertThat(result.security().permissions())
                .containsExactlyInAnyOrder(
                        new PermissionRule("user-a", "user", "user-a", "READ"),
                        new PermissionRule("GROUP_engineering", "group", "GROUP_engineering", "READ"));
    }

    @Test
    void toSourceNode_reportsInheritanceDisabledFromNodePermissions() {
        Node node = new Node()
                .id("node-2")
                .name("secret.txt")
                .isFolder(false)
                .permissions(new PermissionsInfo().isInheritanceEnabled(false));

        SourceNode result = AlfrescoSourceNodeAdapter.toSourceNode(
                node, "alfresco-repo", Set.of("user-a"));

        assertThat(result.security().inheritanceEnabled()).isFalse();
    }

    @Test
    void toSourceNode_treatsMissingPermissionBlockAsInheritanceEnabled() {
        Node node = new Node().id("node-3").name("plain.txt").isFolder(false);

        SourceNode result = AlfrescoSourceNodeAdapter.toSourceNode(
                node, "alfresco-repo", Set.of());

        assertThat(result.security()).isNotNull();
        assertThat(result.security().inheritanceEnabled()).isTrue();
        assertThat(result.security().permissions()).isEmpty();
    }

    /**
     * The generic timestamp is core's to write, not this adapter's (#149).
     *
     * <p>{@code source_modifiedAt} is compared as text by the {@code modifiedAfter} and
     * {@code modifiedBefore} range predicates, so it has to be fixed-width, and
     * {@code OffsetDateTime.toString()} is not: it elides zero seconds, so a document modified at exactly
     * a whole second stored as {@code 2026-09-17T10:00Z}, whose {@code Z} sorts after the {@code :} of any
     * bound carrying seconds. Such a document was excluded from ranges it plainly fell in.</p>
     *
     * <p>Core seeds the key from the record in a fixed-width form, so the fix is for this adapter to stop
     * supplying its own. The record has always carried the value; only the duplicate was wrong.</p>
     */
    @Test
    void toSourceNode_leavesTheGenericTimestampToCoreSoItIsFixedWidth() {
        OffsetDateTime onAWholeSecond = OffsetDateTime.parse("2026-09-17T10:00:00Z");
        Node node = new Node()
                .id("node-4")
                .name("quarterly-review.txt")
                .isFolder(false)
                .modifiedAt(onAWholeSecond);

        SourceNode result = AlfrescoSourceNodeAdapter.toSourceNode(node, "alfresco-repo", Set.of());

        // The record carries it, which is what core formats.
        assertThat(result.modifiedAt()).isEqualTo(onAWholeSecond);
        assertThat(result.sourceProperties())
                .doesNotContainKey(ContentLakeIngestProperties.SOURCE_MODIFIED_AT);
    }

    /**
     * The Alfresco-specific copy stays the raw source form, and must never be used in a range predicate.
     *
     * <p>It is kept for two reasons: adapter-aware consumers read the vendor keys expecting what Alfresco
     * reported, and {@code NodeSyncService.getStoredModifiedAt} falls back to it for documents ingested
     * before the generic key existed. Both parse the value rather than comparing it as text, so its width
     * does not matter there.</p>
     */
    @Test
    void toSourceNode_keepsTheAlfrescoTimestampAsTheRawSourceForm() {
        Node node = new Node()
                .id("node-5")
                .name("quarterly-review.txt")
                .isFolder(false)
                .modifiedAt(OffsetDateTime.parse("2026-09-17T10:00:00Z"));

        SourceNode result = AlfrescoSourceNodeAdapter.toSourceNode(node, "alfresco-repo", Set.of());

        assertThat(result.sourceProperties())
                .containsEntry(ContentLakeIngestProperties.ALFRESCO_MODIFIED_AT, "2026-09-17T10:00Z");
    }
}
