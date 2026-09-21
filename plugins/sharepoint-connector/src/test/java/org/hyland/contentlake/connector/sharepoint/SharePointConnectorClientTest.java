package org.hyland.contentlake.connector.sharepoint;

import org.hyland.contentlake.connector.sharepoint.mock.MockGraphServer;
import org.hyland.contentlake.spi.SourceChangePage;
import org.hyland.contentlake.spi.SourceNode;
import org.hyland.contentlake.spi.SourceTombstone;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The whole client against the mock: the closest thing to a real run that needs no tenant and no containers.
 */
class SharePointConnectorClientTest {

    private static final String DRIVE = "b!mock-drive-id";

    private MockGraphServer mock;

    @AfterEach
    void stopMock() {
        if (mock != null) {
            mock.close();
        }
    }

    private SharePointConnectorClient clientFor(MockGraphServer.Options options) throws IOException {
        return clientFor(options, SharePointConnectorSettings.PermissionsMode.PER_ITEM);
    }

    private SharePointConnectorClient clientFor(MockGraphServer.Options options,
                                                SharePointConnectorSettings.PermissionsMode mode)
            throws IOException {
        mock = new MockGraphServer(options);
        SharePointConnectorSettings settings = settings(mock.graphBaseUrl(), mode);
        return new SharePointConnectorClient(settings,
                new GraphHttpClient(mock.graphBaseUrl(), settings.tokenProvider(),
                        ResourceUnitMeter.unmetered()));
    }

    private static SharePointConnectorSettings settings(String graphBaseUrl,
                                                        SharePointConnectorSettings.PermissionsMode mode) {
        return new SharePointConnectorSettings(
                graphBaseUrl,
                SharePointConnectorSettings.AuthMode.STATIC_TOKEN,
                null, null, null, null, null,
                "mock-token",
                List.of(DRIVE),
                null,
                List.of(), List.of(), List.of(), List.of(),
                SharePointAclMapper.AclFallback.FAIL_CLOSED,
                SharePointAclMapper.GroupGrants.MAP,
                mode,
                Set.of(),
                0, 1);
    }

    private static MockGraphServer.Options options() {
        return MockGraphServer.Options.defaults(Path.of("src/test/resources/fixtures"));
    }

    @Test
    void identifiesItselfAndTheDriveItStartsFrom() throws Exception {
        SharePointConnectorClient client = clientFor(options());

        assertThat(client.getSourceType()).isEqualTo("sharepoint");
        // The source id defaults to the drive, so a single-drive run needs no extra setting.
        assertThat(client.getSourceId()).isEqualTo(DRIVE);
        assertThat(client.getRootNodeId()).isEqualTo(DRIVE + ":root");
    }

    @Test
    void cannotNameARootWhenSeveralDrivesAreConfigured() throws Exception {
        mock = new MockGraphServer(options());
        SharePointConnectorSettings twoDrives = new SharePointConnectorSettings(
                mock.graphBaseUrl(), SharePointConnectorSettings.AuthMode.STATIC_TOKEN,
                null, null, null, null, null, "mock-token",
                List.of(DRIVE, "b!second-drive"), null,
                List.of(), List.of(), List.of(), List.of(),
                SharePointAclMapper.AclFallback.FAIL_CLOSED, SharePointAclMapper.GroupGrants.MAP,
                SharePointConnectorSettings.PermissionsMode.PER_ITEM,
                Set.of(), 0, 1);
        SharePointConnectorClient client = new SharePointConnectorClient(twoDrives,
                new GraphHttpClient(mock.graphBaseUrl(), twoDrives.tokenProvider(),
                        ResourceUnitMeter.unmetered()));

        // Inventing one would mean walking whichever drive happened to be first. Null makes the host
        // require connector.roots instead.
        assertThat(client.getRootNodeId()).isNull();
    }

    @Test
    void readsOneItemWithItsPermissionsMappedAndItsPropertiesCarried() throws Exception {
        SharePointConnectorClient client = clientFor(options());

        SourceNode node = client.getNode(DRIVE + ":i-named");

        assertThat(node.nodeId()).isEqualTo(DRIVE + ":i-named");
        assertThat(node.sourceType()).isEqualTo("sharepoint");
        assertThat(node.name()).isEqualTo("named-grant.txt");
        assertThat(node.mimeType()).isEqualTo("text/plain");
        assertThat(node.folder()).isFalse();
        assertThat(node.modifiedAt()).isNotNull();
        assertThat(node.readPrincipals()).containsExactlyInAnyOrder("user-guid-bob", "bob@contoso.com");
        assertThat(node.denyPrincipals()).isEmpty();
        assertThat(node.security().permissions()).hasSize(2);
        assertThat(node.sourceProperties())
                .containsEntry("sharepoint_driveId", DRIVE)
                .containsEntry("sharepoint_itemId", "i-named")
                // Recorded because a hierarchy cache keys on it, and because "why is this one document
                // readable by someone else" should be answerable from the index.
                .containsEntry("sharepoint_uniquePermissions", true)
                .containsKey("sharepoint_webUrl");
    }

    @Test
    void marksAFolderAsAContainerWithNoMimeType() throws Exception {
        SharePointConnectorClient client = clientFor(options());

        SourceNode folder = client.getNode(DRIVE + ":f-public");

        assertThat(folder.folder()).isTrue();
        assertThat(folder.mimeType()).isNull();
    }

    @Test
    void rejectsANodeIdThatIsNotComposite() throws Exception {
        SharePointConnectorClient client = clientFor(options());

        assertThatThrownBy(() -> client.getNode("just-an-item-id"))
                .isInstanceOf(GraphException.class)
                .hasMessageContaining("<driveId>:<itemId>");
    }

    @Test
    void walksAContainerCompletelyDespiteGraphIgnoringSkip() throws Exception {
        SharePointConnectorClient client = clientFor(options());

        // Exactly how the host walks: page size 2, skip advanced by 2, ending on an empty page. Graph
        // ignores $skip, so this only works because the client remembers the nextLink per container.
        List<String> names = new ArrayList<>();
        for (int skip = 0; ; skip += 2) {
            List<SourceNode> page = client.getChildren(DRIVE + ":root", skip, 2);
            if (page.isEmpty()) {
                break;
            }
            page.forEach(node -> names.add(node.name()));
        }

        assertThat(names).containsExactly("public", "restricted-user", "restricted-group", "org-link",
                "nested");
    }

    @Test
    void walksSeveralContainersInterleavedTheWayTheHostRecursesIntoThem() throws Exception {
        SharePointConnectorClient client = clientFor(options());

        // The host descends into a child before asking the parent for its next page, so two containers are
        // part-paged at once. A single "next link" instead of a per-container one would cross the streams.
        List<SourceNode> rootPageOne = client.getChildren(DRIVE + ":root", 0, 2);
        List<SourceNode> publicPageOne = client.getChildren(DRIVE + ":f-public", 0, 2);
        List<SourceNode> rootPageTwo = client.getChildren(DRIVE + ":root", 2, 2);
        List<SourceNode> publicPageTwo = client.getChildren(DRIVE + ":f-public", 2, 2);

        assertThat(rootPageOne).extracting(SourceNode::name).containsExactly("public", "restricted-user");
        assertThat(publicPageOne).extracting(SourceNode::name)
                .containsExactly("quarterly-review.txt", "incident-log.md");
        assertThat(rootPageTwo).extracting(SourceNode::name)
                .containsExactly("restricted-group", "org-link");
        assertThat(publicPageTwo).extracting(SourceNode::name)
                .containsExactly("obsolete-note.txt", "quarterly-report.pdf");
    }

    @Test
    void recoversWhenAPageLinkIsNoLongerRemembered() throws Exception {
        SharePointConnectorClient client = clientFor(options());

        // Asking for the second page without having asked for the first. The cache cannot help, so the
        // client re-pages from the start rather than sending Graph a $skip it would ignore.
        List<SourceNode> secondPage = client.getChildren(DRIVE + ":root", 2, 2);

        assertThat(secondPage).extracting(SourceNode::name)
                .containsExactly("restricted-group", "org-link");
    }

    @Test
    void returnsAnEmptyPageBeyondTheEndOfAContainer() throws Exception {
        SharePointConnectorClient client = clientFor(options());

        assertThat(client.getChildren(DRIVE + ":f-level-two", 0, 2)).hasSize(1);
        // Only an empty page ends a container's listing, so this has to be empty rather than a repeat.
        assertThat(client.getChildren(DRIVE + ":f-level-two", 2, 2)).isEmpty();
    }

    @Test
    void seedsACursorAtTheCurrentPositionWithoutReportingTheExistingCorpus() throws Exception {
        SharePointConnectorClient client = clientFor(options());

        String cursor = client.initialCursor();

        assertThat(client.supportsChangeFeed()).isTrue();
        assertThat(cursor).isNotNull().contains(DRIVE).contains("token=delta-");
    }

    @Test
    void reportsADeletionFromTheFeedAsATombstone() throws Exception {
        SharePointConnectorClient client = clientFor(options());

        SourceChangePage page = client.changesSince(client.initialCursor(), 100);

        assertThat(page.cursorExpired()).isFalse();
        assertThat(page.changed()).isEmpty();
        assertThat(page.deleted()).extracting(SourceTombstone::nodeId)
                .containsExactly(DRIVE + ":i-removed");
        assertThat(page.deleted()).extracting(SourceTombstone::reason)
                .containsExactly(SourceTombstone.Reason.DELETED);
        // The cursor has to advance, or the next pass replays this page for ever.
        assertThat(page.nextCursor()).isNotNull().isNotEqualTo(client.initialCursor());
    }

    @Test
    void treatsAStaleCursorAsExpiredRatherThanAsNoChanges() throws Exception {
        SharePointConnectorClient client = clientFor(options());

        SourceChangePage page = client.changesSince(
                "{\"" + DRIVE + "\":\"" + mock.graphBaseUrl() + "/drives/" + DRIVE
                        + "/root/delta?token=delta-from-a-previous-run\"}", 100);

        // The host has to forget the cursor and walk. Reporting an empty page would let it treat an empty
        // deletion list as authoritative.
        assertThat(page.cursorExpired()).isTrue();
        assertThat(page.changed()).isEmpty();
        assertThat(page.deleted()).isEmpty();
        assertThat(page.nextCursor()).isNull();
    }

    @Test
    void treatsACursorItDidNotWriteAsExpired() throws Exception {
        SharePointConnectorClient client = clientFor(options());

        // A cursor from an older encoding, or from another connector entirely. Walking is always safe.
        assertThat(client.changesSince("not-json-at-all", 100).cursorExpired()).isTrue();
        assertThat(client.changesSince(null, 100).cursorExpired()).isTrue();
    }

    @Test
    void downloadsContentToATempFileWithoutLeakingTheBearerToken() throws Exception {
        SharePointConnectorClient client = clientFor(options());

        Path downloaded = client.downloadContent(DRIVE + ":i-quarterly", "quarterly-review.txt")
                .getFile().toPath();
        try {
            // The mock rejects a signed URL called with an Authorization header, so getting bytes at all is
            // the proof the redirect was followed clean.
            assertThat(Files.readString(downloaded, StandardCharsets.UTF_8))
                    .contains("pangolin-ledger-quarterly");
        } finally {
            Files.deleteIfExists(downloaded);
        }

        assertThat(new String(client.getContent(DRIVE + ":i-incident"), StandardCharsets.UTF_8))
                .contains("pangolin-ledger-incident");
    }

    @Test
    void doesNotIngestAnItemWhosePermissionsCannotBeRead() throws Exception {
        SharePointConnectorClient client = clientFor(options());

        // i-noacl resolves as an item but has no permissions fixture, so /permissions answers 404.
        SourceNode node = client.getNode(DRIVE + ":i-noacl");

        // Null rather than an exception, because the host reads it as "the connector answered, and this node
        // is not available": it records the reason and marks the pass incomplete, so the reconciliation
        // sweep does not treat the pass as authoritative and start deleting.
        assertThat(node).isNull();
        assertThat(client.aclMapper().counters().get("documentsWithNoReadableAcl")).isEqualTo(1);
    }

    @Test
    void ingestsAnItemWithAnUnreadableAclOnlyWhenPublicWasChosenExplicitly() throws Exception {
        mock = new MockGraphServer(options());
        SharePointConnectorSettings explicitlyPublic = new SharePointConnectorSettings(
                mock.graphBaseUrl(), SharePointConnectorSettings.AuthMode.STATIC_TOKEN,
                null, null, null, null, null, "mock-token",
                List.of(DRIVE), null, List.of(), List.of(), List.of(), List.of(),
                SharePointAclMapper.AclFallback.PUBLIC, SharePointAclMapper.GroupGrants.MAP,
                SharePointConnectorSettings.PermissionsMode.PER_ITEM,
                Set.of(), 0, 1);
        SharePointConnectorClient client = new SharePointConnectorClient(explicitlyPublic,
                new GraphHttpClient(mock.graphBaseUrl(), explicitlyPublic.tokenProvider(),
                        ResourceUnitMeter.unmetered()));

        SourceNode node = client.getNode(DRIVE + ":i-noacl");

        assertThat(node).isNotNull();
        assertThat(node.readPrincipals()).containsExactly(SharePointAclMapper.EVERYONE_AUTHORITY);
    }

    @Test
    void readsEveryPageOfAPermissionsCollection() throws Exception {
        SharePointConnectorClient client = clientFor(options());

        SourceNode node = client.getNode(DRIVE + ":i-paged");

        // A permissions collection read only as far as its first page silently drops grants, which is an
        // ACL defect rather than a missing feature. The second page's grantee has to be here.
        assertThat(node.readPrincipals()).containsExactlyInAnyOrder(
                "user-guid-page-one", "page.one@contoso.com",
                "user-guid-page-two", "page.two@contoso.com");
        assertThat(mock.requestLog()).anySatisfy(entry -> assertThat(entry).contains("permissions?page=2"));
    }

    @Test
    void keepsCrawlingWhenTheTenantThrottlesMidWalk() throws Exception {
        SharePointConnectorClient client = clientFor(options().withThrottleEveryNthRequest(3, 1));

        // Every third request refused. The walk has to complete anyway, which is the difference between
        // pausing and failing.
        List<SourceNode> children = client.getChildren(DRIVE + ":f-public", 0, 10);

        assertThat(children).extracting(SourceNode::name)
                .containsExactly("quarterly-review.txt", "incident-log.md", "obsolete-note.txt",
                        "quarterly-report.pdf");
    }

    @Test
    void publishesTheSchemaThroughTheClientSoTheHostCanValidateIt() throws Exception {
        SharePointConnectorClient client = clientFor(options());

        assertThat(client.connectorSchema().sourceType()).isEqualTo("sharepoint");
        assertThat(client.connectorSchema().fields()).isNotEmpty();
    }

    @Test
    void readsOnePermissionsCollectionForAWholeInheritingFolderInHierarchicalMode() throws Exception {
        SharePointConnectorClient client =
                clientFor(options(), SharePointConnectorSettings.PermissionsMode.HIERARCHICAL);

        client.getChildren(DRIVE + ":root", 0, 100);
        List<SourceNode> files = client.getChildren(DRIVE + ":f-public", 0, 100);

        // Five folders and four files resolved, and Graph was asked for exactly one permissions collection:
        // the drive root's, which every one of them inherits. Per item this would be nine calls at five
        // resource units each.
        assertThat(files).hasSize(4);
        assertThat(permissionRequests()).containsExactly("/drives/" + DRIVE + "/items/root/permissions");
        assertThat(client.permissionHierarchy().inheritedResolutions()).isEqualTo(9);
    }

    @Test
    void neverServesAnAncestorAclToAnItemThatHasItsOwnInHierarchicalMode() throws Exception {
        SharePointConnectorClient client =
                clientFor(options(), SharePointConnectorSettings.PermissionsMode.HIERARCHICAL);

        client.getChildren(DRIVE + ":root", 0, 100);
        SourceNode named = client.getNode(DRIVE + ":i-named");

        // The correctness risk in the whole optimisation. i-named is granted to Bob alone and sits under a
        // folder the whole tenant may read, so serving it the folder's ACL would publish it to everyone.
        assertThat(named.readPrincipals())
                .containsExactlyInAnyOrder("user-guid-bob", "bob@contoso.com");
        assertThat(named.readPrincipals()).doesNotContain(SharePointAclMapper.EVERYONE_AUTHORITY);
        assertThat(named.sourceProperties()).containsEntry("sharepoint_uniquePermissions", true);
        assertThat(permissionRequests())
                .contains("/drives/" + DRIVE + "/items/i-named/permissions");
    }

    @Test
    void bothModesGrantAnInheritingDocumentToTheSamePrincipals() throws Exception {
        SourceNode perItem = quarterlyReview(SharePointConnectorSettings.PermissionsMode.PER_ITEM);
        SourceNode hierarchical = quarterlyReview(SharePointConnectorSettings.PermissionsMode.HIERARCHICAL);

        // The invariant the whole optimisation rests on: who may read a document must not depend on how the
        // connector found out. It holds because an inheriting item's own permissions collection reports every
        // entry it inherits, which is what the ancestor's collection says too.
        assertThat(hierarchical.readPrincipals()).isEqualTo(perItem.readPrincipals());
        assertThat(hierarchical.readPrincipals())
                .containsExactlyInAnyOrder("GROUP_SITEGROUP_3", SharePointAclMapper.EVERYONE_AUTHORITY);

        // Both also agree that it inherits, and neither marks it as having permissions of its own: that
        // property is how the index answers "why is this one document readable by someone else".
        assertThat(hierarchical.security().inheritanceEnabled()).isTrue();
        assertThat(perItem.security().inheritanceEnabled()).isTrue();
        assertThat(hierarchical.sourceProperties()).doesNotContainKey("sharepoint_uniquePermissions");
        assertThat(perItem.sourceProperties()).doesNotContainKey("sharepoint_uniquePermissions");
    }

    /** One inheriting document, reached the way a walk reaches it, in whichever mode. */
    private SourceNode quarterlyReview(SharePointConnectorSettings.PermissionsMode mode) throws IOException {
        if (mock != null) {
            mock.close();
        }
        SharePointConnectorClient client = clientFor(options(), mode);
        // Through the folder rather than straight at the file, so hierarchical mode has its ancestor cached
        // the way a real pass would.
        client.getChildren(DRIVE + ":root", 0, 100);
        return client.getNode(DRIVE + ":i-quarterly");
    }

    @Test
    void refusesHierarchicalModeWhenTheTenantDoesNotHonourThePreference() throws Exception {
        // A tenant that cannot grant Sites.FullControl.All, which is a one-line change here and an
        // administrator's decision in reality.
        SharePointConnectorClient client = clientFor(options().withHonouredPreferences(Set.of()),
                SharePointConnectorSettings.PermissionsMode.HIERARCHICAL);

        // Refuses rather than degrading. A silent fallback to per-item would multiply the crawl's cost by
        // about five against a daily cap, without saying so anywhere an operator would look.
        assertThatThrownBy(() -> client.getChildren(DRIVE + ":root", 0, 100))
                .isInstanceOf(GraphException.class)
                .hasMessageContaining("hierarchicalsharing")
                .hasMessageContaining("Sites.FullControl.All")
                .hasMessageContaining("sharepoint.permissions-mode=per-item");
    }

    @Test
    void stillReportsAnExpiredCursorInHierarchicalModeRatherThanAnUnhonouredPreference() throws Exception {
        SharePointConnectorClient client =
                clientFor(options(), SharePointConnectorSettings.PermissionsMode.HIERARCHICAL);

        SourceChangePage page = client.changesSince(
                "{\"" + DRIVE + "\":\"" + mock.graphBaseUrl() + "/drives/" + DRIVE
                        + "/root/delta?token=delta-from-a-previous-run\"}", 100);

        // A 410 carries an error envelope and no Preference-Applied header, so a preference check ahead of
        // the resync check would report a stale cursor as a tenant that does not honour hierarchical sharing
        // and the host would never walk.
        assertThat(page.cursorExpired()).isTrue();
    }

    @Test
    void spendsFewerResourceUnitsPerDocumentInHierarchicalModeThanPerItem() throws Exception {
        long perItem = unitsForAWalk(SharePointConnectorSettings.PermissionsMode.PER_ITEM);
        long hierarchical = unitsForAWalk(SharePointConnectorSettings.PermissionsMode.HIERARCHICAL);

        // Measured over the same tree rather than estimated, which is what the issue asks for. Reported so a
        // run of this test is the evidence, not just a green tick.
        System.out.println("Resource units over the fixture tree: per-item=" + perItem
                + ", hierarchical=" + hierarchical);
        assertThat(hierarchical).isLessThan(perItem);
        // Permission reads are five units of every six, so removing most of them has to be a large cut
        // rather than a marginal one, or the mode is not worth the Sites.FullControl.All it costs.
        assertThat(hierarchical).isLessThan(perItem / 2);
    }

    /** Units spent walking the whole fixture tree in one mode, with the two Graph seams unchanged. */
    private long unitsForAWalk(SharePointConnectorSettings.PermissionsMode mode) throws IOException {
        if (mock != null) {
            mock.close();
        }
        mock = new MockGraphServer(options());
        SharePointConnectorSettings settings = settings(mock.graphBaseUrl(), mode);
        ResourceUnitMeter meter = ResourceUnitMeter.unmetered();
        SharePointConnectorClient client = new SharePointConnectorClient(settings,
                new GraphHttpClient(mock.graphBaseUrl(), settings.tokenProvider(), meter));

        walk(client, DRIVE + ":root");
        client.logSummary(1);
        return meter.unitsSpent();
    }

    /** Depth-first, the way the host's discovery service walks a container. */
    private static void walk(SharePointConnectorClient client, String containerId) {
        for (SourceNode child : client.getChildren(containerId, 0, 100)) {
            if (child.folder()) {
                walk(client, child.nodeId());
            }
        }
    }

    /** Just the {@code /permissions} calls the connector made, so they can be counted exactly. */
    private List<String> permissionRequests() {
        List<String> permissionCalls = new ArrayList<>();
        for (String entry : mock.requestLog()) {
            if (entry.contains("/permissions")) {
                // The method prefix and any paging query are noise for a count of which items were read.
                permissionCalls.add(entry.substring(entry.indexOf(' ') + 1).replace("/v1.0", ""));
            }
        }
        return permissionCalls;
    }
}
