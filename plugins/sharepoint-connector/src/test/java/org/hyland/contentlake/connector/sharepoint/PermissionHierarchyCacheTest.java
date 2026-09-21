package org.hyland.contentlake.connector.sharepoint;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.hyland.contentlake.spi.PermissionRule;
import org.hyland.contentlake.spi.SecurityConfig;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The resolution rules on their own, with the two Graph calls stubbed so every one of them is countable.
 *
 * <p>Hand-written stubs rather than mocks: what these tests are about is how many calls of each kind were
 * made and in what order, and a counter plus a recorded list says that more plainly than verifying
 * interactions on a mock. The same rules are exercised end to end against the mock Graph service in
 * {@link SharePointConnectorClientTest}.</p>
 */
class PermissionHierarchyCacheTest {

    private static final String DRIVE = "b!drive";
    private static final ObjectMapper JSON = new ObjectMapper();

    /** {@code root <- folder <- sub}, with one file in each of the two folders. */
    private final Map<String, JsonNode> items = new LinkedHashMap<>();

    private final List<String> permissionReads = new ArrayList<>();
    private final List<String> itemFetches = new ArrayList<>();

    /** Items whose permissions this stub refuses, for the fail-closed path. */
    private final Set<String> unreadable = new java.util.HashSet<>();

    PermissionHierarchyCacheTest() {
        items.put("root", item("root", null, true, false));
        items.put("folder", item("folder", "root", true, false));
        items.put("sub", item("sub", "folder", true, false));
        items.put("inheriting-file", item("inheriting-file", "folder", false, false));
        items.put("sibling-file", item("sibling-file", "folder", false, false));
        items.put("deep-file", item("deep-file", "sub", false, false));
        items.put("own-file", item("own-file", "folder", false, true));
        items.put("orphan-file", item("orphan-file", null, false, false));
    }

    private PermissionHierarchyCache cache(SharePointConnectorSettings.PermissionsMode mode) {
        return new PermissionHierarchyCache(mode,
                (driveId, itemId) -> {
                    permissionReads.add(itemId);
                    if (unreadable.contains(itemId)) {
                        return SharePointAclMapper.MappedAcl.notIngestable();
                    }
                    return acl(itemId);
                },
                (driveId, itemId) -> {
                    itemFetches.add(itemId);
                    JsonNode item = items.get(itemId);
                    if (item == null) {
                        throw new GraphException("No such item " + itemId);
                    }
                    return item;
                });
    }

    @Test
    void perItemModeReadsEveryItemAndCachesNothing() {
        PermissionHierarchyCache cache = cache(SharePointConnectorSettings.PermissionsMode.PER_ITEM);

        cache.resolve(DRIVE, items.get("inheriting-file"));
        cache.resolve(DRIVE, items.get("sibling-file"));

        // The naive path, unchanged: one permission call each, no item fetches, nothing remembered.
        assertThat(permissionReads).containsExactly("inheriting-file", "sibling-file");
        assertThat(itemFetches).isEmpty();
        assertThat(cache.counters().get("cachedContainers")).isZero();
    }

    @Test
    void aFolderOfInheritingFilesCostsOnePermissionCallRatherThanOnePerFile() {
        PermissionHierarchyCache cache = cache(SharePointConnectorSettings.PermissionsMode.HIERARCHICAL);

        // The order a walk arrives in: the container first, then its children.
        cache.resolve(DRIVE, items.get("folder"));
        cache.resolve(DRIVE, items.get("inheriting-file"));
        cache.resolve(DRIVE, items.get("sibling-file"));

        // This is the whole point of the mode. Three items, one permission call, and it is the root's.
        assertThat(permissionReads).containsExactly("root");
        assertThat(cache.inheritedResolutions()).isEqualTo(3);
    }

    @Test
    void anItemWithItsOwnSharingIsNeverServedAnAncestorAcl() {
        PermissionHierarchyCache cache = cache(SharePointConnectorSettings.PermissionsMode.HIERARCHICAL);
        cache.resolve(DRIVE, items.get("folder"));

        SharePointAclMapper.MappedAcl resolved = cache.resolve(DRIVE, items.get("own-file"));

        // The correctness risk in the whole optimisation: if the shared facet were ignored, this file would
        // inherit whoever may read its folder, and a document restricted to one person becomes readable by
        // everyone the folder is shared with.
        assertThat(resolved.readPrincipals()).containsExactly("granted-on-own-file");
        assertThat(permissionReads).contains("own-file");
        assertThat(resolved.hasUniquePermissions()).isTrue();
    }

    @Test
    void walksUpToTheNearestAncestorAndMemoisesEveryContainerOnTheWay() {
        PermissionHierarchyCache cache = cache(SharePointConnectorSettings.PermissionsMode.HIERARCHICAL);

        // Straight at a file two levels down, with nothing cached: the walk never came through here, which
        // is what an incremental change-feed pass looks like.
        cache.resolve(DRIVE, items.get("deep-file"));

        // One item fetch per ancestor at 1 unit each, and one permission read at 5, rather than 5 for the
        // file itself.
        assertThat(itemFetches).containsExactly("sub", "folder", "root");
        assertThat(permissionReads).containsExactly("root");

        // Both containers are now remembered, so a sibling costs nothing at all.
        itemFetches.clear();
        cache.resolve(DRIVE, items.get("inheriting-file"));
        assertThat(itemFetches).isEmpty();
        assertThat(permissionReads).containsExactly("root");
    }

    @Test
    void marksAnInheritedAclAsInheritedRatherThanAsTheItemsOwn() {
        PermissionHierarchyCache cache = cache(SharePointConnectorSettings.PermissionsMode.HIERARCHICAL);

        SharePointAclMapper.MappedAcl resolved = cache.resolve(DRIVE, items.get("inheriting-file"));

        // Same grants as the ancestor, but the item must not be stored with the unique-permissions marker:
        // that property exists to answer "why is this one document readable by someone else".
        assertThat(resolved.readPrincipals()).containsExactly("granted-on-root");
        assertThat(resolved.hasUniquePermissions()).isFalse();
        assertThat(resolved.securityConfig().inheritanceEnabled()).isTrue();
        assertThat(resolved.securityConfig().permissions())
                .extracting(PermissionRule::identity).containsExactly("granted-on-root");
    }

    @Test
    void anItemWithNoParentToInheritFromIsReadOnItsOwn() {
        PermissionHierarchyCache cache = cache(SharePointConnectorSettings.PermissionsMode.HIERARCHICAL);

        cache.resolve(DRIVE, items.get("orphan-file"));

        // An inheritance claim with nothing to inherit from would be a guess about who may read this.
        assertThat(permissionReads).containsExactly("orphan-file");
        assertThat(itemFetches).isEmpty();
    }

    @Test
    void aFailClosedAncestorRefusesItsDescendantsRatherThanWideningThem() {
        unreadable.add("root");
        PermissionHierarchyCache cache = cache(SharePointConnectorSettings.PermissionsMode.HIERARCHICAL);

        SharePointAclMapper.MappedAcl resolved = cache.resolve(DRIVE, items.get("inheriting-file"));

        // The refusal has to propagate down. Falling back to "no ancestor ACL, so read the item's own" would
        // be the more permissive answer and is the wrong direction to fail in.
        assertThat(resolved.ingestable()).isFalse();
        assertThat(resolved.readPrincipals()).isEmpty();
    }

    @Test
    void readsAnItemsOwnPermissionsWhenItsAncestorCannotBeFetched() {
        PermissionHierarchyCache cache = cache(SharePointConnectorSettings.PermissionsMode.HIERARCHICAL);

        // A parent id pointing at an item Graph will not serve: deleted between pages, or outside the scope
        // this connector can read.
        JsonNode strandedChild = item("stranded", "no-such-folder", false, false);
        SharePointAclMapper.MappedAcl resolved = cache.resolve(DRIVE, strandedChild);

        assertThat(itemFetches).containsExactly("no-such-folder");
        assertThat(permissionReads).containsExactly("stranded");
        assertThat(resolved.readPrincipals()).containsExactly("granted-on-stranded");
    }

    @Test
    void doesNotFollowAParentChainForEverWhenItLoops() {
        items.put("loop-a", item("loop-a", "loop-b", true, false));
        items.put("loop-b", item("loop-b", "loop-a", true, false));
        PermissionHierarchyCache cache = cache(SharePointConnectorSettings.PermissionsMode.HIERARCHICAL);

        SharePointAclMapper.MappedAcl resolved = cache.resolve(DRIVE, item("looped-file", "loop-a", false, false));

        // Bounded, and it gives up towards the conservative answer: the item's own permissions.
        assertThat(itemFetches).hasSizeLessThan(128);
        assertThat(permissionReads).containsExactly("looped-file");
        assertThat(resolved.readPrincipals()).containsExactly("granted-on-looped-file");
    }

    /**
     * A {@code driveItem} carrying only what the resolution rules look at.
     *
     * @param sharing whether it carries the {@code shared} facet, which under
     *                {@code Prefer: hierarchicalsharing} means "this item has permissions of its own"
     */
    private static JsonNode item(String id, String parentId, boolean folder, boolean sharing) {
        StringBuilder json = new StringBuilder("{\"id\":\"").append(id).append('"');
        json.append(",\"parentReference\":{\"driveId\":\"").append(DRIVE).append('"');
        if (parentId != null) {
            json.append(",\"id\":\"").append(parentId).append('"');
        }
        json.append('}');
        if (folder) {
            json.append(",\"folder\":{\"childCount\":1}");
        }
        if (sharing) {
            json.append(",\"shared\":{\"scope\":\"users\"}");
        }
        json.append('}');
        try {
            return JSON.readTree(json.toString());
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    /** A mapped ACL naming the item it was read from, so a wrongly reused one is identifiable. */
    private static SharePointAclMapper.MappedAcl acl(String itemId) {
        String principal = "granted-on-" + itemId;
        return new SharePointAclMapper.MappedAcl(Set.of(principal),
                new SecurityConfig(false, List.of(new PermissionRule(principal, "user", principal, "READ"))),
                true, true);
    }

    /**
     * An <em>empty</em> {@code shared} object marks a hierarchy root, because presence is the whole signal
     * (#142).
     *
     * <p>Confirmed against a real tenant: with {@code Prefer: hierarchicalsharing} the drive root came back
     * carrying {@code "shared": {}} where the same request without the preference carried no facet at all.
     * So the facet can be empty, and reading anything inside it -- {@code scope}, {@code sharedBy} -- would
     * work against these fixtures and fail against the cloud.</p>
     *
     * <p>Note the deliberate asymmetry with {@code inheritedFrom}, where an empty object means the opposite:
     * there it names no ancestor and therefore is <em>not</em> inheritance. Presence is the signal for one
     * and content is the signal for the other, which is worth knowing before tidying either.</p>
     */
    @Test
    void anEmptySharedFacetStillMarksAHierarchyRoot() {
        JsonNode emptySharedFacet = item("own-file-empty-facet", "folder", false, false);
        ((com.fasterxml.jackson.databind.node.ObjectNode) emptySharedFacet)
                .putObject("shared");
        PermissionHierarchyCache cache = cache(SharePointConnectorSettings.PermissionsMode.HIERARCHICAL);
        cache.resolve(DRIVE, items.get("folder"));

        SharePointAclMapper.MappedAcl resolved = cache.resolve(DRIVE, emptySharedFacet);

        assertThat(permissionReads).contains("own-file-empty-facet");
        assertThat(resolved.readPrincipals()).containsExactly("granted-on-own-file-empty-facet");
    }
}
