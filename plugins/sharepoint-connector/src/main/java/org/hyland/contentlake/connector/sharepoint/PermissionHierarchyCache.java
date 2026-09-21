package org.hyland.contentlake.connector.sharepoint;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

/**
 * Resolves an item's ACL through SharePoint's sharing hierarchy instead of reading every item's own.
 *
 * <h3>Why this exists</h3>
 * <p>Graph charges 5 resource units for any permission operation, and documents that {@code permissions}
 * cannot be {@code $expand}ed onto a {@code driveItem} GET. A crawl that reads them per item therefore
 * spends about six units per document, of which five are the ACL read: against a per-application cap of
 * 1,250 units a minute and 1.2M a day, that bounds a first crawl near 200,000 documents in 24 hours.
 * Resolving inherited ACLs from the nearest ancestor that actually has them brings the figure close to one
 * unit per document, which is the content download and nothing else.</p>
 *
 * <h3>The signal it keys on, and why there is only one</h3>
 * <p>With {@code Prefer: hierarchicalsharing}, Graph returns the {@code shared} facet only on an item where
 * sharing was applied -- the root of a permission hierarchy -- rather than on every item that inherits from
 * one. So the facet's <em>presence</em> is the only thing that distinguishes "this item has permissions of
 * its own" from "this item inherits", and its absence is what makes an inherited ACL safe to reuse.</p>
 *
 * <p>That is also why {@link SharePointConnectorClient} refuses to run in this mode when Graph does not echo
 * the preference back. Without the header the facet appears on inheriting items too, absence stops meaning
 * anything, and every judgement below would be a guess.</p>
 *
 * <h3>What is cached, and what deliberately is not</h3>
 * <p>Only containers: the drive root, hierarchy roots, and folders whose ACL was resolved on the way down.
 * A file's resolution is never stored, because a cache that grows with item count is the memory problem the
 * withdrawn flattened item index was, and it buys nothing -- nothing asks about a file twice.</p>
 *
 * <p>The map is bounded and cleared wholesale when it fills, the same way the client's page-link cache is.
 * Losing an entry costs a re-read, never a wrong answer, which is what makes the crude policy acceptable.</p>
 *
 * <h3>One step of the original design is deliberately absent</h3>
 * <p>The design this implements also proposed caching a resolved set under {@code inheritedFrom.id} whenever
 * a per-item read came back with every entry inherited, for a second-order hit on the next sibling. That was
 * written for a version that keyed on {@code inheritedFrom} rather than on the {@code shared} facet, and the
 * facet makes it vestigial: the only items read here are the ones that have sharing of their own, whose
 * entries are by definition not all inherited, and a walk reaches a container before its children so the
 * ancestor is already cached. Implementing it would mean carrying the ancestor id out through
 * {@code MappedAcl} to buy a hit that does not arise.</p>
 *
 * <h3>The one correctness risk</h3>
 * <p>Serving an ancestor's ACL to an item that has its own would make a restricted document readable by
 * whoever may read its folder. Three things keep that from happening: an item carrying {@code shared} is
 * always read on its own; an item whose parent cannot be established is read on its own; and an ancestor
 * whose own ACL could not be read stays not-ingestable, so the refusal propagates down rather than being
 * replaced by something more permissive.</p>
 */
final class PermissionHierarchyCache {

    private static final Logger log = Logger.getLogger(PermissionHierarchyCache.class.getName());

    /**
     * Cap on cached container ACLs, with the same clear-on-full policy as the client's page-link cache.
     *
     * <p>Bounded by folder count rather than item count in practice, so this is reached only by a corpus
     * with an extraordinary number of folders.
     */
    private static final int MAX_CACHED_CONTAINERS = 10_000;

    /**
     * How far up a parent chain to walk before giving up and reading the item's own permissions.
     *
     * <p>A real SharePoint path cannot approach this, so reaching it means the {@code parentReference} chain
     * is cyclic or inconsistent. Falling back to the item's own ACL is the answer that cannot over-share.
     */
    private static final int MAX_ANCESTOR_DEPTH = 64;

    /** Reads one item's permissions collection with every page followed, and maps it. Costs 5 units. */
    interface AclReader {
        SharePointAclMapper.MappedAcl read(String driveId, String itemId);
    }

    /** Fetches one {@code driveItem}, to continue up a parent chain. Costs 1 unit. */
    interface ItemFetcher {
        JsonNode fetch(String driveId, String itemId);
    }

    private final SharePointConnectorSettings.PermissionsMode mode;
    private final AclReader aclReader;
    private final ItemFetcher itemFetcher;

    /** {@code <driveId>:<containerItemId>} to the ACL that container's descendants inherit. */
    private final Map<String, SharePointAclMapper.MappedAcl> containerAcls = new ConcurrentHashMap<>();

    private final AtomicLong permissionReads = new AtomicLong();
    private final AtomicLong inheritedResolutions = new AtomicLong();
    private final AtomicLong ancestorItemFetches = new AtomicLong();

    PermissionHierarchyCache(SharePointConnectorSettings.PermissionsMode mode,
                             AclReader aclReader,
                             ItemFetcher itemFetcher) {
        this.mode = mode == null ? SharePointConnectorSettings.PermissionsMode.PER_ITEM : mode;
        this.aclReader = aclReader;
        this.itemFetcher = itemFetcher;
    }

    /**
     * The ACL for one item, however this mode obtains it.
     *
     * <p>In {@code per-item} mode this is a straight pass-through to the reader and nothing is cached, so the
     * naive path is exactly what it was before this class existed.</p>
     *
     * @param item the whole {@code driveItem}, because the decision needs its {@code shared} facet, whether
     *             it is a folder, and its {@code parentReference}
     */
    SharePointAclMapper.MappedAcl resolve(String driveId, JsonNode item) {
        String itemId = GraphHttpClient.text(item, "id");
        if (mode == SharePointConnectorSettings.PermissionsMode.PER_ITEM) {
            return read(driveId, itemId);
        }

        SharePointAclMapper.MappedAcl own = containerAcls.get(key(driveId, itemId));
        if (own != null) {
            // Asked about again, which happens when a container is both walked into and re-reported by the
            // change feed.
            return own;
        }

        if (hasOwnSharing(item)) {
            SharePointAclMapper.MappedAcl acl = read(driveId, itemId);
            remember(driveId, itemId, acl);
            return acl;
        }

        String parentId = parentIdOf(item);
        if (parentId == null) {
            // Either the drive root, or an item whose parent Graph did not report. Both have to be read: an
            // inheritance claim with nothing to inherit from is a guess.
            SharePointAclMapper.MappedAcl acl = read(driveId, itemId);
            remember(driveId, itemId, acl);
            return acl;
        }

        SharePointAclMapper.MappedAcl inherited = fromAncestor(driveId, parentId, 0);
        if (inherited == null) {
            log.warning("Could not resolve an inherited ACL for " + key(driveId, itemId)
                    + " within " + MAX_ANCESTOR_DEPTH + " ancestors; reading its own permissions instead");
            return read(driveId, itemId);
        }

        inheritedResolutions.incrementAndGet();
        if (isFolder(item)) {
            // A folder is a container, so its children will ask about it. Caching here is what turns a
            // 10,000-file folder into one resolution instead of one item fetch per file.
            remember(driveId, itemId, inherited.asInherited());
        }
        return inherited.asInherited();
    }

    /**
     * The ACL a descendant of {@code ancestorId} inherits, walking up until something is cached or has its
     * own sharing.
     *
     * @return {@code null} when the chain ran deeper than {@link #MAX_ANCESTOR_DEPTH}, which the caller
     *         answers by reading the item's own permissions
     */
    private SharePointAclMapper.MappedAcl fromAncestor(String driveId, String ancestorId, int depth) {
        if (depth >= MAX_ANCESTOR_DEPTH) {
            return null;
        }

        SharePointAclMapper.MappedAcl cached = containerAcls.get(key(driveId, ancestorId));
        if (cached != null) {
            return cached;
        }

        // Not cached, so the walk did not come through this container. One item fetch at 1 unit is a fifth
        // of what reading the child's own permissions would cost, and it is paid once per container.
        JsonNode ancestor;
        try {
            ancestor = itemFetcher.fetch(driveId, ancestorId);
            ancestorItemFetches.incrementAndGet();
        } catch (GraphException e) {
            log.warning("Could not fetch ancestor " + key(driveId, ancestorId)
                    + " while resolving an inherited ACL (" + e.getMessage() + ")");
            return null;
        }

        if (hasOwnSharing(ancestor)) {
            SharePointAclMapper.MappedAcl acl = read(driveId, ancestorId);
            remember(driveId, ancestorId, acl);
            return acl;
        }

        String parentId = parentIdOf(ancestor);
        if (parentId == null) {
            SharePointAclMapper.MappedAcl acl = read(driveId, ancestorId);
            remember(driveId, ancestorId, acl);
            return acl;
        }

        SharePointAclMapper.MappedAcl inherited = fromAncestor(driveId, parentId, depth + 1);
        if (inherited == null) {
            return null;
        }
        SharePointAclMapper.MappedAcl asInherited = inherited.asInherited();
        remember(driveId, ancestorId, asInherited);
        return asInherited;
    }

    private SharePointAclMapper.MappedAcl read(String driveId, String itemId) {
        permissionReads.incrementAndGet();
        return aclReader.read(driveId, itemId);
    }

    /**
     * Whether this item has sharing of its own, under {@code Prefer: hierarchicalsharing}.
     *
     * <p>The facet is the whole signal. It is present on a permission-hierarchy root and absent on everything
     * that inherits from one, which is exactly the distinction the preference exists to make.</p>
     */
    private static boolean hasOwnSharing(JsonNode item) {
        return item != null && item.hasNonNull("shared");
    }

    private static boolean isFolder(JsonNode item) {
        return item != null && item.hasNonNull("folder");
    }

    /**
     * The parent item id, or {@code null} when there is none to inherit from.
     *
     * <p>A drive root reports a {@code parentReference} with no {@code id} (or an empty one), which reads as
     * "nothing above this", so the same check covers both the root and a payload that omitted the reference
     * entirely.</p>
     */
    private static String parentIdOf(JsonNode item) {
        if (item == null) {
            return null;
        }
        String parentId = GraphHttpClient.text(item.get("parentReference"), "id");
        return parentId == null || parentId.isBlank() ? null : parentId;
    }

    private void remember(String driveId, String itemId, SharePointAclMapper.MappedAcl acl) {
        if (itemId == null) {
            return;
        }
        if (containerAcls.size() >= MAX_CACHED_CONTAINERS) {
            // Bounded and disposable, as with the page-link cache: dropping entries costs re-reads.
            log.fine("Permission hierarchy cache reached its cap; clearing it");
            containerAcls.clear();
        }
        containerAcls.put(key(driveId, itemId), acl);
    }

    private static String key(String driveId, String itemId) {
        return driveId + ":" + itemId;
    }

    /** Permission reads made, which is what the cost per document is decided by. */
    long permissionReads() {
        return permissionReads.get();
    }

    /** Items served an ancestor's ACL, so no permission call was made for them at all. */
    long inheritedResolutions() {
        return inheritedResolutions.get();
    }

    /** Ancestor item fetches, the 1-unit price of resolving a container the walk did not come through. */
    long ancestorItemFetches() {
        return ancestorItemFetches.get();
    }

    /** Counters, for tests and for the run summary. */
    Map<String, Long> counters() {
        return Map.of(
                "permissionReads", permissionReads(),
                "inheritedResolutions", inheritedResolutions(),
                "ancestorItemFetches", ancestorItemFetches(),
                "cachedContainers", (long) containerAcls.size());
    }

    /**
     * One line saying what the mode actually bought, in calls rather than in units.
     *
     * <p>Reported for {@code per-item} too, where every number but the first is zero: "this crawl made one
     * permission call per document" is the answer that explains the cost figure next to it.</p>
     */
    void logSummary(String context) {
        log.info(() -> context + ": permissions mode " + mode.settingValue() + ", "
                + permissionReads() + " permission call(s), " + inheritedResolutions()
                + " item(s) served an inherited ACL, " + ancestorItemFetches()
                + " ancestor item fetch(es), " + containerAcls.size() + " container(s) cached");
    }
}
