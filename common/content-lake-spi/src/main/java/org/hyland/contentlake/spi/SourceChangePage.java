package org.hyland.contentlake.spi;

import java.util.List;

/**
 * One page of a source's change feed: what changed, what was removed, and where to resume.
 *
 * <p>This is the cursor-shaped alternative to walking a source with
 * {@link ContentSourceClient#getChildren}. A walk re-reads every node to find the few that moved and
 * infers deletions by comparing what it saw against what is indexed; a feed is told both directly. The
 * two paths coexist: a source that cannot answer {@link ContentSourceClient#changesSince} is still
 * walked exactly as before.</p>
 *
 * <p>A page is a statement about a window, not about the source as a whole. {@code deleted} is
 * authoritative for the nodes it names and says nothing about any other node, which is what lets the
 * host apply those tombstones without the ratio guards a walk needs.</p>
 *
 * @param changed       nodes created or modified within the window, in whatever order the source
 *                      reports them. A node that was created and then deleted inside one window may
 *                      appear in both lists; the host applies {@code changed} first, so the deletion
 *                      wins
 * @param deleted       nodes the source reports gone or out of scope. Each names one node and is
 *                      first-hand, so it is applied without a reconciliation sweep's safety checks
 * @param nextCursor    the opaque token to pass to the next {@code changesSince} call. Meaningless to
 *                      the host: it is stored verbatim and never parsed, compared or ordered
 * @param moreAvailable whether the source has further pages ready now. {@code false} means the feed is
 *                      drained up to {@code nextCursor}, not that it will never produce anything again
 * @param cursorExpired whether the cursor the host supplied is no longer usable. When {@code true} the
 *                      other four components carry nothing and must be ignored entirely: the host has
 *                      to forget its cursor and fall back to a full walk. Every real feed has this
 *                      state -- a delta link is invalidated, a page token ages out -- so it is a
 *                      component rather than an exception
 */
public record SourceChangePage(List<SourceNode> changed,
                               List<SourceTombstone> deleted,
                               String nextCursor,
                               boolean moreAvailable,
                               boolean cursorExpired) {

    public SourceChangePage {
        changed = changed == null ? List.of() : List.copyOf(changed);
        deleted = deleted == null ? List.of() : List.copyOf(deleted);
    }

    /** A page of changes, with {@code moreAvailable} saying whether the feed has further pages ready. */
    public static SourceChangePage of(List<SourceNode> changed,
                                      List<SourceTombstone> deleted,
                                      String nextCursor,
                                      boolean moreAvailable) {
        return new SourceChangePage(changed, deleted, nextCursor, moreAvailable, false);
    }

    /** Nothing changed in this window; {@code cursor} is where to resume. */
    public static SourceChangePage empty(String cursor) {
        return new SourceChangePage(List.of(), List.of(), cursor, false, false);
    }

    /**
     * The supplied cursor is no longer usable. Carries no changes, no deletions and no next cursor: a
     * source that has forgotten the window cannot describe it, and pretending otherwise would let the
     * host act on an empty {@code deleted} list as if it were authoritative.
     */
    public static SourceChangePage expired() {
        return new SourceChangePage(List.of(), List.of(), null, false, true);
    }

    /** Whether this page reports neither a change nor a deletion. */
    public boolean isEmpty() {
        return changed.isEmpty() && deleted.isEmpty();
    }
}
