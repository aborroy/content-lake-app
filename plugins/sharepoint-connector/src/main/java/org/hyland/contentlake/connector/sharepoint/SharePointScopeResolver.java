package org.hyland.contentlake.connector.sharepoint;

import org.hyland.contentlake.spi.ScopeResolver;
import org.hyland.contentlake.spi.SourceNode;

import java.util.List;
import java.util.Locale;

/**
 * Path and MIME scope, because SharePoint has no equivalent of the {@code cl:indexed} aspect.
 *
 * <p>Returned even when nothing is configured, in which case it admits everything. Keeping one
 * implementation means the traversal rules for a folder are decided in one place rather than half here and
 * half in the host's default.</p>
 *
 * <h3>A folder is traversed even when it is out of scope</h3>
 * <p>Excluding {@code /Archive} must not exclude {@code /Archive/2026/Current} unless that is also excluded,
 * and an include pattern of {@code /Finance/Reports} has to be reachable through {@code /Finance}, which does
 * not itself match it. So {@link #shouldTraverse} only refuses a folder that an <em>exclude</em> pattern
 * names, and include patterns never stop a descent.</p>
 *
 * <h3>Paths are absent in a delta response</h3>
 * <p>Graph omits {@code parentReference.path} from delta results, so a node arriving through the change feed
 * usually has a null path. Path patterns cannot be applied to a node with no path, and this admits it rather
 * than dropping it: a filter that silently discarded everything the feed reported would look exactly like a
 * broken feed. A deployment that needs path scope enforced on the feed has to leave the feed off, and that
 * tradeoff is worth stating in the documentation rather than encoding here.</p>
 */
public final class SharePointScopeResolver implements ScopeResolver {

    private final List<String> includePaths;
    private final List<String> excludePaths;
    private final List<String> includeMimeTypes;
    private final List<String> excludeMimeTypes;

    public SharePointScopeResolver(List<String> includePaths,
                                   List<String> excludePaths,
                                   List<String> includeMimeTypes,
                                   List<String> excludeMimeTypes) {
        this.includePaths = normalise(includePaths);
        this.excludePaths = normalise(excludePaths);
        this.includeMimeTypes = normalise(includeMimeTypes);
        this.excludeMimeTypes = normalise(excludeMimeTypes);
    }

    @Override
    public boolean isInScope(SourceNode node) {
        if (node == null) {
            return false;
        }
        String path = node.path();
        if (path != null) {
            if (matchesAny(excludePaths, path)) {
                return false;
            }
            if (!includePaths.isEmpty() && !matchesAny(includePaths, path)) {
                return false;
            }
        }
        if (node.folder()) {
            // MIME patterns are about documents. A folder has no content type to test.
            return true;
        }
        String mimeType = node.mimeType() == null ? "" : node.mimeType().toLowerCase(Locale.ROOT);
        if (matchesAnyMime(excludeMimeTypes, mimeType)) {
            return false;
        }
        return includeMimeTypes.isEmpty() || matchesAnyMime(includeMimeTypes, mimeType);
    }

    @Override
    public boolean shouldTraverse(SourceNode node) {
        if (node == null || !node.folder()) {
            return false;
        }
        String path = node.path();
        // Only an explicit exclusion stops a descent. An include pattern deeper in the tree has to stay
        // reachable through the folders above it, which do not match it themselves.
        return path == null || !matchesAny(excludePaths, path);
    }

    private static boolean matchesAny(List<String> prefixes, String path) {
        String candidate = path.toLowerCase(Locale.ROOT);
        for (String prefix : prefixes) {
            if (candidate.equals(prefix) || candidate.startsWith(prefix.endsWith("/") ? prefix : prefix + "/")) {
                return true;
            }
        }
        return false;
    }

    /** {@code *} wildcards, so {@code image/*} works the way an operator expects. */
    private static boolean matchesAnyMime(List<String> patterns, String mimeType) {
        for (String pattern : patterns) {
            if (pattern.equals("*") || pattern.equals(mimeType)) {
                return true;
            }
            if (pattern.endsWith("/*")) {
                String family = pattern.substring(0, pattern.length() - 1);
                if (mimeType.startsWith(family)) {
                    return true;
                }
            }
        }
        return false;
    }

    private static List<String> normalise(List<String> values) {
        if (values == null) {
            return List.of();
        }
        return values.stream()
                .filter(value -> value != null && !value.isBlank())
                .map(value -> value.trim().toLowerCase(Locale.ROOT))
                .toList();
    }
}
