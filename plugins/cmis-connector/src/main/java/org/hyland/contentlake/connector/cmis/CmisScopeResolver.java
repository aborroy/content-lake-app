package org.hyland.contentlake.connector.cmis;

import org.hyland.contentlake.spi.ScopeResolver;
import org.hyland.contentlake.spi.SourceNode;

import java.util.List;
import java.util.Locale;

/**
 * Path and MIME-type scope for a CMIS source.
 *
 * <p>Alfresco carries scope in the repository, as the {@code cl:indexed} aspect, so an editor decides
 * what is ingested and the connector only reads that decision. CMIS has no equivalent, so scope has to
 * come from configuration, which makes the rules here the whole of it.</p>
 *
 * <h3>The rules</h3>
 * <ul>
 *   <li><strong>Includes are a whitelist when present.</strong> Empty means everything under the root,
 *       because a connector configured with only an endpoint should ingest what it was pointed at rather
 *       than nothing.</li>
 *   <li><strong>Excludes are applied after includes and win.</strong> That order is what makes
 *       "everything under /Sites except /Sites/archive" expressible.</li>
 *   <li><strong>A folder is traversed when it could contain something in scope</strong>, which is not the
 *       same question as whether the folder itself is in scope. An include of {@code /Sites/marketing/docs}
 *       has to traverse {@code /Sites} and {@code /Sites/marketing} to reach it, so a folder is traversed
 *       when its path is a prefix of an include pattern as well as when an include pattern is a prefix of
 *       it. Getting this wrong makes a deeper include pattern silently ingest nothing.</li>
 *   <li><strong>MIME rules never apply to folders</strong>, which have no content type, and are checked
 *       only for documents.</li>
 * </ul>
 *
 * <p>MIME patterns accept a trailing {@code *} ({@code text/*}) and a bare {@code *}, which is what makes
 * "every text format" configurable without listing the subtypes a repository might report.</p>
 */
class CmisScopeResolver implements ScopeResolver {

    private final List<String> includePaths;
    private final List<String> excludePaths;
    private final List<String> includeMimeTypes;
    private final List<String> excludeMimeTypes;

    CmisScopeResolver(List<String> includePaths,
                      List<String> excludePaths,
                      List<String> includeMimeTypes,
                      List<String> excludeMimeTypes) {
        this.includePaths = normalizePaths(includePaths);
        this.excludePaths = normalizePaths(excludePaths);
        this.includeMimeTypes = lowercase(includeMimeTypes);
        this.excludeMimeTypes = lowercase(excludeMimeTypes);
    }

    @Override
    public boolean isInScope(SourceNode node) {
        if (node == null) {
            return false;
        }
        String path = normalize(node.path());
        if (!pathAllowed(path)) {
            return false;
        }
        return node.folder() || mimeAllowed(node.mimeType());
    }

    @Override
    public boolean shouldTraverse(SourceNode node) {
        if (node == null || !node.folder()) {
            return false;
        }
        String path = normalize(node.path());
        if (isExcluded(path)) {
            return false;
        }
        if (includePaths.isEmpty()) {
            return true;
        }
        // Either this folder is inside an include pattern, or it is on the way to one.
        for (String include : includePaths) {
            if (startsWithSegment(path, include) || startsWithSegment(include, path)) {
                return true;
            }
        }
        return false;
    }

    private boolean pathAllowed(String path) {
        if (isExcluded(path)) {
            return false;
        }
        if (includePaths.isEmpty()) {
            return true;
        }
        for (String include : includePaths) {
            if (startsWithSegment(path, include)) {
                return true;
            }
        }
        return false;
    }

    private boolean isExcluded(String path) {
        for (String exclude : excludePaths) {
            if (startsWithSegment(path, exclude)) {
                return true;
            }
        }
        return false;
    }

    private boolean mimeAllowed(String mimeType) {
        String mime = mimeType == null ? "" : mimeType.trim().toLowerCase(Locale.ROOT);
        for (String exclude : excludeMimeTypes) {
            if (mimeMatches(mime, exclude)) {
                return false;
            }
        }
        if (includeMimeTypes.isEmpty()) {
            return true;
        }
        for (String include : includeMimeTypes) {
            if (mimeMatches(mime, include)) {
                return true;
            }
        }
        return false;
    }

    private static boolean mimeMatches(String mime, String pattern) {
        if (pattern.equals("*")) {
            return true;
        }
        if (pattern.endsWith("*")) {
            return mime.startsWith(pattern.substring(0, pattern.length() - 1));
        }
        return mime.equals(pattern);
    }

    /**
     * Prefix match on whole path segments, so {@code /Sites/marketing} does not match
     * {@code /Sites/marketing-archive}. A plain {@code startsWith} would, and the surprise would be
     * silent: documents from a folder nobody named would appear in the index.
     */
    private static boolean startsWithSegment(String path, String prefix) {
        if (prefix.isEmpty() || prefix.equals("/")) {
            return true;
        }
        if (path.equals(prefix)) {
            return true;
        }
        return path.startsWith(prefix.endsWith("/") ? prefix : prefix + "/");
    }

    private static List<String> normalizePaths(List<String> paths) {
        if (paths == null) {
            return List.of();
        }
        return paths.stream()
                .filter(path -> path != null && !path.isBlank())
                .map(CmisScopeResolver::normalize)
                .filter(path -> !path.isEmpty())
                .toList();
    }

    /** A CMIS path, leading slash guaranteed and trailing slash removed. */
    private static String normalize(String path) {
        if (path == null || path.isBlank()) {
            return "";
        }
        String trimmed = path.trim();
        if (!trimmed.startsWith("/")) {
            trimmed = "/" + trimmed;
        }
        while (trimmed.length() > 1 && trimmed.endsWith("/")) {
            trimmed = trimmed.substring(0, trimmed.length() - 1);
        }
        return trimmed;
    }

    private static List<String> lowercase(List<String> values) {
        if (values == null) {
            return List.of();
        }
        return values.stream()
                .filter(value -> value != null && !value.isBlank())
                .map(value -> value.trim().toLowerCase(Locale.ROOT))
                .toList();
    }
}
