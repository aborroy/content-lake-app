package org.hyland.contentlake.connector.filesystem;

import org.hyland.contentlake.spi.ScopeResolver;
import org.hyland.contentlake.spi.SourceNode;

import java.util.List;
import java.util.Locale;

/**
 * Which files are ingested and which directories are descended into.
 *
 * <p>A file is in scope when it matches the configured extensions, if any were configured, and is not
 * excluded. A directory is traversed unless it is hidden or matches an exclusion.</p>
 *
 * <p>Note that a directory is never <em>in scope</em> while still being traversable: a directory is not a
 * document here, but everything worth ingesting is underneath one.</p>
 */
public final class FileSystemScopeResolver implements ScopeResolver {

    private final FileSystemConnectorSettings settings;

    public FileSystemScopeResolver(FileSystemConnectorSettings settings) {
        this.settings = settings;
    }

    @Override
    public boolean isInScope(SourceNode node) {
        if (node == null || node.folder()) {
            return false;
        }
        if (isExcluded(node)) {
            return false;
        }
        List<String> includeExtensions = settings.includeExtensions();
        if (includeExtensions.isEmpty()) {
            return true;
        }
        String ext = extensionOf(node.name());
        return ext != null && includeExtensions.stream().anyMatch(e -> e.equalsIgnoreCase(ext));
    }

    @Override
    public boolean shouldTraverse(SourceNode node) {
        return node != null && node.folder() && !isExcluded(node);
    }

    private boolean isExcluded(SourceNode node) {
        String name = node.name();
        // Hidden entries are always skipped, which is what keeps .git out without anyone configuring it.
        if (name != null && name.startsWith(".")) {
            return true;
        }
        String path = node.nodeId();
        List<String> excludePatterns = settings.excludePatterns();
        if (path == null || excludePatterns.isEmpty()) {
            return false;
        }
        return excludePatterns.stream()
                .filter(pattern -> pattern != null && !pattern.isBlank())
                .anyMatch(path::contains);
    }

    private static String extensionOf(String name) {
        if (name == null) {
            return null;
        }
        int dot = name.lastIndexOf('.');
        return dot >= 0 && dot < name.length() - 1
                ? name.substring(dot + 1).toLowerCase(Locale.ROOT)
                : null;
    }
}
