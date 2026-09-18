package org.hyland.contentlake.connector.filesystem;

import org.hyland.contentlake.spi.SourceNode;
import org.junit.jupiter.api.Test;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class FileSystemScopeResolverTest {

    private FileSystemScopeResolver resolver(List<String> includeExtensions, List<String> excludePatterns) {
        FileSystemConnectorSettings props = new FileSystemConnectorSettings(
                "/tmp", "fs-test", null, includeExtensions, excludePatterns, 100);
        return new FileSystemScopeResolver(props);
    }

    private SourceNode file(String path, String name) {
        return new SourceNode(path, "fs", "filesystem", name, "/parent", "text/plain",
                OffsetDateTime.now(), false, Set.of("__Everyone__"), Set.of(), Map.of());
    }

    private SourceNode folder(String path, String name) {
        return new SourceNode(path, "fs", "filesystem", name, path, null,
                OffsetDateTime.now(), true, Set.of("__Everyone__"), Set.of(), Map.of());
    }

    @Test
    void isInScope_allFilesWhenNoExtensionFilter() {
        FileSystemScopeResolver resolver = resolver(List.of(), List.of());
        assertThat(resolver.isInScope(file("/root/a.pdf", "a.pdf"))).isTrue();
        assertThat(resolver.isInScope(file("/root/b.bin", "b.bin"))).isTrue();
    }

    @Test
    void isInScope_restrictsToIncludeExtensions() {
        FileSystemScopeResolver resolver = resolver(List.of("pdf", "txt"), List.of());
        assertThat(resolver.isInScope(file("/root/a.pdf", "a.pdf"))).isTrue();
        assertThat(resolver.isInScope(file("/root/b.png", "b.png"))).isFalse();
    }

    @Test
    void isInScope_excludesFoldersAndHiddenAndPatternMatches() {
        FileSystemScopeResolver resolver = resolver(List.of(), List.of("node_modules"));
        assertThat(resolver.isInScope(folder("/root/sub", "sub"))).isFalse();
        assertThat(resolver.isInScope(file("/root/.secret", ".secret"))).isFalse();
        assertThat(resolver.isInScope(file("/root/node_modules/x.txt", "x.txt"))).isFalse();
    }

    @Test
    void shouldTraverse_skipsHiddenAndExcludedDirectories() {
        FileSystemScopeResolver resolver = resolver(List.of(), List.of(".git"));
        assertThat(resolver.shouldTraverse(folder("/root/docs", "docs"))).isTrue();
        assertThat(resolver.shouldTraverse(folder("/root/.hidden", ".hidden"))).isFalse();
        assertThat(resolver.shouldTraverse(folder("/root/.git", ".git"))).isFalse();
        assertThat(resolver.shouldTraverse(file("/root/a.txt", "a.txt"))).isFalse();
    }
}
