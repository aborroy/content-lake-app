package org.hyland.contentlake.connector.cmis;

import org.hyland.contentlake.spi.SourceNode;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Path and MIME scope, which is the whole of what a CMIS source can be narrowed by: there is no
 * {@code cl:indexed} equivalent to read the decision from the repository.
 */
class CmisScopeResolverTest {

    private static SourceNode document(String path, String mimeType) {
        return new SourceNode("id", "repo", "cmis", "doc.txt", path, mimeType, null, false,
                Set.of("__Everyone__"), Set.of(), java.util.Map.of());
    }

    private static SourceNode folder(String path) {
        return new SourceNode("id", "repo", "cmis", "folder", path, null, null, true,
                Set.of("__Everyone__"), Set.of(), java.util.Map.of());
    }

    private static CmisScopeResolver resolver(List<String> include, List<String> exclude) {
        return new CmisScopeResolver(include, exclude, List.of(), List.of());
    }

    @Test
    void withNothingConfiguredEverythingIsInScope() {
        CmisScopeResolver resolver = resolver(List.of(), List.of());

        assertThat(resolver.isInScope(document("/Sites/marketing", "text/plain"))).isTrue();
        assertThat(resolver.shouldTraverse(folder("/anything"))).isTrue();
    }

    @Test
    void includePatternsActAsAWhitelist() {
        CmisScopeResolver resolver = resolver(List.of("/Sites/marketing"), List.of());

        assertThat(resolver.isInScope(document("/Sites/marketing", "text/plain"))).isTrue();
        assertThat(resolver.isInScope(document("/Sites/marketing/2026", "text/plain"))).isTrue();
        assertThat(resolver.isInScope(document("/Sites/finance", "text/plain"))).isFalse();
    }

    @Test
    void excludeWinsOverInclude() {
        CmisScopeResolver resolver = resolver(List.of("/Sites"), List.of("/Sites/archive"));

        assertThat(resolver.isInScope(document("/Sites/marketing", "text/plain"))).isTrue();
        assertThat(resolver.isInScope(document("/Sites/archive", "text/plain"))).isFalse();
        assertThat(resolver.isInScope(document("/Sites/archive/2019", "text/plain"))).isFalse();
        assertThat(resolver.shouldTraverse(folder("/Sites/archive"))).isFalse();
    }

    @Test
    void prefixesMatchWholeSegmentsOnly() {
        // A plain startsWith would ingest /Sites/marketing-archive under an include of /Sites/marketing,
        // and the surprise would be silent: documents from a folder nobody named, in the index.
        CmisScopeResolver resolver = resolver(List.of("/Sites/marketing"), List.of());

        assertThat(resolver.isInScope(document("/Sites/marketing-archive", "text/plain"))).isFalse();
        assertThat(resolver.shouldTraverse(folder("/Sites/marketing-archive"))).isFalse();
    }

    @Test
    void traversesTheAncestorsOfADeeperIncludePattern() {
        // The one that matters most: without this a deep include pattern is unreachable and the pass
        // ingests nothing, which reads as an empty repository rather than as a scope mistake.
        CmisScopeResolver resolver = resolver(List.of("/Sites/marketing/docs"), List.of());

        assertThat(resolver.shouldTraverse(folder("/Sites"))).isTrue();
        assertThat(resolver.shouldTraverse(folder("/Sites/marketing"))).isTrue();
        assertThat(resolver.shouldTraverse(folder("/Sites/marketing/docs"))).isTrue();
        assertThat(resolver.shouldTraverse(folder("/Sites/marketing/docs/2026"))).isTrue();
        assertThat(resolver.shouldTraverse(folder("/Sites/finance"))).isFalse();

        // In scope is a different question from traversed: /Sites itself holds nothing wanted.
        assertThat(resolver.isInScope(document("/Sites", "text/plain"))).isFalse();
    }

    @Test
    void mimeIncludesAcceptATrailingWildcard() {
        CmisScopeResolver resolver =
                new CmisScopeResolver(List.of(), List.of(), List.of("text/*", "application/pdf"), List.of());

        assertThat(resolver.isInScope(document("/x", "text/plain"))).isTrue();
        assertThat(resolver.isInScope(document("/x", "text/markdown"))).isTrue();
        assertThat(resolver.isInScope(document("/x", "application/pdf"))).isTrue();
        assertThat(resolver.isInScope(document("/x", "image/png"))).isFalse();
    }

    @Test
    void mimeExcludesAreAppliedAfterIncludes() {
        CmisScopeResolver resolver =
                new CmisScopeResolver(List.of(), List.of(), List.of("*"), List.of("image/*"));

        assertThat(resolver.isInScope(document("/x", "text/plain"))).isTrue();
        assertThat(resolver.isInScope(document("/x", "image/png"))).isFalse();
    }

    @Test
    void mimeRulesNeverApplyToFolders() {
        // A folder has no content type, so a MIME whitelist that excluded folders would stop the walk at
        // the first one and ingest nothing below it.
        CmisScopeResolver resolver =
                new CmisScopeResolver(List.of(), List.of(), List.of("application/pdf"), List.of());

        assertThat(resolver.isInScope(folder("/Sites"))).isTrue();
        assertThat(resolver.shouldTraverse(folder("/Sites"))).isTrue();
    }

    @Test
    void patternsToleratePathsWrittenWithoutALeadingSlashOrWithATrailingOne() {
        CmisScopeResolver resolver = resolver(List.of("Sites/marketing/"), List.of());

        assertThat(resolver.isInScope(document("/Sites/marketing/q1", "text/plain"))).isTrue();
    }

    @Test
    void aNullNodeIsNeitherInScopeNorTraversed() {
        CmisScopeResolver resolver = resolver(List.of(), List.of());

        assertThat(resolver.isInScope(null)).isFalse();
        assertThat(resolver.shouldTraverse(null)).isFalse();
    }
}
