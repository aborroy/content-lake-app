package org.hyland.contentlake.connector.sharepoint;

import org.hyland.contentlake.spi.SourceNode;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class SharePointScopeResolverTest {

    private static SourceNode file(String path, String mimeType) {
        return new SourceNode("b!d:i", "src", "sharepoint", "name", path, mimeType, null, false,
                Set.of(), Set.of(), Map.of());
    }

    private static SourceNode folder(String path) {
        return new SourceNode("b!d:f", "src", "sharepoint", "name", path, null, null, true,
                Set.of(), Set.of(), Map.of());
    }

    @Test
    void admitsEverythingWhenNothingIsConfigured() {
        SharePointScopeResolver resolver =
                new SharePointScopeResolver(List.of(), List.of(), List.of(), List.of());

        assertThat(resolver.isInScope(file("/Finance/report.docx", "application/pdf"))).isTrue();
        assertThat(resolver.shouldTraverse(folder("/Finance"))).isTrue();
    }

    @Test
    void appliesExcludePathsAfterIncludePaths() {
        SharePointScopeResolver resolver = new SharePointScopeResolver(
                List.of("/Finance"), List.of("/Finance/Drafts"), List.of(), List.of());

        assertThat(resolver.isInScope(file("/Finance/report.docx", "application/pdf"))).isTrue();
        assertThat(resolver.isInScope(file("/Finance/Drafts/wip.docx", "application/pdf"))).isFalse();
        assertThat(resolver.isInScope(file("/Legal/contract.docx", "application/pdf"))).isFalse();
    }

    @Test
    void doesNotMatchAPathPrefixThatIsOnlyAStringPrefix() {
        SharePointScopeResolver resolver = new SharePointScopeResolver(
                List.of("/Finance"), List.of(), List.of(), List.of());

        // /Financials is not inside /Finance, and a naive startsWith would say it was.
        assertThat(resolver.isInScope(file("/Financials/report.docx", "application/pdf"))).isFalse();
        assertThat(resolver.isInScope(file("/Finance", "application/pdf"))).isTrue();
    }

    @Test
    void traversesAFolderThatAnIncludePatternDoesNotItselfMatch() {
        SharePointScopeResolver resolver = new SharePointScopeResolver(
                List.of("/Finance/Reports"), List.of(), List.of(), List.of());

        // /Finance has to stay traversable or /Finance/Reports is unreachable, even though /Finance is not
        // itself in scope.
        assertThat(resolver.shouldTraverse(folder("/Finance"))).isTrue();
        assertThat(resolver.isInScope(file("/Finance/summary.docx", "application/pdf"))).isFalse();
        assertThat(resolver.isInScope(file("/Finance/Reports/q3.docx", "application/pdf"))).isTrue();
    }

    @Test
    void stopsDescendingOnlyAtAnExcludedFolder() {
        SharePointScopeResolver resolver = new SharePointScopeResolver(
                List.of(), List.of("/Archive"), List.of(), List.of());

        assertThat(resolver.shouldTraverse(folder("/Archive"))).isFalse();
        assertThat(resolver.shouldTraverse(folder("/Archive/2026"))).isFalse();
        assertThat(resolver.shouldTraverse(folder("/Current"))).isTrue();
    }

    @Test
    void neverTraversesADocument() {
        SharePointScopeResolver resolver =
                new SharePointScopeResolver(List.of(), List.of(), List.of(), List.of());

        assertThat(resolver.shouldTraverse(file("/Finance/report.docx", "application/pdf"))).isFalse();
    }

    @Test
    void matchesMimeTypeWildcards() {
        SharePointScopeResolver resolver = new SharePointScopeResolver(
                List.of(), List.of(), List.of("application/pdf", "text/*"), List.of());

        assertThat(resolver.isInScope(file("/a.pdf", "application/pdf"))).isTrue();
        assertThat(resolver.isInScope(file("/a.md", "text/markdown"))).isTrue();
        assertThat(resolver.isInScope(file("/a.png", "image/png"))).isFalse();
    }

    @Test
    void excludesAMimeTypeEvenWhenAnIncludePatternAdmitsIt() {
        SharePointScopeResolver resolver = new SharePointScopeResolver(
                List.of(), List.of(), List.of("text/*"), List.of("text/csv"));

        assertThat(resolver.isInScope(file("/a.txt", "text/plain"))).isTrue();
        assertThat(resolver.isInScope(file("/a.csv", "text/csv"))).isFalse();
    }

    @Test
    void ignoresMimePatternsForFolders() {
        SharePointScopeResolver resolver = new SharePointScopeResolver(
                List.of(), List.of(), List.of("application/pdf"), List.of());

        // A folder has no content type to test, and excluding folders would make every document under them
        // unreachable.
        assertThat(resolver.isInScope(folder("/Finance"))).isTrue();
    }

    @Test
    void admitsANodeWithNoPathBecauseADeltaResponseHasNone() {
        SharePointScopeResolver resolver = new SharePointScopeResolver(
                List.of("/Finance"), List.of("/Archive"), List.of(), List.of());

        // Graph omits parentReference.path from delta results. Dropping everything the feed reported would
        // look exactly like a broken feed, so a null path is admitted and the tradeoff is documented.
        assertThat(resolver.isInScope(file(null, "application/pdf"))).isTrue();
        assertThat(resolver.shouldTraverse(folder(null))).isTrue();
    }

    @Test
    void treatsAMissingMimeTypeAsNotMatchingAnIncludePattern() {
        SharePointScopeResolver resolver = new SharePointScopeResolver(
                List.of(), List.of(), List.of("application/pdf"), List.of());

        assertThat(resolver.isInScope(file("/a.bin", null))).isFalse();
    }
}
