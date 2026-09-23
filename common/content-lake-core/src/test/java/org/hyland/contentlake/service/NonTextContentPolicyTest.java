package org.hyland.contentlake.service;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The deny list's matching rules.
 *
 * <p>The cases that matter are the ones where the MIME type is useless: a detached signature arrives as
 * {@code application/octet-stream}, which is also what a source reports for anything it could not identify, so
 * only the extension separates a document from a blob.</p>
 */
class NonTextContentPolicyTest {

    private final NonTextContentPolicy policy = NonTextContentPolicy.defaults();

    @Test
    void deniesTheSignatureSidecarThatEndedARealCrawl() {
        // The file from the OneDrive run: two dots, and an octet-stream type that says nothing.
        assertThat(policy.cannotContainText("application/octet-stream", "Multilanguage.docx_signed.csig")).isTrue();
        assertThat(policy.reasonFor("application/octet-stream", "Multilanguage.docx_signed.csig"))
                .isEqualTo("its extension .csig cannot contain text");
    }

    @Test
    void admitsAnUnidentifiedFileThatIsNotDeniedByExtension() {
        // application/octet-stream is not itself denied, because a source reports it for anything it could not
        // identify. Denying the type would lose every such document rather than one blob.
        assertThat(policy.cannotContainText("application/octet-stream", "minutes.docx")).isFalse();
        assertThat(policy.cannotContainText("application/octet-stream", "no-extension")).isFalse();
    }

    @Test
    void admitsADocumentWhenNeitherTypeNorNameIsKnown() {
        // "We know nothing about it" is not "it cannot contain text". Guessing here loses documents.
        assertThat(policy.cannotContainText(null, null)).isFalse();
        assertThat(policy.cannotContainText("", "  ")).isFalse();
    }

    @Test
    void deniesByMimeTypeEvenWhenTheNameIsUnhelpful() {
        assertThat(policy.cannotContainText("application/zip", "bundle")).isTrue();
        assertThat(policy.reasonFor("application/zip", "bundle"))
                .isEqualTo("its type application/zip cannot contain text");
    }

    @Test
    void ignoresAMimeTypeParameterAndCase() {
        assertThat(policy.cannotContainText("Application/ZIP; charset=binary", "bundle")).isTrue();
    }

    @Test
    void takesTheLastExtensionRatherThanTheFirst() {
        // "report.csig.docx" is a document; "report.docx.csig" is a signature. The last dot decides.
        assertThat(policy.cannotContainText(null, "report.docx.csig")).isTrue();
        assertThat(policy.cannotContainText(null, "report.csig.docx")).isFalse();
    }

    @Test
    void treatsADotfileAsHavingNoExtension() {
        // ".key" as a whole file name is a dotfile, not a private key, and the leading dot is not a separator.
        assertThat(policy.cannotContainText(null, ".key")).isFalse();
        assertThat(policy.cannotContainText(null, "server.key")).isTrue();
    }

    @Test
    void treatsATrailingDotAsHavingNoExtension() {
        assertThat(policy.cannotContainText(null, "archive.")).isFalse();
    }

    @Test
    void admitsEveryTypeThePipelineTreatsAsText() {
        // The deny list must not overlap the text short circuit, or a markdown file would be skipped.
        assertThat(policy.cannotContainText("text/plain", "notes.txt")).isFalse();
        assertThat(policy.cannotContainText("text/markdown", "incident-log.md")).isFalse();
        assertThat(policy.cannotContainText("application/json", "payload.json")).isFalse();
        assertThat(policy.cannotContainText("application/pdf", "quarterly-report.pdf")).isFalse();
    }

    @Test
    void normalisesConfiguredValuesSoAPlausibleConfigurationMatches() {
        NonTextContentPolicy configured = NonTextContentPolicy.of(
                List.of(" Application/X-Custom ; charset=binary "),
                List.of(".SIG", "blob"));

        assertThat(configured.deniedMimeTypes()).containsExactly("application/x-custom");
        assertThat(configured.deniedExtensions()).containsExactlyInAnyOrder("sig", "blob");
        assertThat(configured.cannotContainText("application/x-custom", "x")).isTrue();
        assertThat(configured.cannotContainText(null, "detached.sig")).isTrue();
    }

    @Test
    void anEmptyPolicyDeniesNothing() {
        NonTextContentPolicy none = NonTextContentPolicy.allowEverything();
        assertThat(none.cannotContainText("application/zip", "bundle.zip")).isFalse();
        assertThat(none.reasonFor("application/zip", "bundle.zip")).isNull();
    }

    @Test
    void toleratesNullListsRatherThanFailingConstruction() {
        NonTextContentPolicy nulls = new NonTextContentPolicy(null, null);
        assertThat(nulls.deniedMimeTypes()).isEmpty();
        assertThat(nulls.deniedExtensions()).isEmpty();
        assertThat(nulls.cannotContainText("application/zip", "bundle.zip")).isFalse();
    }

    @Test
    void isImmutableEvenWhenBuiltFromAMutableSet() {
        Set<String> mutable = new java.util.HashSet<>(Set.of("csig"));
        NonTextContentPolicy built = new NonTextContentPolicy(Set.of(), mutable);
        mutable.add("docx");
        assertThat(built.cannotContainText(null, "minutes.docx")).isFalse();
    }
}
