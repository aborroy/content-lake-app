package org.hyland.contentlake.rag.security;

import org.hyland.contentlake.security.CallerIdentities;
import org.hyland.contentlake.security.SourceIdentity;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The exact strings and per-source answers this token is required to produce.
 *
 * <p>The behaviour-preservation lock for caller identity. {@code getName()} is a rate-limit bucket key and the
 * stored author of a feedback row, and the per-source answers decide which documents a caller sees, so a
 * change in any of them has to fail here rather than reach a caller's results.</p>
 */
class MultiIdentityAuthenticationTest {

    /** What {@code DualSourceAuthenticationFilter} builds: Alfresco as the fallback, Nuxeo added. */
    private static MultiIdentityAuthentication bothRepositories() {
        return new MultiIdentityAuthentication(CallerIdentities.builder()
                .fallback(SourceIdentity.of("alfresco", "alice"))
                .add(SourceIdentity.of("nuxeo", "bob"))
                .build());
    }

    @Test
    void resolvesEachRepositoryToItsOwnPrincipal() {
        CallerIdentities identities = bothRepositories().identities();

        assertThat(identities.usernameFor("alfresco")).isEqualTo("alice");
        assertThat(identities.usernameFor("nuxeo")).isEqualTo("bob");
    }

    @Test
    void sendsAThirdSourceTypeToTheAlfrescoPrincipal() {
        CallerIdentities identities = bothRepositories().identities();

        // The ternary this replaced had nowhere else to send a third type, so it used the Alfresco username.
        // Preserved deliberately: dropping the source instead would remove documents from results.
        assertThat(identities.usernameFor("sharepoint")).isEqualTo("alice");
        assertThat(identities.usernameFor("cmis")).isEqualTo("alice");
    }

    @Test
    void namesItselfAsTheTokenItReplacesAlwaysDid() {
        MultiIdentityAuthentication both = bothRepositories();

        // getName() is a rate-limit bucket key and a stored feedback author, so this string cannot drift.
        assertThat(both.getName()).isEqualTo("alfresco:alice|nuxeo:bob");
        assertThat(both.getName()).isEqualTo(both.identities().describe());
    }

    @Test
    void reportsThePrincipalTheTokenItReplacesReported() {
        // Bare "alice", not the composite name: getPrincipal() and getName() answered differently before and
        // still do.
        assertThat(bothRepositories().getPrincipal()).isEqualTo("alice");
        assertThat(bothRepositories().getPrincipal())
                .isEqualTo(bothRepositories().identities().primaryUsername());
    }

    @Test
    void aSingleIdentityStaysBareSoNothingIsReBucketed() {
        MultiIdentityAuthentication single =
                new MultiIdentityAuthentication(CallerIdentities.single("alice"));

        assertThat(single.getName()).isEqualTo("alice");
        assertThat(single.getPrincipal()).isEqualTo("alice");
        assertThat(single.identities().usernameFor("anything")).isEqualTo("alice");
    }

    @Test
    void agreesWithItselfWhenOnlyOneSideIsPresent() {
        // Unreachable through the filter, which needs both credentials. Asserted so the one-sided shape
        // cannot silently disagree with the composite one.
        MultiIdentityAuthentication alfrescoOnly = new MultiIdentityAuthentication(
                CallerIdentities.builder().fallback(SourceIdentity.of("alfresco", "alice")).build());

        assertThat(alfrescoOnly.getName()).isEqualTo("alfresco:alice");
        assertThat(alfrescoOnly.identities().usernameFor("nuxeo")).isEqualTo("alice");
    }

    @Test
    void isAuthenticatedAndCarriesTheUserRole() {
        MultiIdentityAuthentication both = bothRepositories();

        assertThat(both.isAuthenticated()).isTrue();
        assertThat(both.getCredentials()).isNull();
        assertThat(both.getAuthorities()).extracting(Object::toString).containsExactly("ROLE_USER");
    }

    @Test
    void equalityIsDefinedAndCoversTheWholeIdentitySet() {
        assertThat(bothRepositories()).isEqualTo(bothRepositories());
        assertThat(bothRepositories()).hasSameHashCodeAs(bothRepositories());

        // Both report primary username "alice", so comparing principals alone would call these equal. They
        // are trimmed to different documents, so they are different callers.
        assertThat(bothRepositories())
                .isNotEqualTo(new MultiIdentityAuthentication(CallerIdentities.single("alice")));
        assertThat(bothRepositories()).isNotEqualTo(new MultiIdentityAuthentication(
                CallerIdentities.builder()
                        .fallback(SourceIdentity.of("alfresco", "alice"))
                        .add(SourceIdentity.of("nuxeo", "carol"))
                        .build()));
    }
}
