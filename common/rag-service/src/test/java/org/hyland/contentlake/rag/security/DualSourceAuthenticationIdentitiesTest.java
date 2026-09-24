package org.hyland.contentlake.rag.security;

import org.hyland.contentlake.security.CallerIdentities;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * That the identity set this token now exposes answers exactly what the token itself answered.
 *
 * <p>This is the behaviour-preservation lock for the caller-identity refactor: the permission filter is about
 * to be built from {@link CallerIdentities} instead of from the two getters, so every answer below has to
 * match what {@code isNuxeoSource(sourceId) ? getNuxeoUsername() : getAlfrescoUsername()} produced.</p>
 */
class DualSourceAuthenticationIdentitiesTest {

    @Test
    void resolvesEachRepositoryToItsOwnPrincipal() {
        CallerIdentities identities = new DualSourceAuthentication("alice", "bob").identities();

        assertThat(identities.usernameFor("alfresco")).isEqualTo("alice");
        assertThat(identities.usernameFor("nuxeo")).isEqualTo("bob");
    }

    @Test
    void sendsAThirdSourceTypeToTheAlfrescoPrincipal() {
        CallerIdentities identities = new DualSourceAuthentication("alice", "bob").identities();

        // The ternary had nowhere else to send a third type, so it used the Alfresco username. Preserved
        // deliberately: dropping the source instead would remove documents from results.
        assertThat(identities.usernameFor("sharepoint")).isEqualTo("alice");
        assertThat(identities.usernameFor("cmis")).isEqualTo("alice");
    }

    @Test
    void describesItselfAsTheTokenAlwaysHas() {
        DualSourceAuthentication both = new DualSourceAuthentication("alice", "bob");

        // getName() is a rate-limit bucket key and a stored feedback author, so this string cannot drift.
        assertThat(both.identities().describe()).isEqualTo(both.getName());
        assertThat(both.identities().describe()).isEqualTo("alfresco:alice|nuxeo:bob");
    }

    @Test
    void agreesWithGetNameWhenOnlyOneSideIsPresent() {
        // Unreachable in a deployment: DualSourceAuthenticationFilter is the only producer and it needs
        // both credentials. Asserted so the mapping cannot silently disagree with getName() if that changes.
        DualSourceAuthentication alfrescoOnly = new DualSourceAuthentication("alice", null);
        DualSourceAuthentication nuxeoOnly = new DualSourceAuthentication(null, "bob");

        assertThat(alfrescoOnly.identities().describe()).isEqualTo(alfrescoOnly.getName()).isEqualTo("alice");
        assertThat(nuxeoOnly.identities().describe()).isEqualTo(nuxeoOnly.getName()).isEqualTo("bob");
    }

    @Test
    void reportsThePrincipalTheTokenReports() {
        assertThat(new DualSourceAuthentication("alice", "bob").identities().primaryUsername())
                .isEqualTo(new DualSourceAuthentication("alice", "bob").getPrincipal());
    }
}
