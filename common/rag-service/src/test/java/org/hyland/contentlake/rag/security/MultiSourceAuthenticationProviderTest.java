package org.hyland.contentlake.rag.security;

import org.hyland.contentlake.security.CallerAuthenticator;
import org.hyland.contentlake.security.CallerCredentials;
import org.hyland.contentlake.security.CallerIdentities;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.security.authentication.BadCredentialsException;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The chain, and nothing about HTTP.
 *
 * <p>All this class does is run registered authenticators in order, so its test stubs them. The HTTP
 * validation these assertions used to reach through lives in {@link SourceDirectoryHttpTest}.</p>
 */
class MultiSourceAuthenticationProviderTest {

    /** Ids of the authenticators consulted, in the order they were asked. */
    private final List<String> consulted = new ArrayList<>();

    private CallerAuthenticator stub(String id, int order,
                                    Function<CallerCredentials, CallerIdentities> answer) {
        return new CallerAuthenticator() {
            @Override
            public String id() {
                return id;
            }

            @Override
            public int order() {
                return order;
            }

            @Override
            public boolean supports(CallerCredentials credentials) {
                return true;
            }

            @Override
            public CallerIdentities authenticate(CallerCredentials credentials) {
                consulted.add(id);
                return answer.apply(credentials);
            }
        };
    }

    private CallerAuthenticator declining(String id, int order) {
        return stub(id, order, credentials -> null);
    }

    private CallerAuthenticator accepting(String id, int order, String username) {
        return stub(id, order, credentials -> CallerIdentities.single(username));
    }

    private static Authentication login(String principal, String password) {
        return new UsernamePasswordAuthenticationToken(principal, password);
    }

    @Test
    void consultsAuthenticatorsInAscendingOrderRegardlessOfBeanOrder() {
        // Registered out of order deliberately: the chain's order is the declared one, not Spring's.
        var provider = new MultiSourceAuthenticationProvider(List.of(
                declining("third", 30), declining("first", 10), accepting("second", 20, "alice")));

        assertThat(provider.authenticatorIds()).containsExactly("first", "second", "third");

        Authentication authenticated = provider.authenticate(login("alice", "secret"));

        assertThat(authenticated.getName()).isEqualTo("alice");
        // Stopped at the one that accepted; the later authority was never shown the credentials.
        assertThat(consulted).containsExactly("first", "second");
    }

    @Test
    void aDeclineFallsThroughToTheNextAuthority() {
        var provider = new MultiSourceAuthenticationProvider(List.of(
                declining("alfresco", 10), accepting("nuxeo", 20, "bob")));

        assertThat(provider.authenticate(login("bob", "secret")).getName()).isEqualTo("bob");
        assertThat(consulted).containsExactly("alfresco", "nuxeo");
    }

    @Test
    void everyAuthorityDeclining_isOneRejectionAtTheEnd() {
        var provider = new MultiSourceAuthenticationProvider(List.of(
                declining("alfresco", 10), declining("nuxeo", 20)));

        assertThatThrownBy(() -> provider.authenticate(login("nobody", "secret")))
                .isInstanceOf(BadCredentialsException.class)
                .hasMessageContaining("nobody");
        assertThat(consulted).containsExactly("alfresco", "nuxeo");
    }

    @Test
    void anAuthenticationExceptionPropagatesAndStopsTheChain() {
        var provider = new MultiSourceAuthenticationProvider(List.of(
                stub("alfresco-ticket", 10, credentials -> {
                    throw new BadCredentialsException("Invalid or expired Alfresco ticket");
                }),
                accepting("nuxeo", 20, "bob")));

        assertThatThrownBy(() -> provider.authenticate(login("TICKET_expired", "")))
                .isInstanceOf(BadCredentialsException.class)
                .hasMessageContaining("Alfresco ticket");
        // The credential was addressed to the first authority, so the second never saw it.
        assertThat(consulted).containsExactly("alfresco-ticket");
    }

    @Test
    void anUnsupportedCredentialIsSkippedWithoutBeingAdjudicated() {
        CallerAuthenticator neverSupports = new CallerAuthenticator() {
            @Override
            public String id() {
                return "abstains";
            }

            @Override
            public int order() {
                return 10;
            }

            @Override
            public boolean supports(CallerCredentials credentials) {
                return false;
            }

            @Override
            public CallerIdentities authenticate(CallerCredentials credentials) {
                consulted.add(id());
                return CallerIdentities.single("should-not-happen");
            }
        };
        var provider = new MultiSourceAuthenticationProvider(List.of(
                neverSupports, accepting("nuxeo", 20, "bob")));

        assertThat(provider.authenticate(login("bob", "secret")).getName()).isEqualTo("bob");
        assertThat(consulted).containsExactly("nuxeo");
    }

    @Test
    void anEmptyIdentitySetIsADeclineRatherThanAnAuthenticatedCaller() {
        var provider = new MultiSourceAuthenticationProvider(List.of(
                stub("empty", 10, credentials -> CallerIdentities.EMPTY),
                accepting("nuxeo", 20, "bob")));

        assertThat(provider.authenticate(login("bob", "secret")).getName()).isEqualTo("bob");
        assertThat(consulted).containsExactly("empty", "nuxeo");
    }

    @Test
    void thePasswordReachesTheAuthenticatorAndANullOneBecomesBlank() {
        List<CallerCredentials> seen = new ArrayList<>();
        var provider = new MultiSourceAuthenticationProvider(List.of(
                stub("recording", 10, credentials -> {
                    seen.add(credentials);
                    return CallerIdentities.single(credentials.principal());
                })));

        provider.authenticate(login("alice", "secret"));
        provider.authenticate(new UsernamePasswordAuthenticationToken("TICKET_x", null));

        assertThat(seen.get(0).password()).isEqualTo("secret");
        assertThat(seen.get(1).password()).isEmpty();
        assertThat(seen.get(1).hasNoPassword()).isTrue();
    }

    @Nested
    class WithCmisRegistered {

        /** The three shipped orders, so these cases exercise the real positions rather than invented ones. */
        private MultiSourceAuthenticationProvider providerWith(CallerAuthenticator... extra) {
            List<CallerAuthenticator> all = new ArrayList<>(List.of(
                    declining("alfresco-password", CallerAuthenticatorOrder.ALFRESCO),
                    declining("nuxeo-password", CallerAuthenticatorOrder.NUXEO)));
            all.addAll(List.of(extra));
            return new MultiSourceAuthenticationProvider(all);
        }

        @Test
        void cmisIsReachedWhenTheOtherTwoDecline() {
            var provider = providerWith(
                    accepting("cmis-password", CallerAuthenticatorOrder.CMIS, "alice"));

            assertThat(provider.authenticate(login("alice", "secret")).getName()).isEqualTo("alice");
            assertThat(consulted).containsExactly("alfresco-password", "nuxeo-password", "cmis-password");
        }

        @Test
        void cmisIsNotConsultedWhenAlfrescoAccepts() {
            var provider = new MultiSourceAuthenticationProvider(List.of(
                    accepting("alfresco-password", CallerAuthenticatorOrder.ALFRESCO, "alice"),
                    declining("nuxeo-password", CallerAuthenticatorOrder.NUXEO),
                    declining("cmis-password", CallerAuthenticatorOrder.CMIS)));

            assertThat(provider.authenticate(login("alice", "secret")).getName()).isEqualTo("alice");
            // The caller's password is never shown to a third repository once an earlier authority accepted.
            assertThat(consulted).containsExactly("alfresco-password");
        }
    }

    @Test
    void supportsTheTwoRequestShapesAndNotAResult() {
        var provider = new MultiSourceAuthenticationProvider(List.of(declining("alfresco", 10)));

        assertThat(provider.supports(UsernamePasswordAuthenticationToken.class)).isTrue();
        assertThat(provider.supports(PresentedCredentialsAuthentication.class)).isTrue();
        // ProviderManager holds this provider alone, so a token rejected here is a 401 with no authenticator
        // consulted. A result is not a request and must never be re-adjudicated.
        assertThat(provider.supports(MultiIdentityAuthentication.class)).isFalse();
    }

    @Nested
    class CredentialsThatAreNotAUsernameAndPassword {

        @Test
        void reachTheChainWithTheirAttributesIntact() {
            List<CallerCredentials> seen = new ArrayList<>();
            var provider = new MultiSourceAuthenticationProvider(List.of(
                    stub("bearer", 40, credentials -> {
                        seen.add(credentials);
                        return CallerIdentities.single("alice");
                    })));

            provider.authenticate(new PresentedCredentialsAuthentication(
                    new CallerCredentials("", "", Map.of("bearer", "a.jwt.value"))));

            // The attribute is the whole point: it is how a token travels without a PREFIX:: hack in the
            // principal or a password field that another authenticator would forward to a repository.
            assertThat(seen).hasSize(1);
            assertThat(seen.get(0).attribute("bearer")).isEqualTo("a.jwt.value");
            assertThat(seen.get(0).principal()).isEmpty();
            assertThat(seen.get(0).hasNoPassword()).isTrue();
        }

        @Test
        void carryNoClaimedPrincipalBeforeValidation() {
            var request = new PresentedCredentialsAuthentication(
                    new CallerCredentials("", "", Map.of("bearer", "a.jwt.value")));

            // #162's failure mode: a caller-supplied name beside a caller-supplied token is two unbound
            // inputs, and one caller can then be served another's results.
            assertThat(request.getName()).isEmpty();
            assertThat(request.isAuthenticated()).isFalse();
        }

        @Test
        void cannotBeMarkedAuthenticated() {
            var request = new PresentedCredentialsAuthentication(CallerCredentials.of("", ""));

            assertThatThrownBy(() -> request.setAuthenticated(true))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThat(request.isAuthenticated()).isFalse();
        }

        @Test
        void areStillRejectedOnceWhenEveryAuthorityDeclines() {
            var provider = new MultiSourceAuthenticationProvider(List.of(declining("bearer", 40)));

            assertThatThrownBy(() -> provider.authenticate(new PresentedCredentialsAuthentication(
                    new CallerCredentials("", "", Map.of("bearer", "expired")))))
                    .isInstanceOf(BadCredentialsException.class);
        }
    }
}
