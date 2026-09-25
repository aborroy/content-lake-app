package org.hyland.contentlake.rag.security;

import org.hyland.contentlake.security.CallerCredentials;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.security.authentication.BadCredentialsException;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/**
 * The four Alfresco and Nuxeo authenticators: which credentials each claims, and whether a failure is a
 * decline or a rejection.
 *
 * <p>The distinction is the point of these tests. A marker-prefixed credential was addressed to one authority,
 * so an invalid one is a rejection; a password another authority might accept is a decline.</p>
 */
@ExtendWith(MockitoExtension.class)
class SourceCallerAuthenticatorTest {

    @Mock AlfrescoDirectory alfrescoDirectory;
    @Mock NuxeoDirectory nuxeoDirectory;

    private static final String TOKEN_PREFIX = MultiSourceAuthenticationProvider.NUXEO_TOKEN_PRINCIPAL_PREFIX;

    @Nested
    class AlfrescoTicket {

        @Test
        void claimsATicketPrincipalWithNoPassword() {
            var authenticator = new AlfrescoTicketCallerAuthenticator(alfrescoDirectory);

            assertThat(authenticator.supports(CallerCredentials.of("TICKET_abc", ""))).isTrue();
            // A password means the caller meant an ordinary login and the name only looks like a ticket.
            assertThat(authenticator.supports(CallerCredentials.of("TICKET_abc", "secret"))).isFalse();
            assertThat(authenticator.supports(CallerCredentials.of("alice", ""))).isFalse();
            assertThat(authenticator.supports(CallerCredentials.of(TOKEN_PREFIX + "t", ""))).isFalse();
        }

        @Test
        void resolvesTheUsernameBehindAValidTicket() {
            when(alfrescoDirectory.validateTicket("TICKET_abc")).thenReturn("rag-user");

            var identities = new AlfrescoTicketCallerAuthenticator(alfrescoDirectory)
                    .authenticate(CallerCredentials.of("TICKET_abc", ""));

            assertThat(identities.describe()).isEqualTo("rag-user");
            // Untyped, so it answers for every source exactly as the plain token it replaces did.
            assertThat(identities.usernameFor("alfresco")).isEqualTo("rag-user");
            assertThat(identities.usernameFor("cmis")).isEqualTo("rag-user");
        }

        @Test
        void rejectsRatherThanDecliningAnInvalidTicket() {
            when(alfrescoDirectory.validateTicket("TICKET_expired")).thenReturn(null);

            assertThatThrownBy(() -> new AlfrescoTicketCallerAuthenticator(alfrescoDirectory)
                    .authenticate(CallerCredentials.of("TICKET_expired", "")))
                    .isInstanceOf(BadCredentialsException.class)
                    .hasMessageContaining("Alfresco ticket");
        }
    }

    @Nested
    class NuxeoToken {

        @Test
        void claimsATokenPrincipalWithNoPassword() {
            var authenticator = new NuxeoTokenCallerAuthenticator(nuxeoDirectory);

            assertThat(authenticator.supports(CallerCredentials.of(TOKEN_PREFIX + "t", ""))).isTrue();
            assertThat(authenticator.supports(CallerCredentials.of(TOKEN_PREFIX + "t", "secret"))).isFalse();
            assertThat(authenticator.supports(CallerCredentials.of("TICKET_abc", ""))).isFalse();
        }

        @Test
        void stripsThePrefixBeforeAskingTheDirectory() {
            when(nuxeoDirectory.validateToken("raw-token")).thenReturn("rag-user");

            var identities = new NuxeoTokenCallerAuthenticator(nuxeoDirectory)
                    .authenticate(CallerCredentials.of(TOKEN_PREFIX + "raw-token", ""));

            assertThat(identities.describe()).isEqualTo("rag-user");
        }

        @Test
        void rejectsRatherThanDecliningAnInvalidToken() {
            when(nuxeoDirectory.validateToken("dead")).thenReturn(null);

            assertThatThrownBy(() -> new NuxeoTokenCallerAuthenticator(nuxeoDirectory)
                    .authenticate(CallerCredentials.of(TOKEN_PREFIX + "dead", "")))
                    .isInstanceOf(BadCredentialsException.class)
                    .hasMessageContaining("Nuxeo authentication token");
        }
    }

    @Nested
    class PasswordLogins {

        @Test
        void claimEverythingThatIsNotAReservedPrincipal() {
            var alfresco = new AlfrescoPasswordCallerAuthenticator(alfrescoDirectory);
            var nuxeo = new NuxeoPasswordCallerAuthenticator(nuxeoDirectory);

            assertThat(alfresco.supports(CallerCredentials.of("alice", "secret"))).isTrue();
            assertThat(nuxeo.supports(CallerCredentials.of("alice", "secret"))).isTrue();
            // A ticket must never be forwarded as a username: it would land in another access log.
            assertThat(alfresco.supports(CallerCredentials.of("TICKET_abc", ""))).isFalse();
            assertThat(nuxeo.supports(CallerCredentials.of("TICKET_abc", ""))).isFalse();
            assertThat(alfresco.supports(CallerCredentials.of(TOKEN_PREFIX + "t", ""))).isFalse();
            assertThat(nuxeo.supports(CallerCredentials.of(TOKEN_PREFIX + "t", ""))).isFalse();
            // But a ticket-shaped name presented with a password is an ordinary login again.
            assertThat(alfresco.supports(CallerCredentials.of("TICKET_abc", "secret"))).isTrue();
        }

        @Test
        void produceAnUntypedIdentityOnSuccess() {
            when(alfrescoDirectory.authenticate("alice", "secret")).thenReturn(true);

            var identities = new AlfrescoPasswordCallerAuthenticator(alfrescoDirectory)
                    .authenticate(CallerCredentials.of("alice", "secret"));

            // Bare "alice", not "alfresco:alice": this string is a rate-limit bucket key and a stored
            // feedback author, so typing it would re-bucket live callers and orphan existing rows.
            assertThat(identities.describe()).isEqualTo("alice");
            assertThat(identities.usernameFor("nuxeo")).isEqualTo("alice");
        }

        @Test
        void declineRatherThanThrowSoALaterAuthorityStillSeesTheCredential() {
            when(alfrescoDirectory.authenticate("alice", "secret")).thenReturn(false);

            assertThat(new AlfrescoPasswordCallerAuthenticator(alfrescoDirectory)
                    .authenticate(CallerCredentials.of("alice", "secret"))).isNull();
        }

        @Test
        void nuxeoDeclinesLikewiseAndNamesItselfDistinctly() {
            when(nuxeoDirectory.authenticate("alice", "secret")).thenReturn(false);
            var nuxeo = new NuxeoPasswordCallerAuthenticator(nuxeoDirectory);

            assertThat(nuxeo.authenticate(CallerCredentials.of("alice", "secret"))).isNull();
            assertThat(nuxeo.id()).isEqualTo("nuxeo-password");
            verifyNoInteractions(alfrescoDirectory);
        }
    }

    @Nested
    class ChainOrder {

        @Test
        void putsAlfrescoAheadOfNuxeoSoExistingDeploymentsAreUnchanged() {
            assertThat(new AlfrescoTicketCallerAuthenticator(alfrescoDirectory).order())
                    .isLessThan(new NuxeoTokenCallerAuthenticator(nuxeoDirectory).order());
            assertThat(new AlfrescoPasswordCallerAuthenticator(alfrescoDirectory).order())
                    .isLessThan(new NuxeoPasswordCallerAuthenticator(nuxeoDirectory).order());
        }

        @Test
        void idsAreDistinctBecauseTheyBreakTiesAndAppearInLogs() {
            assertThat(List.of(
                    new AlfrescoTicketCallerAuthenticator(alfrescoDirectory).id(),
                    new AlfrescoPasswordCallerAuthenticator(alfrescoDirectory).id(),
                    new NuxeoTokenCallerAuthenticator(nuxeoDirectory).id(),
                    new NuxeoPasswordCallerAuthenticator(nuxeoDirectory).id()))
                    .doesNotHaveDuplicates();
        }
    }

    @Nested
    class AReservedPrincipalNeverReachesTheWrongAuthority {

        @Test
        void soAPasswordAuthenticatorMakesNoCallForATicket() {
            var alfresco = new AlfrescoPasswordCallerAuthenticator(alfrescoDirectory);
            CallerCredentials ticket = CallerCredentials.of("TICKET_abc", "");

            // supports() is false, so the provider never calls authenticate(); asserted here as the
            // property rather than the mechanism, since it is what keeps the ticket out of the call.
            assertThat(alfresco.supports(ticket)).isFalse();
            verify(alfrescoDirectory, never()).authenticate("TICKET_abc", "");
        }
    }
}
