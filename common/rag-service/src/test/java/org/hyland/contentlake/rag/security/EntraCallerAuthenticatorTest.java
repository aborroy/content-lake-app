package org.hyland.contentlake.rag.security;

import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.crypto.RSASSASigner;
import com.nimbusds.jose.jwk.JWKSet;
import com.nimbusds.jose.jwk.RSAKey;
import com.nimbusds.jose.jwk.gen.RSAKeyGenerator;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
import com.sun.net.httpserver.HttpServer;
import org.hyland.contentlake.security.CallerCredentials;
import org.hyland.contentlake.security.CallerIdentities;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.security.authentication.BadCredentialsException;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Date;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Entra caller authentication, against real signed tokens and a real JWKS endpoint.
 *
 * <p>Signed with a generated key rather than mocked, because what is being tested is whether a token is
 * accepted, and a mocked decoder would assert the checks against themselves. The tenant is not needed for any
 * of this: only its published values are, and those are what the app registration supplies.</p>
 */
class EntraCallerAuthenticatorTest {

    private static final String ISSUER = "https://login.microsoftonline.com/tenant-abc/v2.0";
    private static final String AUDIENCE = "api://client-def";
    private static final String KEY_ID = "test-signing-key";
    private static final String OID = "8f14e45f-ceea-467a-9c1e-1b2c3d4e5f60";

    private HttpServer jwks;
    private RSAKey signingKey;

    @BeforeEach
    void startJwks() throws Exception {
        signingKey = new RSAKeyGenerator(2048).keyID(KEY_ID).generate();
        byte[] body = new JWKSet(signingKey.toPublicJWK()).toString().getBytes(StandardCharsets.UTF_8);
        jwks = HttpServer.create(new InetSocketAddress(0), 0);
        jwks.createContext("/keys", exchange -> {
            try {
                exchange.getResponseHeaders().set("Content-Type", "application/json");
                exchange.sendResponseHeaders(200, body.length);
                exchange.getResponseBody().write(body);
            } finally {
                exchange.close();
            }
        });
        jwks.start();
    }

    @AfterEach
    void stopJwks() {
        if (jwks != null) {
            jwks.stop(0);
        }
    }

    @Nested
    class AValidToken {

        @Test
        void authenticatesTheCallerAsTheObjectIdClaim() throws Exception {
            CallerIdentities identities = authenticator().authenticate(bearer(validClaims().build()));

            assertThat(identities).isNotNull();
            assertThat(identities.usernameFor("sharepoint")).isEqualTo(OID);
        }

        @Test
        void yieldsATypedIdentityScopedToTheSharePointSource() throws Exception {
            CallerIdentities identities = authenticator().authenticate(bearer(validClaims().build()));

            // Typed, unlike every other authenticator here. A GUID is meaningless as an Alfresco or Nuxeo
            // username, so the other sources are dropped rather than filtered against something that cannot
            // match.
            assertThat(identities.describe()).isEqualTo("sharepoint:" + OID);
            assertThat(identities.usernameFor("alfresco")).isNull();
            assertThat(identities.usernameFor("nuxeo")).isNull();
        }
    }

    @Nested
    class TokensThatMustBeRejected {

        @Test
        void oneMintedForMicrosoftGraphRatherThanForUs() throws Exception {
            // The check the access_as_user scope exists for. Signature and issuer are both valid here; only
            // the audience distinguishes a token minted for us from one a user can obtain for anything else.
            JWTClaimsSet graphToken = validClaims()
                    .audience("https://graph.microsoft.com")
                    .build();

            assertThatThrownBy(() -> authenticator().authenticate(bearer(graphToken)))
                    .isInstanceOf(BadCredentialsException.class)
                    .hasMessageContaining("bearer token");
        }

        @Test
        void oneFromAnotherDirectory() throws Exception {
            JWTClaimsSet otherTenant = validClaims()
                    .issuer("https://login.microsoftonline.com/someone-else/v2.0")
                    .build();

            assertThatThrownBy(() -> authenticator().authenticate(bearer(otherTenant)))
                    .isInstanceOf(BadCredentialsException.class);
        }

        @Test
        void anExpiredOne() throws Exception {
            JWTClaimsSet expired = validClaims()
                    .expirationTime(Date.from(Instant.now().minusSeconds(3600)))
                    .build();

            assertThatThrownBy(() -> authenticator().authenticate(bearer(expired)))
                    .isInstanceOf(BadCredentialsException.class);
        }

        @Test
        void oneNotYetValid() throws Exception {
            JWTClaimsSet future = validClaims()
                    .notBeforeTime(Date.from(Instant.now().plusSeconds(3600)))
                    .build();

            assertThatThrownBy(() -> authenticator().authenticate(bearer(future)))
                    .isInstanceOf(BadCredentialsException.class);
        }

        @Test
        void aTamperedOne() throws Exception {
            String token = signed(validClaims().build());
            // Flip a character in the payload, leaving the signature over the original.
            int firstDot = token.indexOf('.');
            char tampered = token.charAt(firstDot + 5) == 'A' ? 'B' : 'A';
            String forged = token.substring(0, firstDot + 5) + tampered + token.substring(firstDot + 6);

            assertThatThrownBy(() -> authenticator().authenticate(bearerToken(forged)))
                    .isInstanceOf(BadCredentialsException.class);
        }

        @Test
        void oneSignedByAKeyTheTenantDoesNotPublish() throws Exception {
            RSAKey foreign = new RSAKeyGenerator(2048).keyID(KEY_ID).generate();
            SignedJWT jwt = new SignedJWT(
                    new JWSHeader.Builder(JWSAlgorithm.RS256).keyID(KEY_ID).build(), validClaims().build());
            jwt.sign(new RSASSASigner(foreign));

            assertThatThrownBy(() -> authenticator().authenticate(bearerToken(jwt.serialize())))
                    .isInstanceOf(BadCredentialsException.class);
        }

        @Test
        void oneCarryingNoPrincipalClaim() throws Exception {
            // Validated but unusable. Not served as an unidentified caller, and sub is deliberately not
            // substituted: it is pairwise per application and matches no stored principal.
            JWTClaimsSet noOid = new JWTClaimsSet.Builder()
                    .issuer(ISSUER)
                    .audience(AUDIENCE)
                    .subject("pairwise-subject-value")
                    .expirationTime(Date.from(Instant.now().plusSeconds(600)))
                    .build();

            assertThatThrownBy(() -> authenticator().authenticate(bearer(noOid)))
                    .isInstanceOf(BadCredentialsException.class)
                    .hasMessageContaining("oid");
        }
    }

    @Nested
    class WhatItClaims {

        @Test
        void onlyCredentialsCarryingABearerToken() throws Exception {
            EntraCallerAuthenticator authenticator = authenticator();

            assertThat(authenticator.supports(
                    new CallerCredentials("", "", Map.of("bearer", "x.y.z")))).isTrue();
            // A password login is never adjudicated here, and a token never reaches a password authenticator.
            assertThat(authenticator.supports(CallerCredentials.of("alice", "secret"))).isFalse();
            assertThat(authenticator.supports(CallerCredentials.of("TICKET_abc", ""))).isFalse();
        }

        @Test
        void isLastInTheChainAndSaysSo() throws Exception {
            assertThat(authenticator().order()).isEqualTo(CallerAuthenticatorOrder.ENTRA);
            assertThat(authenticator().order()).isGreaterThan(CallerAuthenticatorOrder.CMIS);
            assertThat(authenticator().id()).isEqualTo("entra-bearer");
        }
    }

    @Nested
    class TheConfiguredClaim {

        @Test
        void defaultsToOidRatherThanAnAddress() throws Exception {
            // Pinned because getting it wrong shows a caller an empty result set with no error. The connector
            // always emits the object id; a given tenant may send only one of upn and email.
            JWTClaimsSet both = validClaims()
                    .claim("userPrincipalName", "alice@contoso.com")
                    .claim("email", "alice.smith@contoso.com")
                    .build();

            assertThat(authenticator().authenticate(bearer(both)).usernameFor("sharepoint")).isEqualTo(OID);
        }

        @Test
        void canBeAnAddressForACorpusAddressedThatWay() throws Exception {
            JWTClaimsSet withEmail = validClaims().claim("email", "alice.smith@contoso.com").build();

            CallerIdentities identities = authenticatorUsingClaim("email").authenticate(bearer(withEmail));

            assertThat(identities.usernameFor("sharepoint")).isEqualTo("alice.smith@contoso.com");
        }
    }

    private EntraCallerAuthenticator authenticator() {
        return authenticatorUsingClaim("oid");
    }

    private EntraCallerAuthenticator authenticatorUsingClaim(String claim) {
        EntraTokenValidator validator = new EntraTokenValidator(
                EntraTokenValidator.decoder(jwksUri(), ISSUER, AUDIENCE));
        return new EntraCallerAuthenticator(validator, "sharepoint", claim);
    }

    private String jwksUri() {
        return "http://127.0.0.1:" + jwks.getAddress().getPort() + "/keys";
    }

    private static JWTClaimsSet.Builder validClaims() {
        return new JWTClaimsSet.Builder()
                .issuer(ISSUER)
                .audience(AUDIENCE)
                .subject("pairwise-subject-value")
                .claim("oid", OID)
                .issueTime(Date.from(Instant.now().minusSeconds(30)))
                .notBeforeTime(Date.from(Instant.now().minusSeconds(30)))
                .expirationTime(Date.from(Instant.now().plusSeconds(600)));
    }

    private CallerCredentials bearer(JWTClaimsSet claims) throws Exception {
        return bearerToken(signed(claims));
    }

    private static CallerCredentials bearerToken(String token) {
        return new CallerCredentials("", "",
                Map.of(BearerTokenAuthenticationFilter.BEARER_ATTRIBUTE, token));
    }

    private String signed(JWTClaimsSet claims) throws Exception {
        SignedJWT jwt = new SignedJWT(
                new JWSHeader.Builder(JWSAlgorithm.RS256).keyID(KEY_ID).build(), claims);
        jwt.sign(new RSASSASigner(signingKey));
        return jwt.serialize();
    }
}
