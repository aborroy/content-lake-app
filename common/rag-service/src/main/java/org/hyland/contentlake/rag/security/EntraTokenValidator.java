package org.hyland.contentlake.rag.security;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.security.oauth2.core.DelegatingOAuth2TokenValidator;
import org.springframework.security.oauth2.core.OAuth2TokenValidator;
import org.springframework.security.oauth2.core.OAuth2TokenValidatorResult;
import org.springframework.security.oauth2.core.OAuth2Error;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.security.oauth2.jwt.JwtIssuerValidator;
import org.springframework.security.oauth2.jwt.JwtTimestampValidator;
import org.springframework.security.oauth2.jwt.JwtValidationException;
import org.springframework.security.oauth2.jwt.NimbusJwtDecoder;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Validates an Entra-issued bearer token and hands back its claims.
 *
 * <p>Four checks, and the third is the one that matters most:</p>
 *
 * <ul>
 *   <li><strong>Signature</strong>, against the tenant's JWKS. {@link NimbusJwtDecoder} fetches and caches the
 *       key set and refetches on an unknown key id, so a signing-key rotation does not need a restart.</li>
 *   <li><strong>Issuer</strong>, pinned to the tenant. A token from another directory is not our caller.</li>
 *   <li><strong>Audience</strong>, pinned to <em>our own</em> application. This is why the registration
 *       publishes an {@code access_as_user} scope at all. Without it, any token a user can obtain for anything
 *       else in the tenant, a Microsoft Graph token most obviously, could be replayed here and accepted. The
 *       signature and the issuer would both be perfectly valid; only the audience distinguishes a token minted
 *       for us from one minted for someone else.</li>
 *   <li><strong>Expiry and {@code nbf}</strong>, by {@link JwtTimestampValidator}.</li>
 * </ul>
 *
 * <p>Registered only when caller authentication is switched on, and it refuses to start without the values it
 * needs, following {@code EntraGroupResolver}: a validator that cannot verify anything must not be in the chain
 * pretending to.</p>
 */
@Slf4j
@Component
@ConditionalOnProperty(name = "rag.security.entra.caller-auth.enabled", havingValue = "true")
public class EntraTokenValidator {

    private final JwtDecoder decoder;

    // @Autowired is required rather than decoration: there is a second, package-private constructor for
    // tests, and Spring infers a constructor only when there is exactly one.
    @Autowired
    public EntraTokenValidator(
            @Value("${rag.security.entra.tenant-id:}") String tenantId,
            @Value("${rag.security.entra.caller-auth.client-id:}") String clientId,
            @Value("${rag.security.entra.caller-auth.audience:}") String audience,
            @Value("${rag.security.entra.caller-auth.issuer:}") String issuer,
            @Value("${rag.security.entra.caller-auth.jwks-uri:}") String jwksUri) {
        String tenant = trimToEmpty(tenantId);
        String client = trimToEmpty(clientId);
        if (tenant.isEmpty() || client.isEmpty()) {
            throw new IllegalStateException("rag.security.entra.caller-auth.enabled is true, so "
                    + "rag.security.entra.tenant-id and rag.security.entra.caller-auth.client-id are required");
        }
        String resolvedIssuer = issuer.isBlank()
                ? "https://login.microsoftonline.com/" + tenant + "/v2.0"
                : issuer.trim();
        String resolvedJwks = jwksUri.isBlank()
                ? "https://login.microsoftonline.com/" + tenant + "/discovery/v2.0/keys"
                : jwksUri.trim();
        // api://<client-id> is what "Expose an API" defaults the Application ID URI to, so it is the audience
        // unless the tenant chose another one.
        String resolvedAudience = audience.isBlank() ? "api://" + client : audience.trim();

        log.info("Entra caller authentication active: issuer={} audience={} jwks={}",
                resolvedIssuer, resolvedAudience, resolvedJwks);
        this.decoder = decoder(resolvedJwks, resolvedIssuer, resolvedAudience);
    }

    /** Test seam: a decoder already pointed at a local key set. */
    EntraTokenValidator(JwtDecoder decoder) {
        this.decoder = decoder;
    }

    static JwtDecoder decoder(String jwksUri, String issuer, String audience) {
        NimbusJwtDecoder nimbus = NimbusJwtDecoder.withJwkSetUri(jwksUri).build();
        nimbus.setJwtValidator(new DelegatingOAuth2TokenValidator<>(
                new JwtTimestampValidator(),
                new JwtIssuerValidator(issuer),
                audienceValidator(audience)));
        return nimbus;
    }

    /**
     * Rejects a token whose {@code aud} does not name this application.
     *
     * <p>Written out rather than taken from a helper because the failure mode is silent acceptance: a validator
     * that passes when the claim is absent would accept exactly the tokens this check exists to reject.</p>
     */
    private static OAuth2TokenValidator<Jwt> audienceValidator(String audience) {
        return jwt -> {
            List<String> presented = jwt.getAudience();
            if (presented != null && presented.contains(audience)) {
                return OAuth2TokenValidatorResult.success();
            }
            log.debug("Rejecting a token whose audience is {} rather than {}", presented, audience);
            return OAuth2TokenValidatorResult.failure(new OAuth2Error(
                    "invalid_token", "The token audience does not name this application", null));
        };
    }

    /**
     * The token's claims, or {@code null} when it is not a valid token for this application.
     *
     * <p>Never throws. What a caller does about an invalid token is the authenticator's decision, not this
     * class's, and a decode failure is not distinguishable here from a token addressed elsewhere.</p>
     */
    Jwt validate(String token) {
        try {
            return decoder.decode(token);
        } catch (JwtValidationException e) {
            // Issuer, audience, expiry: the token parsed and its signature held, but a claim did not.
            log.debug("Rejecting a bearer token: {}", e.getMessage());
            return null;
        } catch (Exception e) {
            // Malformed, tampered, unknown signing key, or the JWKS endpoint was unreachable.
            log.debug("Rejecting an undecodable bearer token: {}", e.getMessage());
            return null;
        }
    }

    /** The claim used as the caller's principal, read from a validated token. */
    static String principal(Jwt jwt, String claim) {
        Object value = jwt.getClaim(claim);
        if (value instanceof String text && !text.isBlank()) {
            return text.trim();
        }
        // A subject is present in every token, but it is pairwise per application and matches no ACL, so it
        // is deliberately not a fallback. An absent principal claim is a configuration error, not a caller
        // to be guessed at.
        log.warn("A validated token carries no usable '{}' claim, so the caller cannot be identified; "
                + "sub is deliberately not substituted because it matches no stored principal", claim);
        return null;
    }

    private static String trimToEmpty(String value) {
        return value == null ? "" : value.trim();
    }
}
