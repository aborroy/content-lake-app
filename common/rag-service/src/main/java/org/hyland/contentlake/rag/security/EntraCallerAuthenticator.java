package org.hyland.contentlake.rag.security;

import lombok.extern.slf4j.Slf4j;
import org.hyland.contentlake.security.CallerAuthenticator;
import org.hyland.contentlake.security.CallerCredentials;
import org.hyland.contentlake.security.CallerIdentities;
import org.hyland.contentlake.security.SourceIdentity;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.security.authentication.BadCredentialsException;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.stereotype.Component;

/**
 * Identifies a SharePoint user from an Entra-issued bearer token.
 *
 * <p>Off by default and absent when off. Reached only for a credential carrying
 * {@link BearerTokenAuthenticationFilter#BEARER_ATTRIBUTE}, so it never sees a password and no password
 * authenticator ever sees a token.</p>
 *
 * <h3>Why the principal is the {@code oid} claim</h3>
 *
 * <p>{@code SharePointAclMapper} emits three principals for a named user grant: the Entra object id, and the
 * {@code userPrincipalName} and {@code email} where each is present. A caller matching any one of them is
 * enough, so the question is which is safest to take from the token, not which the connector prefers.</p>
 *
 * <p>The object id, because it is the only one the connector always emits and the only one that cannot change.
 * {@code #142} established that a real {@code sharePointIdentity} carries {@code email} and no
 * {@code userPrincipalName}, which is why both addresses are stored; taking either of them here would make
 * trimming depend on which one that tenant happens to send. A user who renames keeps their object id, where
 * their address does not survive.</p>
 *
 * <p>{@code sub} is deliberately never a fallback: it is pairwise per application, so it matches no stored
 * principal and would silently trim a valid caller to nothing.</p>
 *
 * <p>The claim is configurable for a corpus addressed some other way, and the default is asserted by a test,
 * because getting it wrong shows a caller an empty result set with no error rather than failing loudly.</p>
 *
 * <h3>Why the identity is typed, unlike every other authenticator here</h3>
 *
 * <p>An object id is a GUID. As an untyped identity it would be used as the caller's username against every
 * source, where it matches nothing and contributes clauses that cannot ever match. Typed as
 * {@code sharepoint:<oid>} it scopes to the source whose ACLs actually hold object ids, and every other source
 * is dropped because this caller has no identity for it. Both are fail-closed; the typed one is the honest
 * one.</p>
 *
 * <p>That makes {@code getName()} read {@code sharepoint:<oid>}, which is a rate-limit bucket key and a stored
 * feedback author. Acceptable here and not in the CMIS case, because this is a new sign-in path: no existing
 * token bucket or feedback row was ever written under an Entra identity, so nothing is re-bucketed or
 * orphaned.</p>
 */
@Slf4j
@Component
@ConditionalOnProperty(name = "rag.security.entra.caller-auth.enabled", havingValue = "true")
public class EntraCallerAuthenticator implements CallerAuthenticator {

    private final EntraTokenValidator validator;
    private final String sourceType;
    private final String principalClaim;

    public EntraCallerAuthenticator(
            EntraTokenValidator validator,
            @Value("${rag.security.entra.source-type:sharepoint}") String sourceType,
            @Value("${rag.security.entra.caller-auth.principal-claim:oid}") String principalClaim) {
        this.validator = validator;
        this.sourceType = sourceType == null || sourceType.isBlank() ? "sharepoint" : sourceType.trim();
        this.principalClaim = principalClaim == null || principalClaim.isBlank()
                ? "oid" : principalClaim.trim();
    }

    @Override
    public String id() {
        return "entra-bearer";
    }

    @Override
    public int order() {
        return CallerAuthenticatorOrder.ENTRA;
    }

    @Override
    public boolean supports(CallerCredentials credentials) {
        return credentials.attribute(BearerTokenAuthenticationFilter.BEARER_ATTRIBUTE) != null;
    }

    /**
     * The caller's identity, from the token and nothing else.
     *
     * <p>Throws rather than declining. A bearer token whose audience names this application was unambiguously
     * addressed here, so an invalid one is a rejection, exactly as an expired Alfresco ticket is. Declining
     * would fall through to authenticators that cannot read a token at all and produce a rejection with a
     * worse message.</p>
     */
    @Override
    public CallerIdentities authenticate(CallerCredentials credentials) {
        String token = credentials.attribute(BearerTokenAuthenticationFilter.BEARER_ATTRIBUTE);
        Jwt jwt = validator.validate(token);
        if (jwt == null) {
            throw new BadCredentialsException("Invalid or expired bearer token");
        }
        String principal = EntraTokenValidator.principal(jwt, principalClaim);
        if (principal == null) {
            // Validated but unusable. Not a decline: the credential was ours to adjudicate and we cannot say
            // who the caller is, so serving them anything would be serving an unidentified caller.
            throw new BadCredentialsException(
                    "The bearer token carries no '" + principalClaim + "' claim to identify the caller");
        }
        log.debug("Authenticated an Entra caller as {}:{}", sourceType, principal);
        return CallerIdentities.builder()
                .add(SourceIdentity.of(sourceType, principal))
                .build();
    }
}
