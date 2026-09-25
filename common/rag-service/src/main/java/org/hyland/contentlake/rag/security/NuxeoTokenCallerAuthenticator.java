package org.hyland.contentlake.rag.security;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.hyland.contentlake.security.CallerAuthenticator;
import org.hyland.contentlake.security.CallerCredentials;
import org.hyland.contentlake.security.CallerIdentities;
import org.springframework.security.authentication.BadCredentialsException;
import org.springframework.stereotype.Component;

/**
 * A Nuxeo authentication token, presented as a prefixed principal with no password.
 *
 * <p>Throws rather than declining, for the reason {@link AlfrescoTicketCallerAuthenticator} does: the prefix
 * says the credential was addressed to Nuxeo, so an invalid token is a rejection.</p>
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class NuxeoTokenCallerAuthenticator implements CallerAuthenticator {

    private final NuxeoDirectory directory;

    @Override
    public String id() {
        return "nuxeo-token";
    }

    @Override
    public int order() {
        return CallerAuthenticatorOrder.NUXEO;
    }

    @Override
    public boolean supports(CallerCredentials credentials) {
        return ReservedPrincipals.isNuxeoTokenLogin(credentials);
    }

    @Override
    public CallerIdentities authenticate(CallerCredentials credentials) {
        String token = credentials.principal()
                .substring(MultiSourceAuthenticationProvider.NUXEO_TOKEN_PRINCIPAL_PREFIX.length());
        String resolvedUsername = directory.validateToken(token);
        if (resolvedUsername != null) {
            log.debug("Authenticated Nuxeo token as '{}'", resolvedUsername);
            return CallerIdentities.single(resolvedUsername);
        }
        log.warn("Nuxeo token validation failed");
        throw new BadCredentialsException("Invalid or expired Nuxeo authentication token");
    }
}
