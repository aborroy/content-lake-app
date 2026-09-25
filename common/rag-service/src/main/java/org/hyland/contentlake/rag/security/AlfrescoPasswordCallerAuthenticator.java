package org.hyland.contentlake.rag.security;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.hyland.contentlake.security.CallerAuthenticator;
import org.hyland.contentlake.security.CallerCredentials;
import org.hyland.contentlake.security.CallerIdentities;
import org.springframework.stereotype.Component;

/**
 * An ordinary username and password, validated against Alfresco.
 *
 * <p>Declines rather than throwing: a password Alfresco does not accept may still be a valid Nuxeo password,
 * which is what the {@code tryAlfrescoAuth(..) || tryNuxeoAuth(..)} this replaces expressed. The caller is
 * rejected once, by the provider, after every authority has declined.</p>
 *
 * <p>The identity is untyped, as it has always been. Typing it as {@code alfresco:alice} would change
 * {@code getName()}, which is a rate-limit bucket key and a stored feedback author.</p>
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class AlfrescoPasswordCallerAuthenticator implements CallerAuthenticator {

    private final AlfrescoDirectory directory;

    @Override
    public String id() {
        return "alfresco-password";
    }

    @Override
    public int order() {
        return CallerAuthenticatorOrder.ALFRESCO;
    }

    @Override
    public boolean supports(CallerCredentials credentials) {
        // Everything that is not a credential addressed to a specific authority by its marker.
        return !ReservedPrincipals.isReserved(credentials);
    }

    @Override
    public CallerIdentities authenticate(CallerCredentials credentials) {
        if (!directory.authenticate(credentials.principal(), credentials.password())) {
            return null;
        }
        log.debug("Authenticated user '{}' via Alfresco", credentials.principal());
        return CallerIdentities.single(credentials.principal());
    }
}
