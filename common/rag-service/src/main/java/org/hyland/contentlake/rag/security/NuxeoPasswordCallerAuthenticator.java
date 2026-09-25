package org.hyland.contentlake.rag.security;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.hyland.contentlake.security.CallerAuthenticator;
import org.hyland.contentlake.security.CallerCredentials;
import org.hyland.contentlake.security.CallerIdentities;
import org.springframework.stereotype.Component;

/**
 * An ordinary username and password, validated against Nuxeo.
 *
 * <p>Declines rather than throwing, and produces an untyped identity. See
 * {@link AlfrescoPasswordCallerAuthenticator} for both reasons; this is the second half of the
 * {@code tryAlfrescoAuth(..) || tryNuxeoAuth(..)} the provider used to hold.</p>
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class NuxeoPasswordCallerAuthenticator implements CallerAuthenticator {

    private final NuxeoDirectory directory;

    @Override
    public String id() {
        return "nuxeo-password";
    }

    @Override
    public int order() {
        return CallerAuthenticatorOrder.NUXEO;
    }

    @Override
    public boolean supports(CallerCredentials credentials) {
        return !ReservedPrincipals.isReserved(credentials);
    }

    @Override
    public CallerIdentities authenticate(CallerCredentials credentials) {
        if (!directory.authenticate(credentials.principal(), credentials.password())) {
            return null;
        }
        log.debug("Authenticated user '{}' via Nuxeo", credentials.principal());
        return CallerIdentities.single(credentials.principal());
    }
}
