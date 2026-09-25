package org.hyland.contentlake.rag.security;

import org.hyland.contentlake.security.AlfrescoTicketHeader;
import org.hyland.contentlake.security.CallerCredentials;

/**
 * The two principal forms that are a whole credential rather than a username.
 *
 * <p>An Alfresco ticket and a Nuxeo token are presented as the principal with an empty password, which makes
 * them indistinguishable from a username unless something tests for the marker. Two rules follow, and both
 * are security properties rather than tidiness:</p>
 *
 * <ul>
 *   <li>The authenticator that owns a marker claims it, so the credential reaches the authority it was
 *       addressed to.</li>
 *   <li>Every other authenticator declines it without an HTTP call. Forwarding an Alfresco ticket to another
 *       system as a username would put it in that system's access log.</li>
 * </ul>
 *
 * <p>A non-empty password means the caller meant an ordinary login and the value only looks like a ticket, so
 * the marker tests require no password.</p>
 */
final class ReservedPrincipals {

    private ReservedPrincipals() {
    }

    static boolean isAlfrescoTicketLogin(CallerCredentials credentials) {
        return credentials.principalStartsWith(AlfrescoTicketHeader.TICKET_PREFIX)
                && credentials.hasNoPassword();
    }

    static boolean isNuxeoTokenLogin(CallerCredentials credentials) {
        return credentials.principalStartsWith(MultiSourceAuthenticationProvider.NUXEO_TOKEN_PRINCIPAL_PREFIX)
                && credentials.hasNoPassword();
    }

    /** Whether the principal is a credential addressed to a specific authority rather than a username. */
    static boolean isReserved(CallerCredentials credentials) {
        return isAlfrescoTicketLogin(credentials) || isNuxeoTokenLogin(credentials);
    }
}
