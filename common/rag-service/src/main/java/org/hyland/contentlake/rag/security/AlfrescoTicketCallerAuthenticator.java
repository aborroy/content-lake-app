package org.hyland.contentlake.rag.security;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.hyland.contentlake.security.CallerAuthenticator;
import org.hyland.contentlake.security.CallerCredentials;
import org.hyland.contentlake.security.CallerIdentities;
import org.springframework.security.authentication.BadCredentialsException;
import org.springframework.stereotype.Component;

/**
 * An Alfresco UI ticket, presented as the principal with no password.
 *
 * <p>The one authenticator that throws rather than declining. A {@code TICKET_} principal is unambiguously
 * addressed to Alfresco, so an invalid or expired one is a rejection, not a credential another authority
 * might recognise. That is the behaviour the fixed chain had and it is preserved exactly, including for an
 * unconfigured repository: a ticket presented to a deployment with no Alfresco URL is still a rejected
 * ticket, not a fall-through to a password login against Nuxeo.</p>
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class AlfrescoTicketCallerAuthenticator implements CallerAuthenticator {

    private final AlfrescoDirectory directory;

    @Override
    public String id() {
        return "alfresco-ticket";
    }

    @Override
    public int order() {
        return CallerAuthenticatorOrder.ALFRESCO;
    }

    @Override
    public boolean supports(CallerCredentials credentials) {
        return ReservedPrincipals.isAlfrescoTicketLogin(credentials);
    }

    @Override
    public CallerIdentities authenticate(CallerCredentials credentials) {
        String ticket = credentials.principal();
        String resolvedUsername = directory.validateTicket(ticket);
        if (resolvedUsername != null) {
            log.debug("Authenticated Alfresco ticket '{}' as '{}'", truncated(ticket), resolvedUsername);
            return CallerIdentities.single(resolvedUsername);
        }
        log.warn("Alfresco ticket validation failed for '{}'", truncated(ticket));
        throw new BadCredentialsException("Invalid or expired Alfresco ticket");
    }

    /** Tickets are credentials, so only enough of one to correlate a log line is written out. */
    private static String truncated(String ticket) {
        return ticket.substring(0, Math.min(ticket.length(), 20));
    }
}
