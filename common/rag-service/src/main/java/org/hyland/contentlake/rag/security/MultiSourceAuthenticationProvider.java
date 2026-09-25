package org.hyland.contentlake.rag.security;

import lombok.extern.slf4j.Slf4j;
import org.hyland.contentlake.security.CallerAuthenticator;
import org.hyland.contentlake.security.CallerCredentials;
import org.hyland.contentlake.security.CallerIdentities;
import org.springframework.security.authentication.AuthenticationProvider;
import org.springframework.security.authentication.BadCredentialsException;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.AuthenticationException;
import org.springframework.stereotype.Component;

import java.util.Comparator;
import java.util.List;

/**
 * Identifies a caller by asking each registered {@link CallerAuthenticator} in turn.
 *
 * <p>What this used to be: a fixed chain of {@code if} branches naming Alfresco's tickets API and Nuxeo's
 * {@code /me}, with nowhere to put a third authority. Now it knows only how to run a chain, and every
 * authority is a bean. Adding one is a new {@code CallerAuthenticator}, not an edit here.</p>
 *
 * <p>The loop is the whole policy, and it has exactly three exits:</p>
 *
 * <ul>
 *   <li>An authenticator returns identities: the caller is authenticated as those, and nothing further is
 *       consulted.</li>
 *   <li>An authenticator throws: the credential was addressed to that authority and is invalid, so it
 *       propagates and no other authority is shown the credential.</li>
 *   <li>Every authenticator declines: one rejection, at the end.</li>
 * </ul>
 *
 * <p>Order is ascending {@link CallerAuthenticator#order()}, which is a security property rather than a
 * detail: it decides which system is shown a caller's credentials first. {@code ApplicationContextLoadsTest}
 * pins the resolved order, because that is the only place it is protected.</p>
 */
@Slf4j
@Component
public class MultiSourceAuthenticationProvider implements AuthenticationProvider {

    /**
     * Keeps its name and package-private visibility: {@link NuxeoTokenAuthenticationFilter} builds a principal
     * with it, and {@link ReservedPrincipals} tests for it.
     */
    static final String NUXEO_TOKEN_PRINCIPAL_PREFIX = "NUXEO_TOKEN::";

    private final List<CallerAuthenticator> authenticators;

    public MultiSourceAuthenticationProvider(List<CallerAuthenticator> authenticators) {
        this.authenticators = authenticators.stream()
                .sorted(Comparator.comparingInt(CallerAuthenticator::order)
                        .thenComparing(CallerAuthenticator::id))
                .toList();
        log.info("Caller authentication chain: {}",
                this.authenticators.stream().map(CallerAuthenticator::id).toList());
    }

    /** The chain in the order it is consulted. Read by the test that pins that order. */
    public List<String> authenticatorIds() {
        return authenticators.stream().map(CallerAuthenticator::id).toList();
    }

    @Override
    public Authentication authenticate(Authentication authentication) throws AuthenticationException {
        Object presented = authentication.getCredentials();
        CallerCredentials credentials = CallerCredentials.of(
                authentication.getName(), presented == null ? "" : presented.toString());

        for (CallerAuthenticator authenticator : authenticators) {
            if (!authenticator.supports(credentials)) {
                continue;
            }
            CallerIdentities identities = authenticator.authenticate(credentials);
            if (identities != null && !identities.isEmpty()) {
                log.debug("Authenticated '{}' via {}", identities.describe(), authenticator.id());
                return new MultiIdentityAuthentication(identities);
            }
        }

        log.warn("Authentication failed for user '{}'", credentials.principal());
        throw new BadCredentialsException("Invalid credentials for user: " + credentials.principal());
    }

    /**
     * Only the token Spring's {@code BasicAuthenticationFilter} and the two header filters produce.
     *
     * <p>{@code ProviderManager} holds this provider alone, so any token class rejected here gets a
     * {@code ProviderNotFoundException} and a 401. An authenticator whose credential arrives as something
     * other than a username and password has to widen this first.</p>
     */
    @Override
    public boolean supports(Class<?> authentication) {
        return UsernamePasswordAuthenticationToken.class.isAssignableFrom(authentication);
    }
}
