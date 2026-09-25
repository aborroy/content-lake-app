package org.hyland.contentlake.rag.security;

import org.hyland.contentlake.security.CallerCredentials;
import org.springframework.security.authentication.AbstractAuthenticationToken;

import java.util.List;

/**
 * An unauthenticated request carrying credentials that are not a username and password.
 *
 * <p>Spring's {@code BasicAuthenticationFilter} produces a {@code UsernamePasswordAuthenticationToken}, which
 * is all {@link MultiSourceAuthenticationProvider} could accept. A bearer token is neither a username nor a
 * password, and the two ways of forcing it into that shape are both worse than a second token type: putting it
 * in the principal behind a {@code PREFIX::} marker, which is the hack the ticket and Nuxeo-token paths already
 * pay for twice, or putting it in the password field, where every authenticator that tries an ordinary login
 * would forward it to a repository as a password.</p>
 *
 * <p>So the credential travels as a {@link CallerCredentials} attribute, which is what that map exists for, and
 * this token is the envelope that gets it to the provider. It carries no principal: the principal is whatever
 * the authenticator derives from the credential, which for a signed token means the token alone decides who the
 * caller is.</p>
 *
 * <p>Always unauthenticated. {@link MultiIdentityAuthentication} is what replaces it on success.</p>
 */
public class PresentedCredentialsAuthentication extends AbstractAuthenticationToken {

    private final CallerCredentials credentials;

    public PresentedCredentialsAuthentication(CallerCredentials credentials) {
        // List.of() rather than null: Spring Security 7 added a builder constructor, so a bare super(null)
        // no longer compiles. No authorities anyway, since nothing has been validated yet.
        super(List.of());
        this.credentials = credentials;
        setAuthenticated(false);
    }

    /** What the caller presented, read by the provider instead of a name and password. */
    public CallerCredentials credentials() {
        return credentials;
    }

    @Override
    public Object getCredentials() {
        return credentials;
    }

    /**
     * Empty, deliberately.
     *
     * <p>There is no claimed identity to report before the credential has been validated. Anything else here
     * would be a caller-supplied name travelling beside a caller-supplied token, which is the two-unbound-inputs
     * shape {@code #162} recorded as a way for one caller to be served another's results.</p>
     */
    @Override
    public Object getPrincipal() {
        return "";
    }

    @Override
    public void setAuthenticated(boolean authenticated) {
        if (authenticated) {
            throw new IllegalArgumentException(
                    "PresentedCredentialsAuthentication is a request, not a result; it is never authenticated");
        }
        super.setAuthenticated(false);
    }
}
