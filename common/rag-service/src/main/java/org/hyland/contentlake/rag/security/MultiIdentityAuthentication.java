package org.hyland.contentlake.rag.security;

import org.hyland.contentlake.security.CallerAuthentication;
import org.hyland.contentlake.security.CallerIdentities;
import org.springframework.security.authentication.AbstractAuthenticationToken;
import org.springframework.security.core.authority.SimpleGrantedAuthority;

import java.util.List;

/**
 * An authenticated caller, carrying however many identities authenticated them.
 *
 * <p>One token for every shape, replacing a type per credential combination. A caller with a single login
 * carries one untyped identity, which is not a lesser case: it answers for every source type, exactly as the
 * plain token it replaces did.</p>
 *
 * <p>Extends {@link AbstractAuthenticationToken} rather than implementing {@link CallerAuthentication}
 * directly, which closes a latent gap in the type it supersedes: that one defined no {@code equals} or
 * {@code hashCode}, and {@code ProviderManager}'s {@code eraseCredentials} pass skips a token it does not
 * recognise as a {@code CredentialsContainer}.</p>
 *
 * <h3>Two strings that cannot drift</h3>
 *
 * <p>{@code getName()} is a rate-limit bucket key and the stored author of a feedback row, and
 * {@code getPrincipal()} is what the token it replaces reported. Both are pinned to what they were:
 * {@code getName()} is {@link CallerIdentities#describe()} and {@code getPrincipal()} is
 * {@link CallerIdentities#primaryUsername()}. For a single identity that makes both the bare username, so
 * live token buckets are not re-keyed and stored feedback rows are not orphaned.</p>
 */
public class MultiIdentityAuthentication extends AbstractAuthenticationToken implements CallerAuthentication {

    private final CallerIdentities identities;

    public MultiIdentityAuthentication(CallerIdentities identities) {
        super(List.of(new SimpleGrantedAuthority("ROLE_USER")));
        this.identities = identities;
        setAuthenticated(true);
    }

    @Override
    public CallerIdentities identities() {
        return identities;
    }

    @Override
    public Object getCredentials() {
        return null;
    }

    @Override
    public Object getPrincipal() {
        return identities.primaryUsername();
    }

    @Override
    public String getName() {
        return identities.describe();
    }
}
