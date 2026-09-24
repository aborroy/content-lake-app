package org.hyland.contentlake.security;

import lombok.RequiredArgsConstructor;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Service;

/**
 * The caller's identities, whatever authenticated them.
 *
 * <p>The one place that knows a caller may carry more than one identity. Every other reader asks this
 * service instead of testing for a concrete token type, which is what previously made a third identity
 * source impossible to add without editing each of them.</p>
 *
 * <p>A credential that proves a single login produces an ordinary Spring token with no per-source
 * knowledge. That is not a lesser case to be special-cased at each call site: it is one untyped identity,
 * which answers for every source type, and this service is where that equivalence is made.</p>
 */
@Service
@RequiredArgsConstructor
public class CallerIdentityService {

    private final SecurityContextService securityContextService;

    /**
     * The identities carried by this authentication, or the single untyped identity of the current caller.
     *
     * <p>The fallback path goes through {@link SecurityContextService#getCurrentUsername()}, which refuses
     * to invent a principal, so an anonymous or blank-named caller still produces a 401 rather than a
     * filter scoped to nobody.</p>
     */
    public CallerIdentities identities(Authentication authentication) {
        if (authentication instanceof CallerAuthentication caller) {
            return caller.identities();
        }
        return CallerIdentities.single(securityContextService.getCurrentUsername());
    }

    /** The identities of the caller on this request thread. */
    public CallerIdentities currentIdentities() {
        return identities(SecurityContextHolder.getContext().getAuthentication());
    }
}
