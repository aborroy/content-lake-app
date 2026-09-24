package org.hyland.contentlake.security;

import org.springframework.security.core.Authentication;

/**
 * An {@link Authentication} that knows the caller's identity in each content source.
 *
 * <p>Exists so the query path can ask "who is this caller, per source?" without naming a concrete token
 * type. Before this, every place that needed a caller's second identity tested for one specific class, which
 * meant a third identity source could not be added without editing each of them.</p>
 *
 * <p>Read it through {@code CallerIdentityService} rather than with {@code instanceof} at the point of use:
 * a caller authenticated by a single credential does not implement this interface at all, and the service is
 * where that case is turned into the equivalent single untyped identity.</p>
 */
public interface CallerAuthentication extends Authentication {

    /** The caller's identities. Never null, and never empty for an authenticated caller. */
    CallerIdentities identities();
}
