package org.hyland.contentlake.security;

import org.springframework.security.core.AuthenticationException;

/**
 * Identifies a caller against one credential authority.
 *
 * <p>The identification half of query-side security, and the counterpart to {@link SourceGroupResolver},
 * which is the authorization half. Authorization is already source-agnostic: ACLs map to read principals per
 * source, every source is discovered from {@code cin_sourceId}, and a source with no resolver falls back
 * fail-closed. Identification was not: it was a fixed chain of two {@code if} branches against Alfresco and
 * Nuxeo, with nowhere to register a third. This interface is that nowhere.</p>
 *
 * <p>Implementations stay in-tree, in {@code rag-service}. This is deliberately not part of
 * {@code content-lake-spi} and is not loaded from a connector jar: it runs inside the service that enforces
 * security trimming, so plugin code must not be able to decide who a caller is.</p>
 *
 * <h3>The three outcomes, which are not interchangeable</h3>
 *
 * <ul>
 *   <li>Identities: this authority recognised the caller. The chain stops here.</li>
 *   <li>{@code null} or empty: a decline. This authority cannot speak to these credentials, or it rejected
 *       them and that says nothing about the others. The chain continues to the next authenticator, and if
 *       every one declines the caller is rejected once, at the end.</li>
 *   <li>A thrown {@link AuthenticationException}: this credential was addressed to this authority and is
 *       invalid. It propagates and the request is rejected without consulting anything else.</li>
 * </ul>
 *
 * <p>Confusing the last two matters in both directions. Throwing for credentials merely not recognised
 * denies a caller whom a later authority would have accepted; declining a credential that was unambiguously
 * addressed here, such as an expired ticket, turns a clear rejection into a confusing fall-through to
 * authorities that were never meant to see it.</p>
 */
public interface CallerAuthenticator {

    /**
     * A stable identifier, used in logs and pinned by a test so the chain's runtime order is protected.
     * Registration order is a security property: it decides which authority is consulted first, and
     * therefore which system sees a credential.
     */
    String id();

    /**
     * Position in the chain, ascending. Existing authorities keep the order they validated in before this
     * interface existed, so adding an authenticator cannot change where an established deployment's
     * credentials are sent.
     */
    int order();

    /**
     * Whether this authenticator can adjudicate these credentials at all.
     *
     * <p>Where the {@code PREFIX::} tests live. Each authenticator recognises its own marker, rather than one
     * chain of {@code if} branches recognising all of them.</p>
     */
    boolean supports(CallerCredentials credentials);

    /**
     * The caller's identities, or {@code null} to decline.
     *
     * @throws AuthenticationException when the credentials were addressed to this authority and are invalid
     */
    CallerIdentities authenticate(CallerCredentials credentials);
}
