package org.hyland.contentlake.rag.security;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.hyland.contentlake.security.CallerAuthenticator;
import org.hyland.contentlake.security.CallerCredentials;
import org.hyland.contentlake.security.CallerIdentities;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

/**
 * Signs a caller in against a CMIS repository, so a deployment whose only source is the CMIS connector can be
 * queried by its own users rather than by a service account.
 *
 * <p>Off by default and absent when off, for the reason {@code EntraGroupResolver} is: an authority that
 * exists and cannot work is worse than none, because it is then in the chain.</p>
 *
 * <p>Unlike every group resolver, this uses the <strong>caller's own</strong> credentials. There is no service
 * account and no secret to configure.</p>
 *
 * <h3>Never throws</h3>
 *
 * <p>A CMIS 401 is a decline, not a rejection. Nothing in a username and password says they were addressed to
 * this repository rather than to Alfresco or Nuxeo, so refusing them here would deny a caller whom an earlier
 * or later authority would have accepted. Only a marker-prefixed credential is unambiguous enough to reject,
 * and this authenticator never sees one.</p>
 *
 * <h3>The identity is untyped, and there is no "cmis" constant here</h3>
 *
 * <p>An untyped identity answers {@code usernameFor(anyType)} through the fallback, so the {@code cmis} source
 * is covered with no literal to drift from the connector's own. {@code CmisConnectorPlugin.SOURCE_TYPE} is
 * {@code "cmis"} but it is package-private in a jar that is not on this classpath, and
 * {@code PermissionSourceCatalog.sourceType(..)} reads the type out of the {@code cin_sourceId} bucket key
 * rather than comparing against a literal. {@code PermissionSourceCatalog.ALFRESCO} and {@code NUXEO} exist
 * only because the admin bypass and the two configured source-id slots name them; there is no third.</p>
 *
 * <p>Untyped is also the invariant rather than a convenience: typing it as {@code cmis:alice} would change
 * {@code getName()}, which re-buckets {@code RateLimitFilter}'s token buckets and orphans stored feedback
 * rows.</p>
 *
 * <p>For the same reason there is no {@code rag.security.cmis.source-type} setting. Entra's exists because a
 * <em>resolver</em> must declare what it resolves; an authenticator producing an untyped identity has nothing
 * to declare.</p>
 */
@Slf4j
@Component
@ConditionalOnProperty(name = "rag.security.cmis.enabled", havingValue = "true")
@RequiredArgsConstructor
public class CmisCallerAuthenticator implements CallerAuthenticator {

    private final CmisRepositoryDirectory directory;

    @Override
    public String id() {
        return "cmis-password";
    }

    @Override
    public int order() {
        return CallerAuthenticatorOrder.CMIS;
    }

    @Override
    public boolean supports(CallerCredentials credentials) {
        // Not a reserved principal: forwarding an Alfresco ticket to a third repository would put it in that
        // repository's access log. Not a blank password either, since some repositories treat an empty
        // password as an anonymous bind and would authenticate any username at all.
        return !ReservedPrincipals.isReserved(credentials)
                && !credentials.hasNoPassword()
                && !credentials.principal().isBlank();
    }

    @Override
    public CallerIdentities authenticate(CallerCredentials credentials) {
        if (!directory.authenticate(credentials.principal(), credentials.password())) {
            return null;
        }
        // The login string exactly as presented. A CMIS repository reports no canonical username, and
        // CmisAclMapper writes ace.getPrincipalId() into cin_read unchanged, so normalising here would stop
        // the principal matching the ACE on any repository that is case-sensitive.
        return CallerIdentities.single(credentials.principal());
    }
}
