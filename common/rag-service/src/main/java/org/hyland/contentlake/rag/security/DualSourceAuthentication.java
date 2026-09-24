package org.hyland.contentlake.rag.security;

import org.hyland.contentlake.security.CallerAuthentication;
import org.hyland.contentlake.security.CallerIdentities;
import org.hyland.contentlake.security.SourceIdentity;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;

import java.util.Collection;
import java.util.List;

/**
 * Compound authentication token holding independent Alfresco and Nuxeo principals.
 *
 * <p>Created when a request presents credentials for both repositories simultaneously:
 * an Alfresco ticket ({@code Authorization: Basic base64(TICKET_xxx:)}) and Nuxeo
 * credentials ({@code X-Nuxeo-Authorization: Basic base64(user:pass)}).
 * Permission filters built from this token cover both sources independently, so
 * results from either repository are returned according to each user's permissions.</p>
 *
 * <p>Carries its two principals as {@link CallerIdentities} so the query path can read them without naming
 * this class. That is the step that lets a third identity source exist; this type is superseded once nothing
 * constructs it.</p>
 */
public class DualSourceAuthentication implements CallerAuthentication {

    private final String alfrescoUsername;
    private final String nuxeoUsername;

    public DualSourceAuthentication(String alfrescoUsername, String nuxeoUsername) {
        this.alfrescoUsername = alfrescoUsername;
        this.nuxeoUsername = nuxeoUsername;
    }

    /** Returns the Alfresco-authenticated username, or {@code null} if not authenticated against Alfresco. */
    public String getAlfrescoUsername() {
        return alfrescoUsername;
    }

    /** Returns the Nuxeo-authenticated username, or {@code null} if not authenticated against Nuxeo. */
    public String getNuxeoUsername() {
        return nuxeoUsername;
    }

    /**
     * The two principals as an identity set: the Alfresco one is the fallback, so a source of a third type
     * resolves to it exactly as {@code isNuxeoSource(id) ? nuxeoUser : alfrescoUser} sent it there.
     *
     * <p>Only {@code DualSourceAuthenticationFilter} constructs this type and it requires both credentials
     * to validate, so the one-sided shape below is unreachable in a deployment. It is mapped to a single
     * untyped identity anyway, because that is what {@link #getName()} has always reported for it.</p>
     */
    @Override
    public CallerIdentities identities() {
        if (alfrescoUsername == null) {
            return nuxeoUsername == null ? CallerIdentities.EMPTY : CallerIdentities.single(nuxeoUsername);
        }
        if (nuxeoUsername == null) {
            return CallerIdentities.single(alfrescoUsername);
        }
        return CallerIdentities.builder()
                .fallback(SourceIdentity.of("alfresco", alfrescoUsername))
                .add(SourceIdentity.of("nuxeo", nuxeoUsername))
                .build();
    }

    @Override
    public Collection<? extends GrantedAuthority> getAuthorities() {
        return List.of(new SimpleGrantedAuthority("ROLE_USER"));
    }

    @Override
    public Object getCredentials() {
        return null;
    }

    @Override
    public Object getDetails() {
        return null;
    }

    /** Primary principal: Alfresco username when present, otherwise Nuxeo. */
    @Override
    public Object getPrincipal() {
        return alfrescoUsername != null ? alfrescoUsername : nuxeoUsername;
    }

    @Override
    public boolean isAuthenticated() {
        return true;
    }

    @Override
    public void setAuthenticated(boolean isAuthenticated) {
        throw new UnsupportedOperationException("DualSourceAuthentication is immutable");
    }

    /** Returns a composite name for logging, e.g. {@code "alfresco:alice|nuxeo:bob"}. */
    @Override
    public String getName() {
        if (alfrescoUsername != null && nuxeoUsername != null) {
            return "alfresco:" + alfrescoUsername + "|nuxeo:" + nuxeoUsername;
        }
        return alfrescoUsername != null ? alfrescoUsername : nuxeoUsername;
    }
}
