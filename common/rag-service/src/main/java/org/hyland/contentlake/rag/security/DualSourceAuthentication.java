package org.hyland.contentlake.rag.security;

import org.springframework.security.core.Authentication;
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
 */
public class DualSourceAuthentication implements Authentication {

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
