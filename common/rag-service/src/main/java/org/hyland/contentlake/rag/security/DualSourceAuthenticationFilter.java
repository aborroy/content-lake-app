package org.hyland.contentlake.rag.security;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import lombok.extern.slf4j.Slf4j;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

/**
 * Authenticates requests carrying credentials for BOTH Alfresco and Nuxeo simultaneously.
 *
 * <p>Expected headers:</p>
 * <ul>
 *   <li>{@code Authorization: Basic base64(TICKET_xxx:)} — Alfresco ticket</li>
 *   <li>{@code X-Nuxeo-Authorization: Basic base64(user:pass)} — Nuxeo Basic credentials</li>
 * </ul>
 *
 * <p>Both credentials are validated independently. On success a {@link DualSourceAuthentication}
 * is stored in the security context, allowing permission filters to cover both repositories in
 * a single request. If either header is absent this filter does nothing and the standard
 * single-source filters ({@link AlfrescoTicketAuthenticationFilter} /
 * {@link NuxeoTokenAuthenticationFilter}) take over.</p>
 *
 * <p>This filter must run before the other two authentication filters.</p>
 */
@Slf4j
public class DualSourceAuthenticationFilter extends OncePerRequestFilter {

    /** Custom header the UI sends for Nuxeo credentials in dual-auth mode. */
    static final String NUXEO_AUTHORIZATION_HEADER = "X-Nuxeo-Authorization";
    private static final String AUTHORIZATION_HEADER = "Authorization";

    private final MultiSourceAuthenticationProvider provider;

    public DualSourceAuthenticationFilter(MultiSourceAuthenticationProvider provider) {
        this.provider = provider;
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request,
                                    HttpServletResponse response,
                                    FilterChain chain)
            throws ServletException, IOException {

        // Skip if already authenticated (e.g. by a previous filter in an async dispatch)
        if (SecurityContextHolder.getContext().getAuthentication() != null) {
            chain.doFilter(request, response);
            return;
        }

        String authHeader = request.getHeader(AUTHORIZATION_HEADER);
        String nuxeoAuthHeader = request.getHeader(NUXEO_AUTHORIZATION_HEADER);

        // Both headers must be present for dual-auth; otherwise fall through
        if (authHeader == null || nuxeoAuthHeader == null) {
            chain.doFilter(request, response);
            return;
        }

        String ticket = extractAlfrescoTicket(authHeader);
        if (ticket == null) {
            chain.doFilter(request, response);
            return;
        }

        String[] nuxeoCreds = extractBasicCredentials(nuxeoAuthHeader);
        if (nuxeoCreds == null) {
            chain.doFilter(request, response);
            return;
        }

        // Validate each credential independently — neither short-circuits the other
        String alfrescoUsername = provider.validateAlfrescoTicket(ticket);
        if (alfrescoUsername == null) {
            log.debug("Dual-auth: Alfresco ticket validation failed — falling through to single-source filters");
            chain.doFilter(request, response);
            return;
        }

        String nuxeoUsername = provider.validateNuxeoCredentials(nuxeoCreds[0], nuxeoCreds[1]);
        if (nuxeoUsername == null) {
            log.debug("Dual-auth: Nuxeo credentials validation failed — falling through to single-source filters");
            chain.doFilter(request, response);
            return;
        }

        log.debug("Dual-auth: established alfresco='{}' nuxeo='{}'", alfrescoUsername, nuxeoUsername);
        SecurityContextHolder.getContext()
                .setAuthentication(new DualSourceAuthentication(alfrescoUsername, nuxeoUsername));

        chain.doFilter(request, response);
    }

    private String extractAlfrescoTicket(String authHeader) {
        if (!authHeader.startsWith("Basic ")) {
            return null;
        }
        try {
            String decoded = new String(
                    Base64.getDecoder().decode(authHeader.substring(6).trim()),
                    StandardCharsets.UTF_8);
            if (!decoded.startsWith("TICKET_")) {
                return null;
            }
            int colon = decoded.indexOf(':');
            return colon >= 0 ? decoded.substring(0, colon) : decoded;
        } catch (IllegalArgumentException e) {
            log.debug("Could not decode Authorization header as Alfresco ticket: {}", e.getMessage());
            return null;
        }
    }

    private String[] extractBasicCredentials(String header) {
        if (!header.startsWith("Basic ")) {
            return null;
        }
        try {
            String decoded = new String(
                    Base64.getDecoder().decode(header.substring(6).trim()),
                    StandardCharsets.UTF_8);
            int colon = decoded.indexOf(':');
            if (colon < 0) {
                return null;
            }
            return new String[]{decoded.substring(0, colon), decoded.substring(colon + 1)};
        } catch (IllegalArgumentException e) {
            log.debug("Could not decode X-Nuxeo-Authorization as Basic credentials: {}", e.getMessage());
            return null;
        }
    }
}
