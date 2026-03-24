package org.alfresco.contentlake.security;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletRequestWrapper;
import jakarta.servlet.http.HttpServletResponse;
import lombok.extern.slf4j.Slf4j;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.web.authentication.preauth.PreAuthenticatedAuthenticationToken;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * Filter that extracts Alfresco tickets from requests.
 * Checks both query parameters (?alf_ticket=...) and Authorization header.
 *
 * When a ticket is extracted from the Authorization header and authentication
 * succeeds, the header is stripped from the request before continuing the
 * filter chain. This prevents Spring's BasicAuthenticationFilter from
 * re-processing the header (which would fail because a bare ticket has no
 * colon separator, causing a 401 + WWW-Authenticate response).
 */
@Slf4j
public class AlfrescoTicketAuthenticationFilter extends OncePerRequestFilter {

    private final AuthenticationManager authenticationManager;

    public AlfrescoTicketAuthenticationFilter(AuthenticationManager authenticationManager) {
        this.authenticationManager = authenticationManager;
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain filterChain)
            throws ServletException, IOException {

        String ticket = request.getParameter("alf_ticket");
        boolean ticketFromHeader = false;

        if (ticket == null) {
            ticket = extractTicketFromHeader(request);
            ticketFromHeader = ticket != null;
        }

        boolean authenticated = false;
        if (ticket != null && ticket.startsWith("TICKET_")) {
            try {
                log.debug("Found Alfresco ticket in request");
                PreAuthenticatedAuthenticationToken authRequest =
                        new PreAuthenticatedAuthenticationToken(ticket, ticket);
                Authentication authentication = authenticationManager.authenticate(authRequest);
                SecurityContextHolder.getContext().setAuthentication(authentication);
                authenticated = true;
            } catch (Exception e) {
                log.debug("Ticket authentication failed: {}", e.getMessage());
                SecurityContextHolder.clearContext();
            }
        }

        // Strip the Authorization header so BasicAuthenticationFilter does not
        // attempt to parse the ticket as user:password credentials
        if (authenticated && ticketFromHeader) {
            filterChain.doFilter(new AuthorizationHeaderStrippingRequest(request), response);
        } else {
            filterChain.doFilter(request, response);
        }
    }

    /**
     * Extracts ticket from Authorization header if it's in Basic auth format
     * with just a ticket (no colon separator).
     */
    private String extractTicketFromHeader(HttpServletRequest request) {
        String authHeader = request.getHeader("Authorization");

        if (authHeader != null && authHeader.startsWith("Basic ")) {
            try {
                String base64Credentials = authHeader.substring(6);
                byte[] decodedBytes = Base64.getDecoder().decode(base64Credentials);
                String credentials = new String(decodedBytes, StandardCharsets.UTF_8);
                if (!credentials.contains(":") && credentials.startsWith("TICKET_")) {
                    return credentials;
                }
            } catch (Exception e) {
                log.debug("Failed to extract ticket from header: {}", e.getMessage());
            }
        }

        return null;
    }

    /**
     * Request wrapper that hides the Authorization header from downstream filters.
     */
    private static class AuthorizationHeaderStrippingRequest extends HttpServletRequestWrapper {

        private static final String AUTHORIZATION = "authorization";

        AuthorizationHeaderStrippingRequest(HttpServletRequest request) {
            super(request);
        }

        @Override
        public String getHeader(String name) {
            if (AUTHORIZATION.equalsIgnoreCase(name)) {
                return null;
            }
            return super.getHeader(name);
        }

        @Override
        public Enumeration<String> getHeaders(String name) {
            if (AUTHORIZATION.equalsIgnoreCase(name)) {
                return Collections.emptyEnumeration();
            }
            return super.getHeaders(name);
        }

        @Override
        public Enumeration<String> getHeaderNames() {
            List<String> names = new ArrayList<>();
            Enumeration<String> original = super.getHeaderNames();
            while (original.hasMoreElements()) {
                String name = original.nextElement();
                if (!AUTHORIZATION.equalsIgnoreCase(name)) {
                    names.add(name);
                }
            }
            return Collections.enumeration(names);
        }
    }
}