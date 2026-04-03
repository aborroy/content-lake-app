package org.hyland.contentlake.rag.security;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpHeaders;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;

/**
 * Extracts Nuxeo UI authentication tokens from requests before the Basic auth
 * filter runs.
 */
@Slf4j
public class NuxeoTokenAuthenticationFilter extends OncePerRequestFilter {

    private static final String NUXEO_TOKEN_HEADER = "X-Authentication-Token";

    private final AuthenticationManager authenticationManager;

    public NuxeoTokenAuthenticationFilter(AuthenticationManager authenticationManager) {
        this.authenticationManager = authenticationManager;
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain filterChain)
            throws ServletException, IOException {

        if (SecurityContextHolder.getContext().getAuthentication() == null) {
            String nuxeoToken = request.getHeader(NUXEO_TOKEN_HEADER);
            String authorizationHeader = request.getHeader(HttpHeaders.AUTHORIZATION);

            if (nuxeoToken != null && !nuxeoToken.isBlank()
                    && (authorizationHeader == null || authorizationHeader.isBlank())) {
                try {
                    Authentication authRequest = new UsernamePasswordAuthenticationToken(
                            MultiSourceAuthenticationProvider.NUXEO_TOKEN_PRINCIPAL_PREFIX + nuxeoToken.trim(),
                            ""
                    );
                    Authentication authentication = authenticationManager.authenticate(authRequest);
                    SecurityContextHolder.getContext().setAuthentication(authentication);
                } catch (Exception e) {
                    log.debug("Nuxeo token authentication failed: {}", e.getMessage());
                    SecurityContextHolder.clearContext();
                }
            }
        }

        filterChain.doFilter(request, response);
    }
}
