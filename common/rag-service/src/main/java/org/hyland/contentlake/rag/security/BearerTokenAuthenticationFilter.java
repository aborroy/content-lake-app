package org.hyland.contentlake.rag.security;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import lombok.extern.slf4j.Slf4j;
import org.hyland.contentlake.security.CallerCredentials;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.util.Map;

/**
 * Turns {@code Authorization: Bearer <token>} into a request the authentication chain can adjudicate.
 *
 * <p>The token travels as a {@link CallerCredentials} attribute inside a
 * {@link PresentedCredentialsAuthentication}, carrying no principal: whoever the caller is comes from the
 * validated token and nowhere else.</p>
 *
 * <p>Runs before {@code BasicAuthenticationFilter} and only ever looks at a {@code Bearer} scheme, so a Basic
 * credential passes straight through untouched. It does not strip the header, unlike
 * {@code DualSourceAuthenticationFilter}: there is nothing downstream that would misread a {@code Bearer}
 * value, because {@code BasicAuthenticationFilter} ignores any scheme but its own.</p>
 *
 * <p>An invalid token is a 401 and the chain stops here, which is the same treatment an invalid Alfresco ticket
 * gets. Falling through instead would let a request carrying a rejected token be retried as anonymous and reach
 * a {@code permitAll} endpoint, which reads as the token having worked.</p>
 */
@Slf4j
public class BearerTokenAuthenticationFilter extends OncePerRequestFilter {

    /** The {@link CallerCredentials} attribute a bearer token travels in. */
    public static final String BEARER_ATTRIBUTE = "bearer";

    private static final String AUTHORIZATION_HEADER = "Authorization";
    private static final String BEARER_PREFIX = "Bearer ";

    private final AuthenticationManager authenticationManager;

    public BearerTokenAuthenticationFilter(AuthenticationManager authenticationManager) {
        this.authenticationManager = authenticationManager;
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request,
                                    HttpServletResponse response,
                                    FilterChain chain)
            throws ServletException, IOException {

        if (SecurityContextHolder.getContext().getAuthentication() != null) {
            chain.doFilter(request, response);
            return;
        }

        String header = request.getHeader(AUTHORIZATION_HEADER);
        if (header == null || !header.startsWith(BEARER_PREFIX)) {
            chain.doFilter(request, response);
            return;
        }

        String token = header.substring(BEARER_PREFIX.length()).trim();
        if (token.isEmpty()) {
            chain.doFilter(request, response);
            return;
        }

        try {
            Authentication authenticated = authenticationManager.authenticate(
                    new PresentedCredentialsAuthentication(
                            new CallerCredentials("", "", Map.of(BEARER_ATTRIBUTE, token))));
            SecurityContextHolder.getContext().setAuthentication(authenticated);
        } catch (AuthenticationException e) {
            // Cleared rather than left half-set, so nothing downstream sees a partially authenticated context.
            SecurityContextHolder.clearContext();
            log.debug("Bearer authentication failed: {}", e.getMessage());
            response.sendError(HttpServletResponse.SC_UNAUTHORIZED, "Invalid bearer token");
            return;
        }

        chain.doFilter(request, response);
    }
}
