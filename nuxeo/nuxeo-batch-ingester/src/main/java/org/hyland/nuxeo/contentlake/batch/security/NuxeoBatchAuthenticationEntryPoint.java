package org.hyland.nuxeo.contentlake.batch.security;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.web.AuthenticationEntryPoint;

import java.io.IOException;

/**
 * Returns a plain 401 for browser-driven sync API calls so Web UI requests do
 * not trigger the browser's native Basic-auth popup.
 */
public class NuxeoBatchAuthenticationEntryPoint implements AuthenticationEntryPoint {

    private static final String REALM = "Basic realm=\"Nuxeo Batch Ingester\"";

    @Override
    public void commence(HttpServletRequest request,
                         HttpServletResponse response,
                         AuthenticationException authException) throws IOException {
        if (!isSyncApiRequest(request)) {
            response.setHeader("WWW-Authenticate", REALM);
        }

        response.setStatus(HttpStatus.UNAUTHORIZED.value());
        response.setContentType(MediaType.APPLICATION_JSON_VALUE);
        response.getWriter().write("{\"error\":\"Authentication required\"}");
    }

    private boolean isSyncApiRequest(HttpServletRequest request) {
        String uri = request.getRequestURI();
        return uri != null && uri.startsWith("/api/");
    }
}
