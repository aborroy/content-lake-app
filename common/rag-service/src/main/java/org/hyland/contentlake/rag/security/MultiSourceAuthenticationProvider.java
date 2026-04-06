package org.hyland.contentlake.rag.security;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.*;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.security.authentication.AuthenticationProvider;
import org.springframework.security.authentication.BadCredentialsException;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.util.Base64;
import java.util.List;
import java.util.Map;

/**
 * Validates HTTP Basic Auth credentials against Alfresco and/or Nuxeo.
 *
 * <p>Tries Alfresco first (via the tickets API), then Nuxeo (via {@code /me}).
 * A successful response from either source grants the authenticated principal.</p>
 */
@Slf4j
@Component
public class MultiSourceAuthenticationProvider implements AuthenticationProvider {

    private static final int CONNECT_TIMEOUT_MS = 3_000;
    private static final int READ_TIMEOUT_MS = 5_000;
    static final String NUXEO_TOKEN_PRINCIPAL_PREFIX = "NUXEO_TOKEN::";

    @Value("${content.service.url}")
    private String alfrescoUrl;

    @Value("${nuxeo.base-url:}")
    private String nuxeoUrl;

    @Override
    public Authentication authenticate(Authentication authentication) throws AuthenticationException {
        String principal = authentication.getName();
        Object credentials = authentication.getCredentials();
        String password = credentials == null ? "" : credentials.toString();

        if (principal.startsWith("TICKET_") && password.isEmpty()) {
            String resolvedUsername = tryAlfrescoTicketAuth(principal);
            if (resolvedUsername != null) {
                log.debug("Authenticated Alfresco ticket '{}' as '{}'",
                        principal.substring(0, Math.min(principal.length(), 20)),
                        resolvedUsername);
                return authenticatedUser(resolvedUsername);
            }
            log.warn("Alfresco ticket validation failed for '{}'",
                    principal.substring(0, Math.min(principal.length(), 20)));
            throw new BadCredentialsException("Invalid or expired Alfresco ticket");
        }

        if (principal.startsWith(NUXEO_TOKEN_PRINCIPAL_PREFIX) && password.isEmpty()) {
            String token = principal.substring(NUXEO_TOKEN_PRINCIPAL_PREFIX.length());
            String resolvedUsername = tryNuxeoTokenAuth(token);
            if (resolvedUsername != null) {
                log.debug("Authenticated Nuxeo token as '{}'", resolvedUsername);
                return authenticatedUser(resolvedUsername);
            }
            log.warn("Nuxeo token validation failed");
            throw new BadCredentialsException("Invalid or expired Nuxeo authentication token");
        }

        if (tryAlfrescoAuth(principal, password) || tryNuxeoAuth(principal, password)) {
            log.debug("Authenticated user '{}'", principal);
            return authenticatedUser(principal);
        }

        log.warn("Authentication failed for user '{}'", principal);
        throw new BadCredentialsException("Invalid credentials for user: " + principal);
    }

    @Override
    public boolean supports(Class<?> authentication) {
        return UsernamePasswordAuthenticationToken.class.isAssignableFrom(authentication);
    }

    private Authentication authenticatedUser(String username) {
        return new UsernamePasswordAuthenticationToken(
                username, null, List.of(new SimpleGrantedAuthority("ROLE_USER")));
    }

    @SuppressWarnings("unchecked")
    private String tryAlfrescoTicketAuth(String ticket) {
        if (alfrescoUrl == null || alfrescoUrl.isBlank()) {
            return null;
        }
        try {
            RestTemplate restTemplate = newRestTemplate();
            String validateUrl = alfrescoUrl
                    + "/alfresco/api/-default-/public/authentication/versions/1/tickets/-me-";
            HttpHeaders headers = new HttpHeaders();
            // Alfresco validates UI tickets on /tickets/-me- using Basic base64(ticket)
            // rather than the usual user:password form.
            String encoded = Base64.getEncoder().encodeToString(ticket.getBytes());
            headers.set(HttpHeaders.AUTHORIZATION, "Basic " + encoded);
            headers.setAccept(List.of(MediaType.APPLICATION_JSON));
            ResponseEntity<Map> response = restTemplate.exchange(
                    validateUrl, HttpMethod.GET, new HttpEntity<>(headers), Map.class);
            if (!response.getStatusCode().is2xxSuccessful()) {
                return null;
            }
            return extractAlfrescoUsername(response.getBody());
        } catch (Exception e) {
            log.debug("Alfresco ticket validation unavailable: {}", e.getMessage());
        }
        return null;
    }

    private boolean tryAlfrescoAuth(String username, String password) {
        if (alfrescoUrl == null || alfrescoUrl.isBlank()) {
            return false;
        }
        try {
            RestTemplate restTemplate = newRestTemplate();
            String ticketUrl = alfrescoUrl
                    + "/alfresco/api/-default-/public/authentication/versions/1/tickets";
            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);
            Map<String, String> body = Map.of("userId", username, "password", password);
            ResponseEntity<Void> response = restTemplate.exchange(
                    ticketUrl, HttpMethod.POST, new HttpEntity<>(body, headers), Void.class);
            if (response.getStatusCode().is2xxSuccessful()) {
                log.debug("Authenticated '{}' via Alfresco", username);
                return true;
            }
        } catch (Exception e) {
            log.debug("Alfresco auth unavailable for '{}': {}", username, e.getMessage());
        }
        return false;
    }

    @SuppressWarnings("unchecked")
    private String tryNuxeoTokenAuth(String token) {
        if (nuxeoUrl == null || nuxeoUrl.isBlank()) {
            return null;
        }
        try {
            RestTemplate restTemplate = newRestTemplate();
            String meUrl = buildNuxeoApiUrl() + "/me";
            HttpHeaders headers = new HttpHeaders();
            headers.setAccept(List.of(MediaType.APPLICATION_JSON));
            headers.set("X-Authentication-Token", token);
            ResponseEntity<Map> response = restTemplate.exchange(
                    meUrl, HttpMethod.GET, new HttpEntity<>(headers), Map.class);
            if (!response.getStatusCode().is2xxSuccessful()) {
                return null;
            }
            return extractNuxeoUsername(response.getBody());
        } catch (Exception e) {
            log.debug("Nuxeo token validation unavailable: {}", e.getMessage());
        }
        return null;
    }

    private boolean tryNuxeoAuth(String username, String password) {
        if (nuxeoUrl == null || nuxeoUrl.isBlank()) {
            return false;
        }
        try {
            RestTemplate restTemplate = newRestTemplate();
            String meUrl = buildNuxeoApiUrl() + "/me";
            HttpHeaders headers = new HttpHeaders();
            headers.setBasicAuth(username, password);
            headers.setAccept(List.of(MediaType.APPLICATION_JSON));
            ResponseEntity<Void> response = restTemplate.exchange(
                    meUrl, HttpMethod.GET, new HttpEntity<>(headers), Void.class);
            if (response.getStatusCode().is2xxSuccessful()) {
                log.debug("Authenticated '{}' via Nuxeo", username);
                return true;
            }
        } catch (Exception e) {
            log.debug("Nuxeo auth unavailable for '{}': {}", username, e.getMessage());
        }
        return false;
    }

    // ---------------------------------------------------------------
    // Package-private validation helpers for DualSourceAuthenticationFilter
    // ---------------------------------------------------------------

    /**
     * Validates an Alfresco ticket and returns the resolved username, or {@code null} on failure.
     */
    String validateAlfrescoTicket(String ticket) {
        return tryAlfrescoTicketAuth(ticket);
    }

    /**
     * Validates Nuxeo Basic credentials and returns the username on success, or {@code null} on failure.
     */
    String validateNuxeoCredentials(String username, String password) {
        return tryNuxeoAuth(username, password) ? username : null;
    }

    private RestTemplate newRestTemplate() {
        SimpleClientHttpRequestFactory factory = new SimpleClientHttpRequestFactory();
        factory.setConnectTimeout(CONNECT_TIMEOUT_MS);
        factory.setReadTimeout(READ_TIMEOUT_MS);
        return new RestTemplate(factory);
    }

    private String buildNuxeoApiUrl() {
        String trimmed = nuxeoUrl.endsWith("/") ? nuxeoUrl.substring(0, nuxeoUrl.length() - 1) : nuxeoUrl;
        return trimmed.endsWith("/api/v1") ? trimmed : trimmed + "/api/v1";
    }

    @SuppressWarnings("unchecked")
    private String extractAlfrescoUsername(Map body) {
        if (body == null) {
            return null;
        }
        Object entry = body.get("entry");
        if (entry instanceof Map entryMap) {
            String username = firstString(entryMap.get("id"), entryMap.get("userName"), entryMap.get("userId"));
            if (username != null) {
                return username;
            }
        }
        return firstString(body.get("id"), body.get("userName"), body.get("userId"));
    }

    @SuppressWarnings("unchecked")
    private String extractNuxeoUsername(Map body) {
        if (body == null) {
            return null;
        }
        String username = firstString(body.get("id"), body.get("username"), body.get("userName"));
        if (username != null) {
            return username;
        }
        Object properties = body.get("properties");
        if (properties instanceof Map propertyMap) {
            return firstString(propertyMap.get("username"), propertyMap.get("userName"));
        }
        return null;
    }

    private String firstString(Object... values) {
        for (Object value : values) {
            if (value instanceof String text && !text.isBlank()) {
                return text;
            }
        }
        return null;
    }
}
