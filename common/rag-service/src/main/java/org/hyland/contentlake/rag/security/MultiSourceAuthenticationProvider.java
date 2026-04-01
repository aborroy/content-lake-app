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

    @Value("${content.service.url}")
    private String alfrescoUrl;

    @Value("${nuxeo.base-url:}")
    private String nuxeoUrl;

    @Override
    public Authentication authenticate(Authentication authentication) throws AuthenticationException {
        String username = authentication.getName();
        String password = authentication.getCredentials().toString();

        if (username.startsWith("TICKET_") && password.isEmpty()) {
            if (tryAlfrescoTicketAuth(username)) {
                log.debug("Authenticated ticket '{}' via Alfresco", username.substring(0, Math.min(username.length(), 20)));
                return new UsernamePasswordAuthenticationToken(
                        username, null, List.of(new SimpleGrantedAuthority("ROLE_USER")));
            }
            log.warn("Alfresco ticket validation failed for '{}'", username.substring(0, Math.min(username.length(), 20)));
            throw new BadCredentialsException("Invalid or expired Alfresco ticket");
        }

        if (tryAlfrescoAuth(username, password) || tryNuxeoAuth(username, password)) {
            log.debug("Authenticated user '{}'", username);
            return new UsernamePasswordAuthenticationToken(
                    username, null, List.of(new SimpleGrantedAuthority("ROLE_USER")));
        }

        log.warn("Authentication failed for user '{}'", username);
        throw new BadCredentialsException("Invalid credentials for user: " + username);
    }

    @Override
    public boolean supports(Class<?> authentication) {
        return UsernamePasswordAuthenticationToken.class.isAssignableFrom(authentication);
    }

    private boolean tryAlfrescoTicketAuth(String ticket) {
        if (alfrescoUrl == null || alfrescoUrl.isBlank()) {
            return false;
        }
        try {
            RestTemplate restTemplate = newRestTemplate();
            String validateUrl = alfrescoUrl
                    + "/alfresco/api/-default-/public/authentication/versions/1/tickets/-me-";
            HttpHeaders headers = new HttpHeaders();
            String encoded = Base64.getEncoder().encodeToString((ticket + ":").getBytes());
            headers.set(HttpHeaders.AUTHORIZATION, "Basic " + encoded);
            headers.setAccept(List.of(MediaType.APPLICATION_JSON));
            ResponseEntity<Void> response = restTemplate.exchange(
                    validateUrl, HttpMethod.GET, new HttpEntity<>(headers), Void.class);
            return response.getStatusCode().is2xxSuccessful();
        } catch (Exception e) {
            log.debug("Alfresco ticket validation unavailable: {}", e.getMessage());
        }
        return false;
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
}
