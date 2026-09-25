package org.hyland.contentlake.rag.security;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.util.List;
import java.util.Map;

/**
 * Validates credentials against a Nuxeo server's {@code /me} endpoint.
 *
 * <p>The HTTP half of Nuxeo caller authentication. See {@link AlfrescoDirectory} for why the HTTP and policy
 * halves are separate, and for the decline-rather-than-throw rule both follow.</p>
 */
@Slf4j
@Component
public class NuxeoDirectory {

    private static final int CONNECT_TIMEOUT_MS = 3_000;
    private static final int READ_TIMEOUT_MS = 5_000;

    @Value("${nuxeo.base-url:}")
    private String nuxeoUrl;

    /** Whether a server URL is configured at all. An unconfigured directory declines everything. */
    boolean isConfigured() {
        return nuxeoUrl != null && !nuxeoUrl.isBlank();
    }

    /** Resolves the username behind an authentication token, or {@code null} on failure. */
    @SuppressWarnings("unchecked")
    String validateToken(String token) {
        if (!isConfigured()) {
            return null;
        }
        try {
            RestTemplate restTemplate = newRestTemplate();
            String meUrl = apiUrl() + "/me";
            HttpHeaders headers = new HttpHeaders();
            headers.setAccept(List.of(MediaType.APPLICATION_JSON));
            headers.set("X-Authentication-Token", token);
            ResponseEntity<Map> response = restTemplate.exchange(
                    meUrl, HttpMethod.GET, new HttpEntity<>(headers), Map.class);
            if (!response.getStatusCode().is2xxSuccessful()) {
                return null;
            }
            return extractUsername(response.getBody());
        } catch (Exception e) {
            log.debug("Nuxeo token validation unavailable: {}", e.getMessage());
        }
        return null;
    }

    /** Whether the server accepts this username and password. */
    boolean authenticate(String username, String password) {
        if (!isConfigured()) {
            return false;
        }
        try {
            RestTemplate restTemplate = newRestTemplate();
            String meUrl = apiUrl() + "/me";
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

    private String apiUrl() {
        String trimmed = nuxeoUrl.endsWith("/") ? nuxeoUrl.substring(0, nuxeoUrl.length() - 1) : nuxeoUrl;
        return trimmed.endsWith("/api/v1") ? trimmed : trimmed + "/api/v1";
    }

    @SuppressWarnings("unchecked")
    private String extractUsername(Map body) {
        if (body == null) {
            return null;
        }
        String username = DirectoryResponses.firstString(
                body.get("id"), body.get("username"), body.get("userName"));
        if (username != null) {
            return username;
        }
        Object properties = body.get("properties");
        if (properties instanceof Map propertyMap) {
            return DirectoryResponses.firstString(propertyMap.get("username"), propertyMap.get("userName"));
        }
        return null;
    }
}
