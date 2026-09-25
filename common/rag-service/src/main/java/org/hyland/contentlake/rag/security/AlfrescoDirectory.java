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

import java.util.Base64;
import java.util.List;
import java.util.Map;

/**
 * Validates credentials against an Alfresco repository's own authentication API.
 *
 * <p>The HTTP half of Alfresco caller authentication, separated from the policy half so the policy can be
 * registered as one {@link org.hyland.contentlake.security.CallerAuthenticator} among several. Both
 * {@link AlfrescoTicketCallerAuthenticator} and {@link AlfrescoPasswordCallerAuthenticator} read this, and so
 * does {@link DualSourceAuthenticationFilter}, which validates a ticket without going through the chain.</p>
 *
 * <p>Every method answers {@code null} or {@code false} rather than throwing, including for an I/O failure:
 * an unreachable repository is a decline, because it says nothing about the other authorities. The timeouts
 * are short and explicit, because an authentication hang is a request hang.</p>
 */
@Slf4j
@Component
public class AlfrescoDirectory {

    private static final int CONNECT_TIMEOUT_MS = 3_000;
    private static final int READ_TIMEOUT_MS = 5_000;

    @Value("${content.service.url}")
    private String alfrescoUrl;

    /** Whether a repository URL is configured at all. An unconfigured directory declines everything. */
    boolean isConfigured() {
        return alfrescoUrl != null && !alfrescoUrl.isBlank();
    }

    /**
     * Validates a ticket and resolves the username behind it, or {@code null} on failure.
     *
     * <p>Two calls, not one: the tickets endpoint echoes the ticket back as the entry id for some versions,
     * so a response whose id is still {@code TICKET_...} is followed by {@code people/-me-} to get the real
     * username.</p>
     */
    @SuppressWarnings("unchecked")
    String validateTicket(String ticket) {
        if (!isConfigured()) {
            return null;
        }
        try {
            RestTemplate restTemplate = newRestTemplate();
            String validateUrl = alfrescoUrl
                    + "/alfresco/api/-default-/public/authentication/versions/1/tickets/-me-";
            HttpHeaders headers = ticketHeaders(ticket);
            ResponseEntity<Map> response = restTemplate.exchange(
                    validateUrl, HttpMethod.GET, new HttpEntity<>(headers), Map.class);
            if (!response.getStatusCode().is2xxSuccessful()) {
                return null;
            }
            String resolvedUsername = extractUsername(response.getBody());
            if (resolvedUsername != null && !resolvedUsername.startsWith("TICKET_")) {
                return resolvedUsername;
            }

            String meUrl = alfrescoUrl
                    + "/alfresco/api/-default-/public/alfresco/versions/1/people/-me-";
            ResponseEntity<Map> meResponse = restTemplate.exchange(
                    meUrl, HttpMethod.GET, new HttpEntity<>(headers), Map.class);
            if (!meResponse.getStatusCode().is2xxSuccessful()) {
                return null;
            }
            return extractUsername(meResponse.getBody());
        } catch (Exception e) {
            log.debug("Alfresco ticket validation unavailable: {}", e.getMessage());
        }
        return null;
    }

    /** Whether the repository accepts this username and password, by minting a ticket for them. */
    boolean authenticate(String username, String password) {
        if (!isConfigured()) {
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

    private HttpHeaders ticketHeaders(String ticket) {
        HttpHeaders headers = new HttpHeaders();
        // Alfresco validates UI tickets with Basic base64(ticket) rather than user:password.
        String encoded = Base64.getEncoder().encodeToString(ticket.getBytes());
        headers.set(HttpHeaders.AUTHORIZATION, "Basic " + encoded);
        headers.setAccept(List.of(MediaType.APPLICATION_JSON));
        return headers;
    }

    private RestTemplate newRestTemplate() {
        SimpleClientHttpRequestFactory factory = new SimpleClientHttpRequestFactory();
        factory.setConnectTimeout(CONNECT_TIMEOUT_MS);
        factory.setReadTimeout(READ_TIMEOUT_MS);
        return new RestTemplate(factory);
    }

    @SuppressWarnings("unchecked")
    private String extractUsername(Map body) {
        if (body == null) {
            return null;
        }
        Object entry = body.get("entry");
        if (entry instanceof Map entryMap) {
            String username = DirectoryResponses.firstString(
                    entryMap.get("id"), entryMap.get("userName"), entryMap.get("userId"));
            if (username != null) {
                return username;
            }
        }
        return DirectoryResponses.firstString(body.get("id"), body.get("userName"), body.get("userId"));
    }
}
