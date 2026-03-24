package org.alfresco.contentlake.security;

import lombok.extern.slf4j.Slf4j;
import org.springframework.http.*;
import org.springframework.security.authentication.AuthenticationProvider;
import org.springframework.security.authentication.BadCredentialsException;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.web.authentication.preauth.PreAuthenticatedAuthenticationToken;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;

import java.util.Collections;
import java.util.Map;

/**
 * Authentication provider that validates Alfresco tickets.
 * Validates tickets by calling the {@code /people/-me-} Alfresco API and
 * resolves the real username from the response, so that downstream code
 * (e.g.&nbsp;{@code SecurityContextService.getCurrentUsername()}) receives
 * the actual user id rather than the raw ticket string.
 */
@Slf4j
public class AlfrescoTicketAuthenticationProvider implements AuthenticationProvider {

    private final String alfrescoUrl;
    private final RestTemplate restTemplate;

    public AlfrescoTicketAuthenticationProvider(String alfrescoUrl) {
        this.alfrescoUrl = alfrescoUrl;
        this.restTemplate = new RestTemplate();
    }

    @Override
    public Authentication authenticate(Authentication authentication) throws AuthenticationException {
        String ticket = (String) authentication.getPrincipal();

        log.debug("Authenticating with ticket: {}...", ticket.substring(0, Math.min(20, ticket.length())));

        String username = validateAlfrescoTicket(ticket);
        if (username != null) {
            log.info("Successfully authenticated user '{}' with ticket", username);
            return new PreAuthenticatedAuthenticationToken(
                    username,
                    ticket,
                    Collections.singletonList(new SimpleGrantedAuthority("ROLE_USER"))
            );
        }

        log.warn("Authentication failed with invalid ticket");
        throw new BadCredentialsException("Invalid Alfresco ticket");
    }

    /**
     * Validates the ticket against the Alfresco People API and resolves the
     * actual username.  The {@code /people/-me-} endpoint returns the person
     * entry for the ticket owner, whose {@code id} field is the username.
     *
     * @return the resolved username, or {@code null} when the ticket is invalid
     */
    @SuppressWarnings("unchecked")
    private String validateAlfrescoTicket(String ticket) {
        try {
            String url = alfrescoUrl + "/alfresco/api/-default-/public/alfresco/versions/1/people/-me-?alf_ticket=" + ticket;

            HttpHeaders headers = new HttpHeaders();
            HttpEntity<?> request = new HttpEntity<>(headers);

            ResponseEntity<Map> response = restTemplate.exchange(
                    url,
                    HttpMethod.GET,
                    request,
                    Map.class
            );

            if (response.getStatusCode() == HttpStatus.OK && response.getBody() != null) {
                Map<String, Object> body = response.getBody();
                Map<String, Object> entry = (Map<String, Object>) body.get("entry");
                if (entry != null && entry.get("id") != null) {
                    return entry.get("id").toString();
                }
            }
            return null;

        } catch (HttpClientErrorException.Unauthorized | HttpClientErrorException.Forbidden e) {
            log.debug("Alfresco rejected ticket");
            return null;
        } catch (Exception e) {
            log.error("Error validating ticket with Alfresco: {}", e.getMessage());
            return null;
        }
    }

    @Override
    public boolean supports(Class<?> authentication) {
        return PreAuthenticatedAuthenticationToken.class.isAssignableFrom(authentication);
    }
}