package org.alfresco.contentlake.security;

import lombok.extern.slf4j.Slf4j;
import org.springframework.http.*;
import org.springframework.security.authentication.AuthenticationProvider;
import org.springframework.security.authentication.BadCredentialsException;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Authentication provider that validates credentials against Alfresco.
 * Uses the Alfresco tickets endpoint to verify username/password combinations.
 */
@Slf4j
public class AlfrescoAuthenticationProvider implements AuthenticationProvider {

    private final String alfrescoUrl;
    private final RestTemplate restTemplate;

    public AlfrescoAuthenticationProvider(String alfrescoUrl) {
        this.alfrescoUrl = alfrescoUrl;
        this.restTemplate = new RestTemplate();
    }

    @Override
    public Authentication authenticate(Authentication authentication) throws AuthenticationException {
        String username = authentication.getName();
        String password = authentication.getCredentials().toString();

        log.debug("Authenticating user: {}", username);

        if (validateAlfrescoCredentials(username, password)) {
            log.info("Successfully authenticated user: {}", username);
            return new UsernamePasswordAuthenticationToken(
                    username,
                    password,
                    Collections.singletonList(new SimpleGrantedAuthority("ROLE_USER"))
            );
        }

        log.warn("Authentication failed for user: {}", username);
        throw new BadCredentialsException("Invalid Alfresco credentials");
    }

    private boolean validateAlfrescoCredentials(String username, String password) {
        try {
            String url = alfrescoUrl + "/alfresco/api/-default-/public/authentication/versions/1/tickets";

            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);

            Map<String, String> credentials = new HashMap<>();
            credentials.put("userId", username);
            credentials.put("password", password);

            HttpEntity<Map<String, String>> request = new HttpEntity<>(credentials, headers);

            ResponseEntity<Map> response = restTemplate.exchange(
                    url,
                    HttpMethod.POST,
                    request,
                    Map.class
            );

            return response.getStatusCode() == HttpStatus.CREATED;

        } catch (HttpClientErrorException.Unauthorized | HttpClientErrorException.Forbidden e) {
            log.debug("Alfresco rejected credentials for user: {}", username);
            return false;
        } catch (Exception e) {
            log.error("Error validating credentials with Alfresco: {}", e.getMessage());
            throw new BadCredentialsException("Unable to validate credentials with Alfresco: " + e.getMessage());
        }
    }

    @Override
    public boolean supports(Class<?> authentication) {
        return UsernamePasswordAuthenticationToken.class.isAssignableFrom(authentication);
    }
}