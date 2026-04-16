package org.hyland.nuxeo.contentlake.batch.security;

import lombok.extern.slf4j.Slf4j;
import org.hyland.nuxeo.contentlake.config.NuxeoProperties;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.security.authentication.AuthenticationProvider;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.web.client.RestTemplate;

import java.util.List;
import java.util.Map;

/**
 * Authenticates Nuxeo UI requests forwarded with X-Authentication-Token.
 */
@Slf4j
public class NuxeoTokenAuthenticationProvider implements AuthenticationProvider {

    private static final int CONNECT_TIMEOUT_MS = 3_000;
    private static final int READ_TIMEOUT_MS = 5_000;

    public static final String NUXEO_TOKEN_PRINCIPAL_PREFIX = "NUXEO_TOKEN::";

    private final NuxeoProperties props;

    public NuxeoTokenAuthenticationProvider(NuxeoProperties props) {
        this.props = props;
    }

    @Override
    public Authentication authenticate(Authentication authentication) throws AuthenticationException {
        String principal = authentication.getName();
        Object credentials = authentication.getCredentials();
        String password = credentials == null ? "" : credentials.toString();

        if (!principal.startsWith(NUXEO_TOKEN_PRINCIPAL_PREFIX) || !password.isEmpty()) {
            return null;
        }

        String token = principal.substring(NUXEO_TOKEN_PRINCIPAL_PREFIX.length());
        String username = authenticateToken(token);
        if (username == null) {
            return null;
        }

        return new UsernamePasswordAuthenticationToken(
                username,
                null,
                List.of(new SimpleGrantedAuthority("ROLE_USER"))
        );
    }

    @Override
    public boolean supports(Class<?> authentication) {
        return UsernamePasswordAuthenticationToken.class.isAssignableFrom(authentication);
    }

    @SuppressWarnings("unchecked")
    private String authenticateToken(String token) {
        if (token == null || token.isBlank()) {
            return null;
        }
        try {
            HttpHeaders headers = new HttpHeaders();
            headers.setAccept(List.of(MediaType.APPLICATION_JSON));
            headers.set("X-Authentication-Token", token);

            ResponseEntity<Map> response = newRestTemplate().exchange(
                    buildNuxeoApiUrl() + "/me",
                    HttpMethod.GET,
                    new HttpEntity<>(headers),
                    Map.class
            );
            if (!response.getStatusCode().is2xxSuccessful()) {
                return null;
            }
            return extractUsername(response.getBody());
        } catch (Exception e) {
            log.debug("Nuxeo token validation unavailable: {}", e.getMessage());
            return null;
        }
    }

    private RestTemplate newRestTemplate() {
        SimpleClientHttpRequestFactory factory = new SimpleClientHttpRequestFactory();
        factory.setConnectTimeout(CONNECT_TIMEOUT_MS);
        factory.setReadTimeout(READ_TIMEOUT_MS);
        return new RestTemplate(factory);
    }

    private String buildNuxeoApiUrl() {
        String baseUrl = props.getBaseUrl();
        String trimmed = baseUrl.endsWith("/") ? baseUrl.substring(0, baseUrl.length() - 1) : baseUrl;
        return trimmed.endsWith("/api/v1") ? trimmed : trimmed + "/api/v1";
    }

    @SuppressWarnings("unchecked")
    private String extractUsername(Map body) {
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
