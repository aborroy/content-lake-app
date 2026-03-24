package org.alfresco.contentlake.client;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.MediaType;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.RestClient;

import java.time.Instant;

/**
 * Manages OAuth2 token acquisition and caching for the HXPR API.
 * <p>
 * Tokens are cached and refreshed 60 seconds before expiry.
 */
@Slf4j
public class HxprTokenProvider {

    private final RestClient idpClient;
    private final String clientId;
    private final String clientSecret;
    private final String username;
    private final String password;

    private String cachedToken;
    private Instant tokenExpiry;

    public HxprTokenProvider(String tokenUrl, String clientId, String clientSecret,
                             String username, String password) {
        this.idpClient = RestClient.builder().baseUrl(tokenUrl).build();
        this.clientId = clientId;
        this.clientSecret = clientSecret;
        this.username = username;
        this.password = password;
    }

    public synchronized String getToken() {
        if (cachedToken != null && Instant.now().isBefore(tokenExpiry)) {
            return cachedToken;
        }

        log.debug("Fetching new token from IDP");

        MultiValueMap<String, String> formData = new LinkedMultiValueMap<>();
        formData.add("grant_type", "password");
        formData.add("scope", "openid profile email");
        formData.add("client_id", clientId);
        formData.add("client_secret", clientSecret);
        formData.add("username", username);
        formData.add("password", password);

        TokenResponse response = idpClient.post()
                .contentType(MediaType.APPLICATION_FORM_URLENCODED)
                .body(formData)
                .retrieve()
                .body(TokenResponse.class);

        cachedToken = response.accessToken;
        tokenExpiry = Instant.now().plusSeconds(response.expiresIn - 60);
        log.debug("Token obtained, expires in {} seconds", response.expiresIn);
        return cachedToken;
    }

    @Data
    static class TokenResponse {
        @JsonProperty("access_token")
        String accessToken;
        @JsonProperty("expires_in")
        long expiresIn;
    }
}
