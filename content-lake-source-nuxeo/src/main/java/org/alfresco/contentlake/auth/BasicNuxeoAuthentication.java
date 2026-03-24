package org.alfresco.contentlake.auth;

import org.springframework.http.HttpHeaders;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

/**
 * Basic-auth implementation for server-to-server Nuxeo access.
 */
public class BasicNuxeoAuthentication implements NuxeoAuthentication {

    private final String authorizationHeader;

    public BasicNuxeoAuthentication(String username, String password) {
        String credentials = (username != null ? username : "") + ":" + (password != null ? password : "");
        this.authorizationHeader = "Basic " + Base64.getEncoder()
                .encodeToString(credentials.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public void apply(HttpHeaders headers) {
        headers.set(HttpHeaders.AUTHORIZATION, authorizationHeader);
    }
}
