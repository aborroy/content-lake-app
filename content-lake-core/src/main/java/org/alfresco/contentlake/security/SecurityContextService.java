package org.alfresco.contentlake.security;

import lombok.extern.slf4j.Slf4j;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Service;

/**
 * Service to read the authenticated principal from the Spring Security context.
 */
@Slf4j
@Service
public class SecurityContextService {

    /**
     * Gets the authenticated username.
     *
     * @return username, or {@code "anonymous"} if no authenticated principal is present
     */
    public String getCurrentUsername() {
        Authentication authentication = SecurityContextHolder.getContext().getAuthentication();

        if (authentication == null || !authentication.isAuthenticated()) {
            return "anonymous";
        }

        return authentication.getName();
    }
}
