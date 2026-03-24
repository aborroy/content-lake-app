package org.alfresco.contentlake.security;

import jakarta.servlet.DispatcherType;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpStatus;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.authentication.ProviderManager;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.authentication.www.BasicAuthenticationFilter;

import java.util.List;

/**
 * Spring Security configuration for the batch ingester REST API.
 *
 * <p>Secures all API endpoints with Alfresco authentication.
 * Supports both username/password (Basic Auth) and Alfresco ticket authentication.
 * Operations are performed as the authenticated user with their Alfresco permissions.</p>
 */
@Configuration
@EnableWebSecurity
public class SecurityConfig {

    private final AlfrescoAuthenticationProvider alfrescoAuthProvider;
    private final AlfrescoTicketAuthenticationProvider alfrescoTicketAuthProvider;

    public SecurityConfig(@Value("${content.service.url}") String alfrescoUrl) {
        this.alfrescoAuthProvider = new AlfrescoAuthenticationProvider(alfrescoUrl);
        this.alfrescoTicketAuthProvider = new AlfrescoTicketAuthenticationProvider(alfrescoUrl);
    }

    /**
     * Configures the security filter chain.
     * - CSRF disabled (stateless API)
     * - Actuator endpoints public
     * - All /api/** endpoints require authentication
     * - Supports both Basic Auth and Alfresco tickets
     */
    @Bean
    SecurityFilterChain securityFilterChain(HttpSecurity http, AuthenticationManager authenticationManager) throws Exception {
        return http
                .csrf(csrf -> csrf.disable())
                .sessionManagement(session -> session
                        .sessionCreationPolicy(SessionCreationPolicy.STATELESS))
                .authorizeHttpRequests(auth -> auth
                        // Async/error redispatches can happen after the initial
                        // authenticated request has already started streaming.
                        .dispatcherTypeMatchers(DispatcherType.ASYNC, DispatcherType.ERROR).permitAll()
                        .requestMatchers("/actuator/health", "/actuator/info").permitAll()
                        .requestMatchers("/api/**").authenticated()
                        .anyRequest().permitAll()
                )
                .addFilterBefore(
                        new AlfrescoTicketAuthenticationFilter(authenticationManager),
                        BasicAuthenticationFilter.class
                )
                .httpBasic(httpBasic -> {
                    httpBasic.authenticationEntryPoint((request, response, authException) -> {
                        response.setHeader("WWW-Authenticate", "Basic realm=\"Alfresco Content Lake\"");
                        response.sendError(HttpStatus.UNAUTHORIZED.value(), "Alfresco credentials or ticket required");
                    });
                })
                .build();
    }

    /**
     * Configures the authentication manager with Alfresco authentication providers.
     * Ticket authentication is checked first, then username/password.
     *
     * Uses a standalone ProviderManager (no parent) to prevent infinite recursion.
     * When using HttpSecurity's AuthenticationManagerBuilder, Spring Security 6.x
     * wires a parent AuthenticationManager that re-delegates failed authentications,
     * causing a StackOverflowError when all providers reject the token.
     */
    @Bean
    public AuthenticationManager authenticationManager() {
        return new ProviderManager(
                List.of(alfrescoTicketAuthProvider, alfrescoAuthProvider)
        );
    }
}
