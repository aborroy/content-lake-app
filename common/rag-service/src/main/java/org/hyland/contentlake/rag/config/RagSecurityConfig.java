package org.hyland.contentlake.rag.config;

import jakarta.servlet.DispatcherType;
import org.hyland.contentlake.rag.security.AlfrescoDirectory;
import org.hyland.contentlake.rag.security.AlfrescoTicketAuthenticationFilter;
import org.hyland.contentlake.rag.security.DualSourceAuthenticationFilter;
import org.hyland.contentlake.rag.security.MultiSourceAuthenticationProvider;
import org.hyland.contentlake.rag.security.NuxeoDirectory;
import org.hyland.contentlake.rag.security.NuxeoTokenAuthenticationFilter;
import org.hyland.contentlake.rag.security.RagAuthenticationEntryPoint;
import org.hyland.contentlake.rag.security.RateLimitFilter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.authentication.ProviderManager;
import org.springframework.security.config.Customizer;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.authentication.www.BasicAuthenticationFilter;

/**
 * Security configuration for the RAG service.
 *
 * <p>Requires HTTP Basic Auth for all endpoints except the health check.
 * Credentials are validated against the configured content sources (Alfresco and/or Nuxeo)
 * via {@link MultiSourceAuthenticationProvider}.</p>
 */
@Configuration
@EnableWebSecurity
public class RagSecurityConfig {

    @Bean
    SecurityFilterChain securityFilterChain(HttpSecurity http,
                                            AuthenticationManager authenticationManager,
                                            MultiSourceAuthenticationProvider provider,
                                            AlfrescoDirectory alfrescoDirectory,
                                            NuxeoDirectory nuxeoDirectory,
                                            RagProperties ragProperties) throws Exception {
        return http
                .csrf(csrf -> csrf.disable())
                .sessionManagement(session -> session
                        .sessionCreationPolicy(SessionCreationPolicy.STATELESS))
                .authenticationProvider(provider)
                .addFilterBefore(new DualSourceAuthenticationFilter(alfrescoDirectory, nuxeoDirectory),
                        BasicAuthenticationFilter.class)
                .addFilterBefore(new AlfrescoTicketAuthenticationFilter(authenticationManager),
                        BasicAuthenticationFilter.class)
                .addFilterBefore(new NuxeoTokenAuthenticationFilter(authenticationManager),
                        BasicAuthenticationFilter.class)
                // Rate limiting runs AFTER authentication so it keys on the resolved principal (#75).
                .addFilterAfter(new RateLimitFilter(ragProperties), BasicAuthenticationFilter.class)
                .httpBasic(httpBasic -> httpBasic.authenticationEntryPoint(new RagAuthenticationEntryPoint()))
                .authorizeHttpRequests(auth -> auth
                        .dispatcherTypeMatchers(DispatcherType.ASYNC, DispatcherType.ERROR).permitAll()
                        .requestMatchers("/api/rag/health").permitAll()
                        // Only the container probes are public. /actuator/metrics stays exposed over HTTP
                        // but requires credentials: it enumerates endpoints and reveals request volumes
                        // and timings, which is free reconnaissance on a public reference deployment.
                        .requestMatchers("/actuator/health", "/actuator/info").permitAll()
                        // INVARIANT: the MCP endpoint (#61) and every other route stay authenticated.
                        // MCP tools derive the ACL identity from the authenticated request thread, so
                        // the endpoint must NEVER be added to the permit-all list above.
                        .anyRequest().authenticated()
                )
                .build();
    }

    @Bean
    AuthenticationManager authenticationManager(MultiSourceAuthenticationProvider provider) {
        return new ProviderManager(provider);
    }
}
