package org.hyland.contentlake.rag.config;

import jakarta.servlet.DispatcherType;
import org.hyland.contentlake.rag.security.AlfrescoTicketAuthenticationFilter;
import org.hyland.contentlake.rag.security.DualSourceAuthenticationFilter;
import org.hyland.contentlake.rag.security.MultiSourceAuthenticationProvider;
import org.hyland.contentlake.rag.security.NuxeoTokenAuthenticationFilter;
import org.hyland.contentlake.rag.security.RagAuthenticationEntryPoint;
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
                                            MultiSourceAuthenticationProvider provider) throws Exception {
        return http
                .csrf(csrf -> csrf.disable())
                .sessionManagement(session -> session
                        .sessionCreationPolicy(SessionCreationPolicy.STATELESS))
                .authenticationProvider(provider)
                .addFilterBefore(new DualSourceAuthenticationFilter(provider),
                        BasicAuthenticationFilter.class)
                .addFilterBefore(new AlfrescoTicketAuthenticationFilter(authenticationManager),
                        BasicAuthenticationFilter.class)
                .addFilterBefore(new NuxeoTokenAuthenticationFilter(authenticationManager),
                        BasicAuthenticationFilter.class)
                .httpBasic(httpBasic -> httpBasic.authenticationEntryPoint(new RagAuthenticationEntryPoint()))
                .authorizeHttpRequests(auth -> auth
                        .dispatcherTypeMatchers(DispatcherType.ASYNC, DispatcherType.ERROR).permitAll()
                        .requestMatchers("/api/rag/health").permitAll()
                        .requestMatchers("/actuator/**").permitAll()
                        .anyRequest().authenticated()
                )
                .build();
    }

    @Bean
    AuthenticationManager authenticationManager(MultiSourceAuthenticationProvider provider) {
        return new ProviderManager(provider);
    }
}
