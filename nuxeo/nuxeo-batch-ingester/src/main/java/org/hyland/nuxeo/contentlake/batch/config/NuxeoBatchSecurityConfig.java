package org.hyland.nuxeo.contentlake.batch.config;

import jakarta.servlet.DispatcherType;
import org.hyland.nuxeo.contentlake.config.NuxeoProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.provisioning.InMemoryUserDetailsManager;
import org.springframework.security.web.SecurityFilterChain;

/**
 * Secures the Nuxeo sync API with the configured Nuxeo service credentials.
 *
 * <p>This keeps the public proxy behavior aligned with the existing
 * batch-ingester while avoiding a second authentication integration in issue 22.</p>
 */
@Configuration
@EnableWebSecurity
public class NuxeoBatchSecurityConfig {

    @Bean
    SecurityFilterChain securityFilterChain(HttpSecurity http) throws Exception {
        return http
                .csrf(csrf -> csrf.disable())
                .sessionManagement(session -> session.sessionCreationPolicy(SessionCreationPolicy.STATELESS))
                .authorizeHttpRequests(auth -> auth
                        .dispatcherTypeMatchers(DispatcherType.ASYNC, DispatcherType.ERROR).permitAll()
                        .requestMatchers("/actuator/health", "/actuator/info").permitAll()
                        .requestMatchers("/api/**").authenticated()
                        .anyRequest().permitAll()
                )
                .httpBasic(httpBasic -> {})
                .build();
    }

    @Bean
    UserDetailsService userDetailsService(NuxeoProperties props) {
        return new InMemoryUserDetailsManager(
                User.withUsername(props.getUsername())
                        .password("{noop}" + props.getPassword())
                        .roles("SYNC_ADMIN")
                        .build()
        );
    }
}
