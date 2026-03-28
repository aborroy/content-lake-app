package org.hyland.nuxeo.contentlake.live.config;

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
 * Secures actuator endpoints for the Nuxeo live ingester.
 *
 * <p>The live ingester exposes no user-facing REST API, so only the sensitive
 * actuator endpoints (env, beans, etc.) are locked down. The health and info
 * probes remain public so that the container orchestrator can reach them without
 * credentials.</p>
 */
@Configuration
@EnableWebSecurity
public class NuxeoLiveSecurityConfig {

    @Bean
    SecurityFilterChain securityFilterChain(HttpSecurity http) throws Exception {
        return http
                .csrf(csrf -> csrf.disable())
                .sessionManagement(session -> session.sessionCreationPolicy(SessionCreationPolicy.STATELESS))
                .authorizeHttpRequests(auth -> auth
                        .dispatcherTypeMatchers(DispatcherType.ASYNC, DispatcherType.ERROR).permitAll()
                        .requestMatchers("/actuator/health", "/actuator/info").permitAll()
                        .requestMatchers("/actuator/**").authenticated()
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
                        .roles("LIVE_ADMIN")
                        .build()
        );
    }
}
