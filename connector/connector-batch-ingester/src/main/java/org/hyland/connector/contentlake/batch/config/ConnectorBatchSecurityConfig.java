package org.hyland.connector.contentlake.batch.config;

import jakarta.servlet.DispatcherType;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.provisioning.InMemoryUserDetailsManager;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.util.StringUtils;

/**
 * Secures the connector batch ingester.
 *
 * <p>Spring Security is on this module's classpath transitively through {@code content-lake-core}. Without
 * an application-defined chain, Boot's default chain applies and authenticates every path against a
 * password generated at each boot, which leaves the documented sync trigger uncallable. This chain replaces
 * it.</p>
 *
 * <p>A plugin connector's own credentials authenticate the ingester to its source and say nothing about who
 * may trigger a re-ingest, so as with the filesystem ingester a single configured account guards the API.
 * Credentials come from {@code connector.security.*} and have no defaults.</p>
 */
@Configuration
@EnableWebSecurity
@EnableConfigurationProperties(ConnectorBatchProperties.class)
public class ConnectorBatchSecurityConfig {

    @Bean
    SecurityFilterChain securityFilterChain(HttpSecurity http) throws Exception {
        return http
                .csrf(csrf -> csrf.disable())
                .sessionManagement(session -> session.sessionCreationPolicy(SessionCreationPolicy.STATELESS))
                .authorizeHttpRequests(auth -> auth
                        // Async/error redispatches can happen after the initial authenticated request
                        // has already started.
                        .dispatcherTypeMatchers(DispatcherType.ASYNC, DispatcherType.ERROR).permitAll()
                        .requestMatchers("/actuator/health", "/actuator/info").permitAll()
                        // INVARIANT: default deny. Only the container probes above are public, so a new
                        // endpoint is authenticated unless someone deliberately exempts it here. Do not
                        // reintroduce anyRequest().permitAll(). /api/connectors is covered by this and must
                        // stay covered: a connector schema names every setting a source needs.
                        .anyRequest().authenticated()
                )
                .httpBasic(httpBasic -> {})
                .build();
    }

    @Bean
    UserDetailsService userDetailsService(ConnectorBatchProperties props) {
        ConnectorBatchProperties.Security security = props.getSecurity();
        require(security.getUsername(), "connector.security.username");
        require(security.getPassword(), "connector.security.password");

        return new InMemoryUserDetailsManager(
                User.withUsername(security.getUsername())
                        .password("{noop}" + security.getPassword())
                        .roles("SYNC_ADMIN")
                        .build()
        );
    }

    /**
     * Fails startup rather than falling back to a default. A default password here would be a published
     * credential on a service whose only endpoint triggers a full re-ingest.
     */
    private static void require(String value, String property) {
        if (!StringUtils.hasText(value)) {
            throw new IllegalStateException(
                    property + " must be set. The connector batch ingester has no source user directory to "
                            + "authenticate against and ships no default credential.");
        }
    }
}
