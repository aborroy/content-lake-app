package org.hyland.contentlake.connector;

import org.hyland.contentlake.spi.ConnectorContext;
import org.springframework.core.env.Environment;

/**
 * The {@link ConnectorContext} a plugin gets in a Spring application: property lookup through the
 * {@link Environment}, and nothing else.
 *
 * <p>Nothing else on purpose. A context that exposed the application context would let a connector reach
 * every bean in the host, and the point of the plugin boundary is that a connector is written against the
 * SPI alone. Property lookup through the {@code Environment} means a plugin reads its settings from
 * whatever the deployment uses -- environment variables, {@code application.yml}, a config server -- with
 * relaxed binding applied exactly as it is for the in-tree connectors.</p>
 */
public class EnvironmentConnectorContext implements ConnectorContext {

    private final Environment environment;

    public EnvironmentConnectorContext(Environment environment) {
        this.environment = environment;
    }

    @Override
    public String property(String name) {
        return environment.getProperty(name);
    }
}
