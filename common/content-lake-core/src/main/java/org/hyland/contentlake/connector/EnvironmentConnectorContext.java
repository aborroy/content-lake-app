package org.hyland.contentlake.connector;

import org.hyland.contentlake.spi.ConnectorContext;
import org.springframework.core.env.Environment;

import java.util.Locale;

/**
 * The {@link ConnectorContext} a plugin gets in a Spring application: property lookup through the
 * {@link Environment}, and nothing else.
 *
 * <p>Nothing else on purpose. A context that exposed the application context would let a connector reach
 * every bean in the host, and the point of the plugin boundary is that a connector is written against the
 * SPI alone. Property lookup through the {@code Environment} means a plugin reads its settings from
 * whatever the deployment uses: environment variables, {@code application.yml}, a config server.</p>
 */
public class EnvironmentConnectorContext implements ConnectorContext {

    private final Environment environment;

    public EnvironmentConnectorContext(Environment environment) {
        this.environment = environment;
    }

    /**
     * The configured value, looked up as declared and then as an environment variable name.
     *
     * <p>The second lookup is not redundant. An in-tree connector's settings are bound with
     * {@code @ConfigurationProperties}, which applies Boot's relaxed name rules and so finds
     * {@code FILESYSTEM_ROOT_PATH} for {@code filesystem.root-path}. {@code Environment.getProperty} does
     * not: it maps {@code .} to {@code _} and upper-cases, but leaves {@code -} alone, so a hyphenated
     * setting resolves to {@code CMIS_PAGE-SIZE} and is never found. Without this a connector could declare
     * a setting in its schema that no deployment could supply from the environment, and it would fail
     * validation at startup with the operator having done nothing wrong.</p>
     */
    @Override
    public String property(String name) {
        String value = environment.getProperty(name);
        if (value != null || name == null) {
            return value;
        }
        String environmentVariableForm = name.replace('.', '_').replace('-', '_').toUpperCase(Locale.ROOT);
        return environmentVariableForm.equals(name) ? null : environment.getProperty(environmentVariableForm);
    }
}
