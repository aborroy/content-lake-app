package org.hyland.contentlake.connector;

import lombok.extern.slf4j.Slf4j;
import org.hyland.contentlake.spi.ConnectorSchema;
import org.hyland.contentlake.spi.ContentSourceClient;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * Checks each connector's configuration against the schema it publishes, before the application starts
 * serving (#123).
 *
 * <p>Without this, a missing or malformed setting surfaces as a downstream symptom: a repository URL
 * that is not a URL fails on the first request, and a filesystem root that is not mounted ingests
 * nothing and reads as an empty source. Both are configuration mistakes, and both are cheaper to state
 * at startup than to diagnose from the consequence.</p>
 *
 * <p>Runs as a {@link SmartInitializingSingleton} so every bean exists before the source clients are
 * asked for their schemas, and so a failure aborts the refresh rather than arriving after the web server
 * is already answering.</p>
 *
 * <p>An application with no {@link ContentSourceClient} -- the RAG service -- validates nothing, because
 * there is no connector to validate.</p>
 */
@Slf4j
@Component
public class ConnectorConfigurationValidator implements SmartInitializingSingleton {

    /** What to do about a configuration that does not satisfy its schema. */
    public enum Mode {

        /** Abort startup. The default: a connector that cannot work should not appear to be running. */
        FAIL,

        /** Log the problems and carry on, for a deployment whose mount or endpoint appears late. */
        WARN,

        /** Do not check at all. */
        OFF;

        static Mode parse(String value) {
            if (value == null || value.isBlank()) {
                return FAIL;
            }
            try {
                return valueOf(value.trim().toUpperCase(Locale.ROOT));
            } catch (IllegalArgumentException e) {
                log.warn("Unknown connector validation mode '{}'; using {}", value, FAIL);
                return FAIL;
            }
        }
    }

    private final ObjectProvider<ContentSourceClient> sourceClients;
    private final Environment environment;
    private final Mode mode;

    public ConnectorConfigurationValidator(
            ObjectProvider<ContentSourceClient> sourceClients,
            Environment environment,
            @Value("${content-lake.connector.validation:fail}") String mode) {
        this.sourceClients = sourceClients;
        this.environment = environment;
        this.mode = Mode.parse(mode);
    }

    @Override
    public void afterSingletonsInstantiated() {
        if (mode == Mode.OFF) {
            log.debug("Connector configuration validation is off");
            return;
        }

        List<String> problems = validate();
        if (problems.isEmpty()) {
            return;
        }

        String message = "Connector configuration is not usable:"
                + problems.stream().collect(java.util.stream.Collectors.joining("\n  - ", "\n  - ", ""));
        if (mode == Mode.WARN) {
            log.warn("{}\nStarting anyway because content-lake.connector.validation is warn", message);
            return;
        }
        throw new IllegalStateException(message
                + "\nFix the settings above, or set content-lake.connector.validation=warn to start anyway.");
    }

    /** Every problem across every connector in this application, in schema order. */
    List<String> validate() {
        List<String> problems = new ArrayList<>();
        for (ContentSourceClient client : sourceClients) {
            ConnectorSchema schema = client.connectorSchema();
            if (schema == null || schema.fields().isEmpty()) {
                log.debug("Source '{}' publishes no configuration schema; nothing to validate",
                        client.getSourceType());
                continue;
            }
            List<String> found = schema.validate(environment::getProperty);
            logSettings(schema);
            if (found.isEmpty()) {
                log.info("Connector '{}' configuration satisfies its schema ({} settings)",
                        schema.sourceType(), schema.fields().size());
            }
            problems.addAll(found);
        }
        return problems;
    }

    /**
     * The effective configuration at DEBUG, with every secret masked, so a deployment can be diagnosed
     * without a credential reaching the log.
     */
    private void logSettings(ConnectorSchema schema) {
        if (!log.isDebugEnabled()) {
            return;
        }
        for (ConnectorSchema.Field field : schema.fields()) {
            String value = environment.getProperty(field.name());
            String shown;
            if (value == null || value.isBlank()) {
                shown = "<unset>";
            } else {
                shown = field.secret() ? ConnectorSchema.MASK : value;
            }
            log.debug("  {} [{}{}] = {}", field.name(), field.type(),
                    field.required() ? ", required" : "", shown);
        }
    }
}
