package org.hyland.contentlake.spi;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.UnaryOperator;

/**
 * Machine-readable description of the configuration a source connector needs.
 *
 * <p>Connector configuration lives in environment variables described in prose, so nothing checked a
 * deployment before startup and a missing setting surfaced as a confusing downstream symptom rather
 * than a statement that a setting is missing. A schema makes the requirement explicit: it is what
 * {@link #validate} checks a running configuration against, and what operator tooling can read instead
 * of hardcoding a form per source.</p>
 *
 * <p>The schema describes the <em>connector's own</em> settings -- how to reach the source and what part
 * of it is in scope. Shared pipeline configuration (hxpr connection, embedding model, chunking,
 * extraction engines) and per-ingester scheduling are deliberately outside it: they are not properties
 * of the source, and describing them here would put the same fields in every source's schema.</p>
 *
 * <p>Values never appear in a schema, only field descriptors, so publishing one cannot disclose a
 * credential. {@link Field#secret()} marks the fields whose values must stay out of validation messages
 * and logs.</p>
 *
 * @param sourceType the {@code cin_sourceId} prefix this schema belongs to (e.g. {@code "nuxeo"})
 * @param fields     the connector's settings, in the order they are worth reading
 */
public record ConnectorSchema(String sourceType, List<Field> fields) {

    /** Stands in for a secret's value wherever one would otherwise be printed. */
    public static final String MASK = "***";

    public ConnectorSchema {
        fields = fields == null ? List.of() : List.copyOf(fields);
    }

    /** A source that declares nothing, which is what an adapter written before this gets. */
    public static ConnectorSchema empty(String sourceType) {
        return new ConnectorSchema(sourceType, List.of());
    }

    public static Builder builder(String sourceType) {
        return new Builder(sourceType);
    }

    /**
     * What is wrong with a configuration, as sentences naming the setting at fault.
     *
     * <p>Every problem is reported rather than only the first, so an operator fixing a deployment sees
     * the whole list instead of discovering the next one on the following restart.</p>
     *
     * @param valueLookup resolves a field name to its effective value, {@code null} when unset. In an
     *                    application this is the Spring {@code Environment}; a test can pass a map.
     * @return the problems found, empty when the configuration satisfies the schema
     */
    public List<String> validate(UnaryOperator<String> valueLookup) {
        List<String> problems = new ArrayList<>();
        for (Field field : fields) {
            String value = valueLookup.apply(field.name());
            if (value == null || value.isBlank()) {
                if (field.required()) {
                    problems.add("Missing required setting '" + field.name() + "' (" + field.description() + ")");
                }
                continue;
            }
            field.malformed(value.trim()).ifPresent(problems::add);
        }
        return List.copyOf(problems);
    }

    /** What a field's value has to look like. */
    public enum FieldType {

        STRING,
        INTEGER,
        BOOLEAN,

        /** An absolute URL with a scheme and a host, such as a repository endpoint. */
        URL,

        /** A directory that has to exist and be readable by the ingester process. */
        DIRECTORY,

        /** Comma-separated values, or a YAML list when configured through a file. */
        LIST,

        /** One of {@link Field#allowedValues()}, matched case-insensitively. */
        ENUM
    }

    /**
     * One configuration setting.
     *
     * @param name          the property name, as Spring resolves it (e.g. {@code nuxeo.base-url})
     * @param type          what the value has to look like
     * @param description   what the setting does, in one line, for an operator reading it cold
     * @param required      whether the connector cannot work without it. A setting with a usable
     *                      default is not required, even when it is always set in practice
     * @param secret        whether the value is a credential, and so must never be printed
     * @param allowedValues the permitted values for {@link FieldType#ENUM}, empty otherwise
     */
    public record Field(String name,
                        FieldType type,
                        String description,
                        boolean required,
                        boolean secret,
                        List<String> allowedValues) {

        public Field {
            allowedValues = allowedValues == null ? List.of() : List.copyOf(allowedValues);
        }

        /** Why this value does not fit the field, or empty when it does. */
        Optional<String> malformed(String value) {
            String shown = secret ? MASK : "'" + value + "'";
            return switch (type) {
                case INTEGER -> isLong(value)
                        ? Optional.empty()
                        : Optional.of(problem("expects a whole number, got " + shown));
                case BOOLEAN -> "true".equalsIgnoreCase(value) || "false".equalsIgnoreCase(value)
                        ? Optional.empty()
                        : Optional.of(problem("expects true or false, got " + shown));
                case URL -> isAbsoluteUrl(value)
                        ? Optional.empty()
                        : Optional.of(problem("expects an absolute URL with a scheme and a host, got "
                                + shown));
                case DIRECTORY -> directoryProblem(value).map(this::problem);
                case ENUM -> allowedValues.stream().anyMatch(allowed -> allowed.equalsIgnoreCase(value))
                        ? Optional.empty()
                        : Optional.of(problem("expects one of " + allowedValues + ", got " + shown));
                case STRING, LIST -> Optional.empty();
            };
        }

        private String problem(String detail) {
            return "Setting '" + name + "' " + detail;
        }

        private static boolean isLong(String value) {
            try {
                Long.parseLong(value);
                return true;
            } catch (NumberFormatException e) {
                return false;
            }
        }

        private static boolean isAbsoluteUrl(String value) {
            try {
                URI uri = URI.create(value);
                return uri.getScheme() != null && uri.getHost() != null;
            } catch (IllegalArgumentException e) {
                return false;
            }
        }

        /**
         * A path that does not exist is the misconfiguration worth catching at startup: an ingester
         * pointed at the wrong mount reports zero documents rather than an error, which reads like an
         * empty source.
         */
        private static Optional<String> directoryProblem(String value) {
            try {
                Path path = Path.of(value);
                if (!Files.exists(path)) {
                    return Optional.of("points at '" + value + "', which does not exist");
                }
                if (!Files.isDirectory(path)) {
                    return Optional.of("points at '" + value + "', which is not a directory");
                }
                if (!Files.isReadable(path)) {
                    return Optional.of("points at '" + value + "', which is not readable");
                }
                return Optional.empty();
            } catch (InvalidPathException e) {
                return Optional.of("is not a usable path: '" + value + "'");
            }
        }
    }

    /** Reads like the documentation it replaces, so an adapter's schema stays scannable. */
    public static final class Builder {

        private final String sourceType;
        private final List<Field> fields = new ArrayList<>();

        private Builder(String sourceType) {
            this.sourceType = sourceType;
        }

        /** A setting the connector cannot work without. */
        public Builder required(String name, FieldType type, String description) {
            fields.add(new Field(name, type, description, true, false, List.of()));
            return this;
        }

        /** A setting with a usable default. */
        public Builder optional(String name, FieldType type, String description) {
            fields.add(new Field(name, type, description, false, false, List.of()));
            return this;
        }

        /** A credential. Always a string, never printed. */
        public Builder secret(String name, String description, boolean required) {
            fields.add(new Field(name, FieldType.STRING, description, required, true, List.of()));
            return this;
        }

        public Builder enumeration(String name, String description, boolean required,
                                   List<String> allowedValues) {
            fields.add(new Field(name, FieldType.ENUM, description, required, false, allowedValues));
            return this;
        }

        public ConnectorSchema build() {
            return new ConnectorSchema(sourceType, fields);
        }
    }
}
