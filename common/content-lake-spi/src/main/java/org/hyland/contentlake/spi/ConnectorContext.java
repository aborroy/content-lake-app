package org.hyland.contentlake.spi;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

/**
 * The configuration a connector is built from, handed to a plugin at load time (#124).
 *
 * <p>A plugin is instantiated by the JDK {@code ServiceLoader} through a no-argument constructor, so it
 * cannot be given its settings by injection. This is how it reads them instead: the names are the ones its
 * {@link ConnectorSchema} declares, and the values come from wherever the host application resolves
 * configuration -- environment variables, a properties file, or Spring's {@code Environment}.</p>
 *
 * <p>Deliberately narrow. A plugin that could reach the application context could reach anything in it,
 * and the point of the plugin boundary is that a connector is written against the SPI and nothing
 * else.</p>
 */
public interface ConnectorContext {

    /**
     * The value configured for a setting, or {@code null} when it is unset.
     *
     * <p>A blank value is returned as-is: it is the {@link ConnectorSchema} that decides whether blank
     * counts as missing, and it does.</p>
     */
    String property(String name);

    /** The value, or {@code defaultValue} when unset or blank. */
    default String property(String name, String defaultValue) {
        String value = property(name);
        return value == null || value.isBlank() ? defaultValue : value;
    }

    /** The value as an int, or {@code defaultValue} when unset, blank or not a number. */
    default int intProperty(String name, int defaultValue) {
        String value = property(name);
        if (value == null || value.isBlank()) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(value.trim());
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    /** The value as a long, or {@code defaultValue} when unset, blank or not a number. */
    default long longProperty(String name, long defaultValue) {
        String value = property(name);
        if (value == null || value.isBlank()) {
            return defaultValue;
        }
        try {
            return Long.parseLong(value.trim());
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    /**
     * The value as a boolean, or {@code defaultValue} when unset or blank.
     *
     * <p>Only {@code true} and {@code false} are recognised, case-insensitively; anything else is the
     * default rather than {@code false}, so a typo does not silently turn a feature off. Schema validation
     * is what reports the typo.</p>
     */
    default boolean booleanProperty(String name, boolean defaultValue) {
        String value = property(name);
        if (value == null || value.isBlank()) {
            return defaultValue;
        }
        String trimmed = value.trim();
        if ("true".equalsIgnoreCase(trimmed)) {
            return true;
        }
        if ("false".equalsIgnoreCase(trimmed)) {
            return false;
        }
        return defaultValue;
    }

    /**
     * The value as a list: either a comma-separated single value or an indexed sequence
     * ({@code name[0]}, {@code name[1]}, ...), which is how a YAML list resolves.
     *
     * <p>Both forms are accepted because a connector should not care whether its deployment configures a
     * list through an environment variable or a configuration file. Blank entries are dropped and each
     * entry is trimmed.</p>
     *
     * @return the entries, empty when the setting is unset in both forms
     */
    default List<String> listProperty(String name) {
        List<String> values = new ArrayList<>();
        String flat = property(name);
        if (flat != null && !flat.isBlank()) {
            Arrays.stream(flat.split(","))
                    .map(String::trim)
                    .filter(entry -> !entry.isEmpty())
                    .forEach(values::add);
            return List.copyOf(values);
        }
        for (int index = 0; ; index++) {
            String indexed = property(name + "[" + index + "]");
            if (indexed == null) {
                break;
            }
            String trimmed = indexed.trim();
            if (!trimmed.isEmpty()) {
                values.add(trimmed);
            }
        }
        return List.copyOf(values);
    }

    /** The value as an upper-cased enum constant, or {@code defaultValue} when unset or unrecognised. */
    default <E extends Enum<E>> E enumProperty(String name, Class<E> type, E defaultValue) {
        String value = property(name);
        if (value == null || value.isBlank()) {
            return defaultValue;
        }
        try {
            return Enum.valueOf(type, value.trim().toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            return defaultValue;
        }
    }
}
