package org.hyland.contentlake.connector;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * The plugin connectors this application loaded (#124), keyed by source type.
 *
 * <p>A registry rather than beans, deliberately. Registering a plugin's {@code TextExtractor} or
 * {@code ScopeResolver} as a bean would make injection by type ambiguous in every ingester that already has
 * one of its own, so dropping a plugin jar next to an Alfresco ingester would break the Alfresco ingester.
 * The whole point of the plugin boundary is that a connector cannot do that. A consumer that wants a
 * plugin connector asks for it here, by name or by taking the only one there is.</p>
 *
 * <p>Empty in every application that loaded no plugin, which is all of them by default, so injecting this
 * changes nothing until a jar is mounted.</p>
 */
public class ConnectorRegistry {

    /**
     * Load order is kept as a list of its own: {@code Map.copyOf} of a {@link LinkedHashMap} does not
     * preserve iteration order, and the order connectors were loaded in is what a listing reports.
     */
    private final List<LoadedConnector> ordered;

    private final Map<String, LoadedConnector> bySourceType;
    private final List<String> problems;

    public ConnectorRegistry(List<LoadedConnector> connectors, List<String> problems) {
        Map<String, LoadedConnector> map = new LinkedHashMap<>();
        if (connectors != null) {
            for (LoadedConnector connector : connectors) {
                map.put(connector.sourceType(), connector);
            }
        }
        this.ordered = List.copyOf(map.values());
        this.bySourceType = Map.copyOf(map);
        this.problems = problems == null ? List.of() : List.copyOf(problems);
    }

    /** A registry for an application that loaded no plugins. */
    public static ConnectorRegistry empty() {
        return new ConnectorRegistry(List.of(), List.of());
    }

    /** Every plugin connector, in load order. */
    public List<LoadedConnector> connectors() {
        return ordered;
    }

    public Optional<LoadedConnector> bySourceType(String sourceType) {
        return Optional.ofNullable(bySourceType.get(sourceType));
    }

    /**
     * The single plugin connector, when exactly one was loaded.
     *
     * <p>What a host driven by a plugin connector uses: with one jar mounted there is no ambiguity, and with
     * several the host has to be told which one it means rather than picking.</p>
     */
    public Optional<LoadedConnector> single() {
        return ordered.size() == 1 ? Optional.of(ordered.getFirst()) : Optional.empty();
    }

    public boolean isEmpty() {
        return bySourceType.isEmpty();
    }

    /**
     * Everything that went wrong while loading, kept so it can be reported after startup rather than only
     * logged. A jar that failed to load is a fact an operator needs from the running service.
     */
    public List<String> problems() {
        return problems;
    }
}
