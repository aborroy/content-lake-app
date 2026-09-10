package org.hyland.contentlake.connector;

import lombok.extern.slf4j.Slf4j;
import org.hyland.contentlake.spi.ConnectorContext;
import org.hyland.contentlake.spi.ConnectorPlugin;
import org.hyland.contentlake.spi.ConnectorSchema;
import org.hyland.contentlake.spi.ContentSourceClient;
import org.hyland.contentlake.spi.ScopeResolver;
import org.hyland.contentlake.spi.TextExtractor;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Locale;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;
import java.util.stream.Stream;

/**
 * Loads source connectors from jars in a directory, so a connector can be built, shipped and iterated on
 * without touching this build or the deployment repository (#124).
 *
 * <h3>Why a directory of jars rather than a module</h3>
 * <p>An in-tree source needs a Maven module, a line in an intermediate POM and a COPY line in all six
 * service Dockerfiles, because each one parses the whole reactor from the root POM. Miss one and that
 * service's build fails at reactor parse time, for a module it does not even use. That is a documented
 * critical rule because it keeps happening, and it means a connector cannot be developed independently of
 * the deployment repository. A jar in a directory has none of those properties.</p>
 *
 * <h3>How discovery works</h3>
 * <p>A {@link URLClassLoader} over the directory's jars, with this class's loader as parent, then the JDK
 * {@link ServiceLoader} over {@link ConnectorPlugin}. Parent-first delegation is deliberate: the SPI types
 * a plugin implements have to be <em>the same</em> classes the host uses, or the cast would fail, so a
 * plugin that bundles its own copy of {@code content-lake-spi} still resolves to the host's. The cost is
 * that a plugin cannot pin a different version of a library the host already carries, which is why a
 * connector should depend on the SPI as {@code provided} and keep its dependencies to itself.</p>
 *
 * <p>Because the plugin loader's parent is the application classloader, the same scan also finds plugins
 * that shipped inside the application. Those are reported with origin {@code classpath}.</p>
 *
 * <h3>Failure is per plugin, never fatal</h3>
 * <p>A jar that cannot be opened, a service entry naming a class that is not there, a plugin whose
 * constructor throws, a plugin claiming a source type another connector already has: each is logged
 * against its origin and skipped. An ingester with one broken plugin and three working ones has three
 * working connectors. The one exception is configuration: a plugin whose settings do not satisfy its own
 * schema is refused, and in {@code fail} mode that aborts startup, because a connector that cannot work
 * should not appear to be running.</p>
 */
@Slf4j
public class ConnectorPluginLoader {

    /** What a load produced: the connectors, and everything that went wrong. */
    public record LoadResult(List<LoadedConnector> connectors, List<String> problems) {

        public LoadResult {
            connectors = connectors == null ? List.of() : List.copyOf(connectors);
            problems = problems == null ? List.of() : List.copyOf(problems);
        }

        static LoadResult empty() {
            return new LoadResult(List.of(), List.of());
        }
    }

    private static final String JAR_SUFFIX = ".jar";

    private final ConnectorContext context;
    private final ConnectorConfigurationValidator.Mode validationMode;

    /**
     * @param context        what plugins read their settings from
     * @param validationMode what to do about a plugin whose configuration does not satisfy its schema
     */
    public ConnectorPluginLoader(ConnectorContext context,
                                 ConnectorConfigurationValidator.Mode validationMode) {
        this.context = context;
        this.validationMode = validationMode;
    }

    /**
     * Discovers and builds every connector in {@code pluginDirectory}, plus any that shipped on the
     * classpath.
     *
     * @param pluginDirectory directory of connector jars. A {@code null}, blank, missing or unreadable
     *                        path means "no plugin directory", which is the normal case and not a problem:
     *                        a deployment that mounts no connectors should start quietly.
     * @param existingSourceTypes source types the host already has in-tree, which a plugin may not claim
     */
    public LoadResult load(String pluginDirectory, List<String> existingSourceTypes) {
        ClassLoader loader = pluginClassLoader(pluginDirectory);
        if (loader == null) {
            return LoadResult.empty();
        }

        List<LoadedConnector> connectors = new ArrayList<>();
        List<String> problems = new ArrayList<>();
        Map<String, String> claimedTypes = new LinkedHashMap<>();
        for (String existing : existingSourceTypes == null ? List.<String>of() : existingSourceTypes) {
            claimedTypes.put(existing, "in-tree");
        }

        // Iterated by hand rather than with a for-each, so a ServiceConfigurationError from one entry
        // does not end the iteration over the others.
        Iterator<ConnectorPlugin> plugins = ServiceLoader.load(ConnectorPlugin.class, loader).iterator();
        while (true) {
            ConnectorPlugin plugin;
            try {
                if (!plugins.hasNext()) {
                    break;
                }
                plugin = plugins.next();
            } catch (ServiceConfigurationError | RuntimeException e) {
                // A malformed jar, a service file naming a missing class, a constructor that threw.
                problems.add("A connector plugin could not be instantiated: " + rootMessage(e));
                log.error("Skipping a connector plugin that could not be instantiated", e);
                continue;
            }

            Outcome outcome = build(plugin, claimedTypes);
            if (outcome.connector() != null) {
                connectors.add(outcome.connector());
            }
            problems.addAll(outcome.problems());
        }

        if (!connectors.isEmpty()) {
            log.info("Loaded {} connector plugin(s): {}", connectors.size(),
                    connectors.stream().map(c -> c.sourceType() + " (" + c.origin() + ")").toList());
        }
        return new LoadResult(connectors, problems);
    }

    /** One plugin's outcome: the connector it produced, or why it produced none. */
    private record Outcome(LoadedConnector connector, List<String> problems) {

        static Outcome refused(String problem) {
            return new Outcome(null, List.of(problem));
        }

        static Outcome refused(List<String> problems) {
            return new Outcome(null, List.copyOf(problems));
        }

        static Outcome loaded(LoadedConnector connector) {
            return new Outcome(connector, List.of());
        }
    }

    private Outcome build(ConnectorPlugin plugin, Map<String, String> claimedTypes) {
        String origin = originOf(plugin);
        String sourceType;
        ConnectorSchema schema;
        try {
            sourceType = plugin.sourceType();
            schema = plugin.schema();
        } catch (Exception e) {
            log.error("Skipping connector plugin {} from {}: it could not describe itself",
                    plugin.getClass().getName(), origin, e);
            return Outcome.refused("Connector plugin " + plugin.getClass().getName() + " from " + origin
                    + " could not describe itself: " + rootMessage(e));
        }

        if (sourceType == null || sourceType.isBlank()) {
            log.error("Skipping connector plugin {} from {}: it declares no source type",
                    plugin.getClass().getName(), origin);
            return Outcome.refused("Connector plugin " + plugin.getClass().getName() + " from " + origin
                    + " declares no source type");
        }

        String claimedBy = claimedTypes.get(sourceType);
        if (claimedBy != null) {
            // The source type is the prefix of cin_sourceId, so two connectors sharing one would make
            // their documents indistinguishable in the index.
            log.error("Skipping connector plugin from {}: source type '{}' is already provided by {}",
                    origin, sourceType, claimedBy);
            return Outcome.refused("Connector plugin from " + origin + " claims source type '" + sourceType
                    + "', which is already provided by " + claimedBy);
        }

        if (schema != null && validationMode != ConnectorConfigurationValidator.Mode.OFF) {
            List<String> configurationProblems = schema.validate(context::property);
            if (!configurationProblems.isEmpty()) {
                log.error("Connector plugin '{}' from {} is not usable with this configuration: {}",
                        sourceType, origin, configurationProblems);
                return Outcome.refused(configurationProblems);
            }
        }

        try {
            ContentSourceClient client = plugin.createClient(context);
            if (client == null) {
                log.error("Skipping connector plugin '{}' from {}: it built no client", sourceType, origin);
                return Outcome.refused("Connector plugin '" + sourceType + "' from " + origin
                        + " built no client");
            }
            ScopeResolver scopeResolver = plugin.createScopeResolver(context, client);
            TextExtractor textExtractor = plugin.createTextExtractor(context);

            claimedTypes.put(sourceType, origin);
            return Outcome.loaded(new LoadedConnector(
                    sourceType,
                    displayName(plugin, sourceType),
                    origin,
                    schema != null ? schema : ConnectorSchema.empty(sourceType),
                    client,
                    scopeResolver,
                    textExtractor));
        } catch (Exception e) {
            log.error("Skipping connector plugin '{}' from {}: it failed to build", sourceType, origin, e);
            return Outcome.refused("Connector plugin '" + sourceType + "' from " + origin
                    + " failed to build: " + rootMessage(e));
        }
    }

    private static String displayName(ConnectorPlugin plugin, String fallback) {
        try {
            String name = plugin.displayName();
            return name == null || name.isBlank() ? fallback : name;
        } catch (Exception e) {
            return fallback;
        }
    }

    /**
     * A classloader over the directory's jars, or the application's own loader when there is no directory
     * to add. Returns {@code null} only when there is nothing to scan at all, which cannot happen here:
     * the application classloader always exists, so a plugin shipped in-jar is still found.
     */
    private ClassLoader pluginClassLoader(String pluginDirectory) {
        ClassLoader parent = getClass().getClassLoader();
        if (pluginDirectory == null || pluginDirectory.isBlank()) {
            log.debug("No connector plugin directory configured; scanning the classpath only");
            return parent;
        }

        Path directory = Path.of(pluginDirectory.trim());
        if (!Files.isDirectory(directory)) {
            // Not a problem: the normal deployment mounts no connectors.
            log.debug("Connector plugin directory {} does not exist; scanning the classpath only", directory);
            return parent;
        }
        if (!Files.isReadable(directory)) {
            log.warn("Connector plugin directory {} is not readable; scanning the classpath only", directory);
            return parent;
        }

        List<URL> jars = jarsIn(directory);
        if (jars.isEmpty()) {
            log.info("Connector plugin directory {} holds no jars", directory);
            return parent;
        }
        log.info("Scanning {} connector jar(s) in {}", jars.size(), directory);
        return new URLClassLoader(jars.toArray(URL[]::new), parent);
    }

    private static List<URL> jarsIn(Path directory) {
        try (Stream<Path> entries = Files.list(directory)) {
            List<URL> jars = new ArrayList<>();
            for (Path entry : entries.sorted(Comparator.comparing(Path::getFileName)).toList()) {
                if (!Files.isRegularFile(entry)
                        || !entry.getFileName().toString().toLowerCase(Locale.ROOT)
                        .endsWith(JAR_SUFFIX)) {
                    continue;
                }
                try {
                    jars.add(entry.toUri().toURL());
                } catch (MalformedURLException e) {
                    log.error("Ignoring connector jar {}: {}", entry, e.getMessage());
                }
            }
            return jars;
        } catch (IOException e) {
            log.error("Could not list connector plugin directory {}: {}", directory, e.getMessage());
            return List.of();
        }
    }

    /** The jar a plugin came from, so a problem can be traced to the file that has to be replaced. */
    private static String originOf(ConnectorPlugin plugin) {
        try {
            var source = plugin.getClass().getProtectionDomain().getCodeSource();
            if (source == null || source.getLocation() == null) {
                return LoadedConnector.CLASSPATH_ORIGIN;
            }
            String location = source.getLocation().getPath();
            if (!location.toLowerCase(Locale.ROOT).endsWith(JAR_SUFFIX)) {
                return LoadedConnector.CLASSPATH_ORIGIN;
            }
            int lastSlash = location.lastIndexOf('/');
            return lastSlash >= 0 ? location.substring(lastSlash + 1) : location;
        } catch (Exception e) {
            return LoadedConnector.CLASSPATH_ORIGIN;
        }
    }

    private static String rootMessage(Throwable error) {
        Throwable cause = error;
        while (cause.getCause() != null && cause.getCause() != cause) {
            cause = cause.getCause();
        }
        String message = cause.getMessage();
        return message == null || message.isBlank()
                ? cause.getClass().getSimpleName()
                : cause.getClass().getSimpleName() + ": " + message;
    }
}
