package org.hyland.contentlake.connector;

import org.hyland.contentlake.spi.ConnectorSchema;
import org.hyland.contentlake.spi.ContentSourceClient;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import java.util.ArrayList;
import java.util.List;

/**
 * Publishes what connectors this application has and what configuration they need (#123, #124).
 *
 * <ul>
 *   <li>{@code GET /api/connectors} -- which connectors are loaded, where each came from, and anything
 *       that failed to load. The first question about a plugin deployment is whether the jar was picked
 *       up at all, and a log line is a poor way to answer it from outside the container.</li>
 *   <li>{@code GET /api/connectors/schema} -- the settings each connector needs, so operator tooling need
 *       not hardcode a form per source type.</li>
 * </ul>
 *
 * <p>Both cover in-tree connectors and plugin ones. Plugin connectors come from the
 * {@link ConnectorRegistry} rather than from beans, because registering them as beans would make injection
 * by SPI type ambiguous in an ingester that already has its own.</p>
 *
 * <p>Not annotated as a {@code @RestController}: core is component-scanned by every application, so a
 * stereotype here would add these endpoints everywhere, including the RAG service, which has no connector.
 * Each ingester registers it as a {@code @Bean} instead, which also keeps it inside that application's own
 * security rules. Spring maps it regardless of how the bean was registered, because the class carries
 * {@link RequestMapping}.</p>
 *
 * <p>Neither response carries a configured value, so neither can disclose a credential.</p>
 */
@RequestMapping("/api/connectors")
@ResponseBody
public class ConnectorSchemaController {

    /** Origin reported for a connector that is a module of this build rather than a mounted jar. */
    static final String IN_TREE = "in-tree";

    private final ObjectProvider<ContentSourceClient> sourceClients;
    private final ObjectProvider<ConnectorRegistry> registries;

    public ConnectorSchemaController(ObjectProvider<ContentSourceClient> sourceClients,
                                     ObjectProvider<ConnectorRegistry> registries) {
        this.sourceClients = sourceClients;
        this.registries = registries;
    }

    /**
     * One connector.
     *
     * @param sourceType     the {@code cin_sourceId} prefix
     * @param displayName    what to call it
     * @param origin         {@code in-tree}, {@code classpath}, or the jar it was loaded from
     * @param implementation the client class, which is what identifies a plugin's code
     * @param settings       how many settings its schema declares
     */
    public record ConnectorInfo(String sourceType,
                                String displayName,
                                String origin,
                                String implementation,
                                int settings) {
    }

    /**
     * @param connectors what is loaded
     * @param problems   why anything else is not, empty when everything loaded
     */
    public record ConnectorListing(List<ConnectorInfo> connectors, List<String> problems) {
    }

    @GetMapping
    public ConnectorListing connectors() {
        List<ConnectorInfo> connectors = new ArrayList<>();
        for (ContentSourceClient client : sourceClients) {
            ConnectorSchema schema = schemaOf(client);
            connectors.add(new ConnectorInfo(
                    client.getSourceType(),
                    client.getSourceType(),
                    IN_TREE,
                    client.getClass().getName(),
                    schema.fields().size()));
        }
        for (LoadedConnector loaded : registry().connectors()) {
            connectors.add(new ConnectorInfo(
                    loaded.sourceType(),
                    loaded.displayName(),
                    loaded.origin(),
                    loaded.client().getClass().getName(),
                    loaded.schema().fields().size()));
        }
        return new ConnectorListing(connectors, registry().problems());
    }

    @GetMapping("/schema")
    public List<ConnectorSchema> schemas() {
        List<ConnectorSchema> schemas = new ArrayList<>();
        for (ContentSourceClient client : sourceClients) {
            schemas.add(schemaOf(client));
        }
        for (LoadedConnector loaded : registry().connectors()) {
            schemas.add(loaded.schema());
        }
        return schemas;
    }

    private static ConnectorSchema schemaOf(ContentSourceClient client) {
        ConnectorSchema schema = client.connectorSchema();
        return schema != null ? schema : ConnectorSchema.empty(client.getSourceType());
    }

    /** An application that never built a registry -- a sliced test, say -- reports no plugins. */
    private ConnectorRegistry registry() {
        return registries.getIfAvailable(ConnectorRegistry::empty);
    }
}
