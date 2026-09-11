package org.hyland.contentlake.connector;

import org.hyland.contentlake.spi.ConnectorSchema;
import org.hyland.contentlake.spi.ContentSourceClient;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.stereotype.Controller;
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
 * <p>{@code @Controller} is required, not decorative. Spring Framework 7's
 * {@code RequestMappingHandlerMapping.isHandler} tests for {@code @Controller} and nothing else; a type-level
 * {@link RequestMapping} was enough in 6.x and is not any more. Without the stereotype this class is a bean
 * that maps no request, and both endpoints answer 404 in every application -- which is exactly what they did
 * until this was corrected.</p>
 *
 * <p>Being a stereotype it is component-scanned, so an application gets these endpoints by scanning core
 * rather than by registering a bean. A per-application {@code @Bean} on top of that would be a second
 * definition of one handler and fail startup with an ambiguous mapping, so the {@code @Bean} registrations
 * the ingesters used to carry are gone. An application that should not publish them opts out with a
 * {@code @ComponentScan} exclude filter, which is what {@code RagServiceApplication} does -- it ingests from
 * nothing and has no connector to describe. Default-deny security covers these endpoints wherever they are
 * published.</p>
 *
 * <p>Neither response carries a configured value, so neither can disclose a credential.</p>
 */
@Controller
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
