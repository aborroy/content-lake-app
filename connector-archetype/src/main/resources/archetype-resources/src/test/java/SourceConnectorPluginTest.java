package ${package};

import org.hyland.contentlake.spi.ConnectorContext;
import org.hyland.contentlake.spi.ConnectorPlugin;
import org.hyland.contentlake.spi.ConnectorSchema;
import org.hyland.contentlake.spi.ContentSourceClient;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * What a connector has to get right before any of the source's API is touched: it is discoverable, it
 * declares the settings it reads, and it builds a client from them.
 */
class SourceConnectorPluginTest {

    private final SourceConnectorPlugin plugin = new SourceConnectorPlugin();

    private final Map<String, String> config = new HashMap<>();

    private final ConnectorContext context = config::get;

    private void configureFully() {
        config.put("${sourceType}.url", "http://${sourceType}.example.com");
        config.put("${sourceType}.username", "ingester");
        config.put("${sourceType}.password", "hunter2");
    }

    /**
     * The check that catches the mistake nothing else does: a missing or misspelled
     * {@code META-INF/services} entry means the jar loads and the connector never appears.
     */
    @Test
    void isDiscoverableThroughTheServiceLoader() {
        List<ConnectorPlugin> discovered = ServiceLoader.load(ConnectorPlugin.class).stream()
                .map(ServiceLoader.Provider::get)
                .toList();

        assertThat(discovered).extracting(ConnectorPlugin::sourceType).contains("${sourceType}");
    }

    @Test
    void declaresItsSourceType() {
        assertThat(plugin.sourceType()).isEqualTo("${sourceType}");
        assertThat(plugin.displayName()).isNotBlank();
    }

    /** Every setting the plugin reads has to be in the schema, or nothing validates it. */
    @Test
    void publishesEverySettingItReads() {
        assertThat(plugin.schema().fields()).extracting(ConnectorSchema.Field::name)
                .containsExactlyElementsOf(SourceConnectorPlugin.settingNames());
    }

    @Test
    void marksTheCredentialAsSecret() {
        assertThat(plugin.schema().fields())
                .filteredOn(ConnectorSchema.Field::secret)
                .extracting(ConnectorSchema.Field::name)
                .containsExactly("${sourceType}.password");
    }

    @Test
    void reportsAMissingRequiredSettingByName() {
        List<String> problems = plugin.schema().validate(config::get);

        assertThat(problems).anySatisfy(problem -> assertThat(problem).contains("${sourceType}.url"));
    }

    @Test
    void aFullyConfiguredSchemaValidates() {
        configureFully();

        assertThat(plugin.schema().validate(config::get)).isEmpty();
    }

    @Test
    void buildsAClientThatAgreesAboutTheSourceType() {
        configureFully();

        ContentSourceClient client = plugin.createClient(context);

        assertThat(client).isNotNull();
        assertThat(client.getSourceType()).isEqualTo(plugin.sourceType());
        assertThat(client.connectorSchema()).isNotNull();
    }

    @Test
    void readsTheOptionalPageSizeOrItsDefault() {
        configureFully();
        assertThat(((SourceConnectorClient) plugin.createClient(context)).pageSize()).isEqualTo(100);

        config.put("${sourceType}.page-size", "250");
        assertThat(((SourceConnectorClient) plugin.createClient(context)).pageSize()).isEqualTo(250);
    }

    @Test
    void publishesAScopeResolver() {
        configureFully();
        ContentSourceClient client = plugin.createClient(context);

        assertThat(plugin.createScopeResolver(context, client)).isNotNull();
    }
}
