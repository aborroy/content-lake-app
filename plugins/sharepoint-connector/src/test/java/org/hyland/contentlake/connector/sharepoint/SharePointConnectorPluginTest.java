package org.hyland.contentlake.connector.sharepoint;

import org.hyland.contentlake.spi.ConnectorContext;
import org.hyland.contentlake.spi.ConnectorSchema;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The plugin's contract with the host: what it declares, and what it refuses to be built without.
 */
class SharePointConnectorPluginTest {

    /** A context backed by a map, which is all {@code ConnectorContext} is from a plugin's side. */
    private record MapContext(Map<String, String> values) implements ConnectorContext {
        @Override
        public String property(String name) {
            return values.get(name);
        }
    }

    private static Map<String, String> minimalClientCredentials() {
        Map<String, String> values = new HashMap<>();
        values.put("sharepoint.drive-ids", "b!drive-one");
        values.put("sharepoint.client-id", "client-guid");
        values.put("sharepoint.tenant-id", "tenant-guid");
        values.put("sharepoint.client-secret", "a-secret");
        return values;
    }

    @Test
    void isDiscoverableThroughTheServiceLoaderTheHostUses() {
        List<String> found = ServiceLoader.load(org.hyland.contentlake.spi.ConnectorPlugin.class).stream()
                .map(provider -> provider.get().sourceType())
                .toList();

        // The host loads plugins with the JDK ServiceLoader, so a missing or misspelled service file is the
        // difference between a working jar and a jar that is silently inert.
        assertThat(found).contains("sharepoint");
    }

    @Test
    void declaresTheSourceTypeThatPrefixesEveryDocumentsSourceId() {
        SharePointConnectorPlugin plugin = new SharePointConnectorPlugin();

        assertThat(plugin.sourceType()).isEqualTo("sharepoint");
        assertThat(plugin.displayName()).isEqualTo("SharePoint Online connector");
        assertThat(plugin.schema().sourceType()).isEqualTo("sharepoint");
    }

    @Test
    void marksCredentialsSecretSoTheyNeverAppearInAValidationMessageOrTheSchemaEndpoint() {
        ConnectorSchema schema = new SharePointConnectorPlugin().schema();

        List<String> secrets = schema.fields().stream()
                .filter(ConnectorSchema.Field::secret)
                .map(ConnectorSchema.Field::name)
                .toList();

        assertThat(secrets).contains("sharepoint.client-secret", "sharepoint.certificate-password",
                "sharepoint.access-token");
    }

    @Test
    void refusesToLoadWithoutTheSettingsThatDecideWhetherItCanWork() {
        ConnectorSchema schema = new SharePointConnectorPlugin().schema();

        List<String> problems = schema.validate(name -> null);

        // The host refuses a plugin whose required settings are missing, which is what keeps an
        // unconfigured SharePoint jar from loading and becoming a second candidate in a deployment that
        // mounts every connector.
        assertThat(problems).isNotEmpty();
        assertThat(String.join(" ", problems))
                .contains("sharepoint.drive-ids")
                .contains("sharepoint.client-id");
    }

    @Test
    void acceptsAMinimalClientCredentialsConfiguration() {
        ConnectorSchema schema = new SharePointConnectorPlugin().schema();
        Map<String, String> values = minimalClientCredentials();

        assertThat(schema.validate(values::get)).isEmpty();
    }

    @Test
    void derivesTheAuthorityFromTheTenantId() {
        SharePointConnectorSettings settings = new SharePointConnectorPlugin()
                .settingsFrom(new MapContext(minimalClientCredentials()));

        assertThat(settings.authority()).isEqualTo("https://login.microsoftonline.com/tenant-guid");
        assertThat(settings.authMode())
                .isEqualTo(SharePointConnectorSettings.AuthMode.CLIENT_CREDENTIALS);
        assertThat(settings.graphBaseUrl()).isEqualTo("https://graph.microsoft.com/v1.0");
    }

    @Test
    void saysWhichOfTwoSettingsToSupplyWhenNeitherIsThere() {
        Map<String, String> values = minimalClientCredentials();
        values.remove("sharepoint.tenant-id");

        // Conditionally required, which ConnectorSchema cannot express: the tenant id is needed for
        // client-credentials and meaningless for static-token.
        assertThatThrownBy(() -> new SharePointConnectorPlugin().settingsFrom(new MapContext(values)))
                .isInstanceOf(GraphException.class)
                .hasMessageContaining("sharepoint.tenant-id")
                .hasMessageContaining("sharepoint.authority");
    }

    @Test
    void needsNoTenantIdForAStaticTokenRun() {
        Map<String, String> values = new HashMap<>();
        values.put("sharepoint.drive-ids", "b!drive-one");
        values.put("sharepoint.client-id", "unused-but-required-by-the-schema");
        values.put("sharepoint.auth-mode", "static-token");
        values.put("sharepoint.access-token", "a-token");
        values.put("sharepoint.graph-base-url", "http://localhost:8099/v1.0");

        SharePointConnectorSettings settings = new SharePointConnectorPlugin()
                .settingsFrom(new MapContext(values));

        assertThat(settings.authMode()).isEqualTo(SharePointConnectorSettings.AuthMode.STATIC_TOKEN);
        assertThat(settings.authority()).isNull();
        // The two seams that make a mock run evidence about the cloud, and the only two.
        assertThat(settings.graphBaseUrl()).isEqualTo("http://localhost:8099/v1.0");
        assertThat(settings.tokenProvider().supportedInProduction()).isFalse();
    }

    @Test
    void buildsAClientWithoutTouchingTheNetwork() {
        Map<String, String> values = new HashMap<>();
        values.put("sharepoint.drive-ids", "b!drive-one");
        values.put("sharepoint.client-id", "client-guid");
        values.put("sharepoint.auth-mode", "static-token");
        values.put("sharepoint.access-token", "a-token");
        // Deliberately unreachable. The host builds a client for every mounted jar whose schema validates,
        // so construction reaching out would make mounting this jar slow or break a CMIS-only deployment.
        values.put("sharepoint.graph-base-url", "http://127.0.0.1:1/v1.0");

        assertThat(new SharePointConnectorPlugin().createClient(new MapContext(values))).isNotNull();
    }

    @Test
    void suppliesAScopeResolverAndNoTextExtractor() {
        MapContext context = new MapContext(Map.of("sharepoint.exclude-paths", "/Archive"));
        SharePointConnectorPlugin plugin = new SharePointConnectorPlugin();

        assertThat(plugin.createScopeResolver(context, null)).isInstanceOf(SharePointScopeResolver.class);
        // Graph does not convert content on the server the way Nuxeo does, so the host's chain is used.
        assertThat(plugin.createTextExtractor(context)).isNull();
    }

    @Test
    void defaultsTheResourceBudgetBelowTheDocumentedCap() {
        SharePointConnectorSettings settings = new SharePointConnectorPlugin()
                .settingsFrom(new MapContext(minimalClientCredentials()));

        // The cap is 1250 per application per tenant and is shared with anything else using the same app
        // registration, so aiming at it exactly would push the whole application into throttling.
        assertThat(settings.resourceUnitsPerMinute()).isLessThan(1_250);
        assertThat(settings.resourceUnitBurst()).isPositive();
    }

    @Test
    void offersNoAclOptionThatWidensAGroupGrantToTheWholeTenant() {
        ConnectorSchema schema = new SharePointConnectorPlugin().schema();

        List<String> groupGrantValues = schema.fields().stream()
                .filter(field -> field.name().equals("sharepoint.group-grants"))
                .flatMap(field -> field.allowedValues().stream())
                .toList();

        assertThat(groupGrantValues).containsExactlyInAnyOrder("map", "skip");
    }

    @Test
    void offersNoSyncAccountAclFallbackBecauseAppOnlyAuthHasNoAccount() {
        ConnectorSchema schema = new SharePointConnectorPlugin().schema();

        List<String> fallbackValues = schema.fields().stream()
                .filter(field -> field.name().equals("sharepoint.acl-fallback"))
                .flatMap(field -> field.allowedValues().stream())
                .toList();

        assertThat(fallbackValues).containsExactlyInAnyOrder("fail-closed", "public");
    }
}
