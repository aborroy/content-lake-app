package org.hyland.connector.contentlake.batch.config;

import org.junit.jupiter.api.Test;
import org.springframework.boot.env.YamlPropertySourceLoader;
import org.springframework.core.env.PropertySource;
import org.springframework.core.io.ClassPathResource;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Every bindable setting has to appear in {@code application.yml}.
 *
 * <p>The other ingesters check their {@code ConnectorSchema} against their own configuration; this host
 * publishes no schema, because the connector it runs publishes its own. The equivalent question here is
 * whether a setting can actually be set: a field that binds but is absent from {@code application.yml} has
 * no {@code ${ENV_VAR}} placeholder, so a compose deployment cannot reach it and its default is the only
 * value it will ever have.</p>
 */
class ConnectorBatchPropertiesCoverageTest {

    private static final String PREFIX = "connector";

    @Test
    void everyBindableSettingIsDefinedInApplicationYml() throws IOException {
        PropertySource<?> yml = applicationYml();
        List<String> names = propertyNames(ConnectorBatchProperties.class, PREFIX);

        assertThat(names).isNotEmpty();
        assertThat(names).allSatisfy(name ->
                assertThat(defines(yml, name))
                        .as("application.yml defines %s", name)
                        .isTrue());
    }

    /** The settings this host is built around, spelled out so a rename has to be deliberate. */
    @Test
    void theHostSettingsAreWhereTheDocumentationSaysTheyAre() throws IOException {
        PropertySource<?> yml = applicationYml();

        assertThat(defines(yml, "connector.source-type")).isTrue();
        assertThat(defines(yml, "connector.roots")).isTrue();
        assertThat(defines(yml, "connector.max-depth")).isTrue();
        // The loader's own settings belong to core and are shared with every other ingester, so they live
        // under a different prefix on purpose.
        assertThat(defines(yml, "content-lake.connector.plugin-directory")).isTrue();
        assertThat(defines(yml, "content-lake.connector.validation")).isTrue();
    }

    /** Credentials must have no default: an unset one has to fail startup, not fall back. */
    @Test
    void theSyncCredentialsResolveToBlankWhenUnset() throws IOException {
        PropertySource<?> yml = applicationYml();

        assertThat(yml.getProperty("connector.security.username").toString())
                .isEqualTo("${CONNECTOR_SYNC_USERNAME:}");
        assertThat(yml.getProperty("connector.security.password").toString())
                .isEqualTo("${CONNECTOR_SYNC_PASSWORD:}");
    }

    /** The sweep deletes documents over paths a connector reported, so it stays opt-in. */
    @Test
    void theReconciliationSweepIsOffByDefault() {
        assertThat(new ConnectorBatchProperties().getReconcile().isEnabled()).isFalse();
    }

    private static List<String> propertyNames(Class<?> type, String prefix) {
        List<String> names = new ArrayList<>();
        for (Field field : type.getDeclaredFields()) {
            if (field.isSynthetic() || Modifier.isStatic(field.getModifiers())) {
                continue;
            }
            String name = prefix + "." + kebab(field.getName());
            if (isNested(field.getType())) {
                names.addAll(propertyNames(field.getType(), name));
            } else {
                names.add(name);
            }
        }
        return names;
    }

    /** A settings group of ours, as opposed to a leaf value. */
    private static boolean isNested(Class<?> type) {
        Package pkg = type.getPackage();
        return pkg != null && pkg.getName().startsWith("org.hyland");
    }

    private static String kebab(String camel) {
        StringBuilder out = new StringBuilder();
        for (char c : camel.toCharArray()) {
            if (Character.isUpperCase(c)) {
                out.append('-').append(Character.toLowerCase(c));
            } else {
                out.append(c);
            }
        }
        return out.toString().toLowerCase(Locale.ROOT);
    }

    private static boolean defines(PropertySource<?> yml, String name) {
        return yml.containsProperty(name) || yml.containsProperty(name + "[0]");
    }

    private static PropertySource<?> applicationYml() throws IOException {
        List<PropertySource<?>> sources =
                new YamlPropertySourceLoader().load("application", new ClassPathResource("application.yml"));
        assertThat(sources).isNotEmpty();
        return sources.getFirst();
    }
}
