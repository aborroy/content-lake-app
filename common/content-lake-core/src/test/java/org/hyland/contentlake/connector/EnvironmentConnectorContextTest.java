package org.hyland.contentlake.connector;

import org.hyland.contentlake.spi.ConnectorContext;
import org.junit.jupiter.api.Test;
import org.springframework.core.env.MapPropertySource;
import org.springframework.core.env.StandardEnvironment;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * How a plugin's settings are found.
 *
 * <p>The hyphen cases are the point. {@code Environment.getProperty} maps {@code .} to {@code _} and
 * upper-cases, but leaves {@code -} alone, so before the fallback a schema field named
 * {@code sample.page-size} could not be supplied from the environment at all: the connector would fail
 * validation at startup with the operator having set the obvious variable.</p>
 */
class EnvironmentConnectorContextTest {

    @Test
    void findsASettingDeclaredExactly() {
        assertThat(context(Map.of("cmis.url", "http://repo:8080")).property("cmis.url"))
                .isEqualTo("http://repo:8080");
    }

    @Test
    void findsAHyphenatedSettingFromAnEnvironmentVariableName() {
        assertThat(context(Map.of("CMIS_PAGE_SIZE", "50")).property("cmis.page-size")).isEqualTo("50");
    }

    @Test
    void findsANonHyphenatedSettingFromAnEnvironmentVariableName() {
        assertThat(context(Map.of("CMIS_URL", "http://repo:8080")).property("cmis.url"))
                .isEqualTo("http://repo:8080");
    }

    /** The declared form wins, so a deployment that sets both gets the more specific one. */
    @Test
    void prefersTheDeclaredFormOverTheEnvironmentVariableForm() {
        Map<String, Object> properties = new LinkedHashMap<>();
        properties.put("cmis.page-size", "50");
        properties.put("CMIS_PAGE_SIZE", "200");

        assertThat(context(properties).property("cmis.page-size")).isEqualTo("50");
    }

    @Test
    void returnsNullForAnUnsetSetting() {
        assertThat(context(Map.of()).property("cmis.url")).isNull();
    }

    /**
     * A blank value comes back as-is rather than as null: it is the schema that decides whether blank
     * counts as missing, and it does.
     */
    @Test
    void returnsABlankValueUnchanged() {
        assertThat(context(Map.of("cmis.url", "")).property("cmis.url")).isEmpty();
    }

    /** The typed helpers are SPI defaults over this one method, so they inherit the fallback. */
    @Test
    void theTypedHelpersSeeTheEnvironmentVariableFormToo() {
        ConnectorContext context = context(Map.of(
                "CMIS_PAGE_SIZE", "50",
                "CMIS_INCLUDE_TYPES", "cmis:document, cmis:folder",
                "CMIS_FAIL_CLOSED", "true"));

        assertThat(context.intProperty("cmis.page-size", 100)).isEqualTo(50);
        assertThat(context.listProperty("cmis.include-types")).containsExactly("cmis:document", "cmis:folder");
        assertThat(context.booleanProperty("cmis.fail-closed", false)).isTrue();
    }

    private static ConnectorContext context(Map<String, Object> properties) {
        StandardEnvironment environment = new StandardEnvironment();
        environment.getPropertySources().addFirst(new MapPropertySource("test", properties));
        return new EnvironmentConnectorContext(environment);
    }
}
