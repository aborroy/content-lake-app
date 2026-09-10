package org.hyland.contentlake.spi;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * How a plugin reads its settings (#124). The conversions live in the SPI as default methods so every
 * connector parses configuration the same way, instead of each one re-inventing "what does an empty value
 * mean" and getting it subtly different.
 */
class ConnectorContextTest {

    private final Map<String, String> config = new HashMap<>();

    private final ConnectorContext context = config::get;

    @Test
    void readsAPlainValue() {
        config.put("src.url", "http://repo:8080");

        assertThat(context.property("src.url")).isEqualTo("http://repo:8080");
        assertThat(context.property("src.missing")).isNull();
    }

    @Test
    void fallsBackToTheDefaultWhenUnsetOrBlank() {
        config.put("src.blank", "   ");

        assertThat(context.property("src.missing", "fallback")).isEqualTo("fallback");
        assertThat(context.property("src.blank", "fallback")).isEqualTo("fallback");
    }

    @Test
    void readsNumbers() {
        config.put("src.page-size", " 250 ");

        assertThat(context.intProperty("src.page-size", 100)).isEqualTo(250);
        assertThat(context.longProperty("src.page-size", 100L)).isEqualTo(250L);
        assertThat(context.intProperty("src.missing", 100)).isEqualTo(100);
    }

    /** A non-numeric value is the default, not a crash: schema validation is what reports it. */
    @Test
    void aNonNumericValueFallsBackToTheDefault() {
        config.put("src.page-size", "many");

        assertThat(context.intProperty("src.page-size", 100)).isEqualTo(100);
        assertThat(context.longProperty("src.page-size", 100L)).isEqualTo(100L);
    }

    @Test
    void readsBooleans() {
        config.put("src.enabled", "TRUE");
        config.put("src.other", "false");

        assertThat(context.booleanProperty("src.enabled", false)).isTrue();
        assertThat(context.booleanProperty("src.other", true)).isFalse();
    }

    /**
     * An unrecognised value is the default rather than false, so a typo cannot silently turn a feature off.
     * That is the failure mode {@code Boolean.parseBoolean} has and this deliberately does not.
     */
    @Test
    void anUnrecognisedBooleanKeepsTheDefault() {
        config.put("src.enabled", "yes");

        assertThat(context.booleanProperty("src.enabled", true)).isTrue();
        assertThat(context.booleanProperty("src.enabled", false)).isFalse();
    }

    // --- lists, which arrive in two shapes ---

    @Test
    void readsACommaSeparatedList() {
        config.put("src.roots", "/one, /two ,/three");

        assertThat(context.listProperty("src.roots")).containsExactly("/one", "/two", "/three");
    }

    /** A YAML list resolves as indexed properties, which is what an in-tree connector sees too. */
    @Test
    void readsAnIndexedList() {
        config.put("src.roots[0]", "/one");
        config.put("src.roots[1]", "/two");

        assertThat(context.listProperty("src.roots")).containsExactly("/one", "/two");
    }

    @Test
    void anIndexedListStopsAtTheFirstGap() {
        config.put("src.roots[0]", "/one");
        config.put("src.roots[2]", "/three");

        assertThat(context.listProperty("src.roots")).containsExactly("/one");
    }

    @Test
    void blankEntriesAreDropped() {
        config.put("src.roots", "/one,,  ,/two");

        assertThat(context.listProperty("src.roots")).containsExactly("/one", "/two");
    }

    @Test
    void anUnsetListIsEmptyRatherThanNull() {
        assertThat(context.listProperty("src.roots")).isEmpty();
    }

    // --- enums ---

    private enum Mode { NXQL, CHILDREN }

    @Test
    void readsAnEnumIgnoringCase() {
        config.put("src.mode", "children");

        assertThat(context.enumProperty("src.mode", Mode.class, Mode.NXQL)).isEqualTo(Mode.CHILDREN);
    }

    @Test
    void anUnknownEnumValueKeepsTheDefault() {
        config.put("src.mode", "sideways");

        assertThat(context.enumProperty("src.mode", Mode.class, Mode.NXQL)).isEqualTo(Mode.NXQL);
        assertThat(context.enumProperty("src.missing", Mode.class, Mode.NXQL)).isEqualTo(Mode.NXQL);
    }

    /** The values a connector reads are the names its schema declares; nothing else is available. */
    @Test
    void theContextExposesNothingBeyondProperties() {
        assertThat(ConnectorContext.class.getMethods())
                .extracting(java.lang.reflect.Method::getName)
                .containsOnly("property", "intProperty", "longProperty", "booleanProperty",
                        "listProperty", "enumProperty");
        assertThat(List.of(ConnectorContext.class.getMethods()))
                .filteredOn(method -> !method.isDefault())
                .hasSize(1);
    }
}
