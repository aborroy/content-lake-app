package org.hyland.contentlake.rag.service;

import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The configured source ids, now keyed by type rather than one field per known source.
 *
 * <p>There was no test for this record. It decides which sources a permission filter covers and how each is
 * typed, so the reshape needed one.</p>
 */
class PermissionSourceCatalogConfiguredTest {

    @Test
    void keepsTheOrderTheTwoInTreeSourcesHaveAlwaysBeenCheckedIn() {
        PermissionSourceCatalog.Configured configured =
                PermissionSourceCatalog.Configured.of("test-repo", "nuxeo-demo", null);

        // Alfresco before Nuxeo. A source id configured for two types would otherwise be typed by whichever
        // the map happened to yield first.
        assertThat(configured.configuredIds()).containsExactly("test-repo", "nuxeo-demo");
    }

    @Test
    void omitsATypeThatIsNotConfigured() {
        PermissionSourceCatalog.Configured configured =
                PermissionSourceCatalog.Configured.of("test-repo", "  ", null);

        assertThat(configured.configuredIds()).containsExactly("test-repo");
        assertThat(configured.sourceIdOf("nuxeo")).isNull();
    }

    @Test
    void resolvesASourceIdByType() {
        PermissionSourceCatalog.Configured configured =
                PermissionSourceCatalog.Configured.of("test-repo", "nuxeo-demo", null);

        assertThat(configured.sourceIdOf("alfresco")).isEqualTo("test-repo");
        assertThat(configured.sourceIdOf("nuxeo")).isEqualTo("nuxeo-demo");
        assertThat(configured.sourceIdOf("NUXEO")).isEqualTo("nuxeo-demo");
        assertThat(configured.sourceIdOf("sharepoint")).isNull();
        assertThat(configured.sourceIdOf(null)).isNull();
    }

    @Test
    void carriesATypeTheServicesWereNotCompiledAgainst() {
        // The point of the reshape: a fourth source type needs no change to this record.
        Map<String, String> byType = new LinkedHashMap<>();
        byType.put("sharepoint", "site-1");
        byType.put("cmis", "docbase-1");

        PermissionSourceCatalog.Configured configured = new PermissionSourceCatalog.Configured(byType, null);

        assertThat(configured.sourceIdOf("sharepoint")).isEqualTo("site-1");
        assertThat(configured.sourceIdOf("cmis")).isEqualTo("docbase-1");
        assertThat(configured.configuredIds()).containsExactly("site-1", "docbase-1");
    }

    @Test
    void reportsWhetherAnOperatorPinnedTheSet() {
        assertThat(PermissionSourceCatalog.Configured.of("a", "b", null).hasPin()).isFalse();
        assertThat(PermissionSourceCatalog.Configured.of("a", "b", "  ").hasPin()).isFalse();
        assertThat(PermissionSourceCatalog.Configured.of("a", "b", "x,y").hasPin()).isTrue();
    }

    @Test
    void isDefensiveAboutTheMapItWasGiven() {
        Map<String, String> mutable = new LinkedHashMap<>();
        mutable.put("alfresco", "test-repo");
        PermissionSourceCatalog.Configured configured =
                new PermissionSourceCatalog.Configured(mutable, null);

        mutable.put("nuxeo", "added-later");

        // A caller mutating its own map must not change what sources a permission filter covers.
        assertThat(configured.sourceIdOf("nuxeo")).isNull();
        assertThat(configured.configuredIds()).containsExactly("test-repo");
    }

    @Test
    void toleratesNoConfiguredSourceAtAll() {
        PermissionSourceCatalog.Configured configured =
                new PermissionSourceCatalog.Configured(null, null);

        assertThat(configured.configuredIds()).isEmpty();
        assertThat(configured.sourceIdOf("alfresco")).isNull();
        assertThat(configured.hasPin()).isFalse();
    }
}
