package org.hyland.contentlake.connector.sharepoint;

import org.hyland.contentlake.connector.sharepoint.mock.MockGraphServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Resolving a site to its document libraries, and above all what that must <em>not</em> cost.
 */
class SharePointDriveCatalogTest {

    private static final String DRIVE = "b!mock-drive-id";

    private MockGraphServer mock;

    @AfterEach
    void stopMock() {
        if (mock != null) {
            mock.close();
        }
    }

    private static MockGraphServer.Options options() {
        return MockGraphServer.Options.defaults(Path.of("src/test/resources/fixtures"));
    }

    private SharePointConnectorSettings.Builder settings() {
        return SharePointConnectorSettings.builder()
                .graphBaseUrl(mock.graphBaseUrl())
                .authMode(SharePointConnectorSettings.AuthMode.STATIC_TOKEN)
                .accessToken("mock-token")
                .resourceUnitsPerMinute(0)
                .resourceUnitBurst(1);
    }

    private SharePointDriveCatalog catalog(SharePointConnectorSettings settings) {
        return new SharePointDriveCatalog(settings,
                new GraphHttpClient(mock.graphBaseUrl(), settings.tokenProvider(),
                        ResourceUnitMeter.unmetered()));
    }

    @Test
    void explicitDriveIdsCostNoGraphCallAtAll() throws Exception {
        // The back-compatibility assertion, and the reason every existing deployment and the whole mock-based
        // suite behave exactly as before: naming drives directly must not start talking to /sites.
        mock = new MockGraphServer(options());
        SharePointDriveCatalog catalog = catalog(settings().driveIds(List.of(DRIVE)).build());
        int before = mock.requestCount();

        assertThat(catalog.driveIds()).containsExactly(DRIVE);
        assertThat(mock.requestCount()).isEqualTo(before);
        assertThat(catalog.resolvesFromConfiguration()).isTrue();
    }

    @Test
    void aSiteUrlIsResolvedToItsLibraries() throws Exception {
        mock = new MockGraphServer(options());
        SharePointDriveCatalog catalog =
                catalog(settings().siteUrl("https://contoso.sharepoint.com/sites/lake").build());

        assertThat(catalog.driveIds()).containsExactly(DRIVE);
        // Two calls: the site by URL, then its drives.
        assertThat(mock.requestLog()).anyMatch(entry -> entry.contains("/sites/contoso.sharepoint.com:/"));
        assertThat(mock.requestLog()).anyMatch(entry -> entry.contains("/drives"));
    }

    @Test
    void aSiteIdSkipsTheUrlLookup() throws Exception {
        mock = new MockGraphServer(options());
        SharePointDriveCatalog catalog =
                catalog(settings().siteId("contoso.sharepoint.com,site-guid,web-guid").build());

        assertThat(catalog.driveIds()).containsExactly(DRIVE);
        // One call, not two: the caller already had the id.
        assertThat(mock.requestLog()).noneMatch(entry -> entry.contains(":/"));
    }

    @Test
    void theResolutionIsMemoisedBecauseTheHostAsksOnEveryPass() throws Exception {
        // Roots are resolved per pass since the selection work, so an unmemoised lookup would spend the
        // tenant's budget on the same two calls forever.
        mock = new MockGraphServer(options());
        SharePointDriveCatalog catalog =
                catalog(settings().siteUrl("https://contoso.sharepoint.com/sites/lake").build());

        catalog.driveIds();
        int afterFirst = mock.requestCount();
        catalog.driveIds();
        catalog.driveIds();

        assertThat(mock.requestCount()).isEqualTo(afterFirst);
    }

    @Test
    void namesTheSettingsWhenNothingSaysWhatToIngest() throws Exception {
        mock = new MockGraphServer(options());
        SharePointDriveCatalog catalog = catalog(settings().build());

        assertThatThrownBy(catalog::driveIds)
                .isInstanceOf(GraphException.class)
                .hasMessageContaining("sharepoint.drive-ids")
                .hasMessageContaining("sharepoint.site-url")
                .hasMessageContaining("sharepoint.site-id");
    }

    @Test
    void refusesASiteUrlWithNoHost() throws Exception {
        mock = new MockGraphServer(options());
        SharePointDriveCatalog catalog = catalog(settings().siteUrl("not-a-url").build());

        assertThatThrownBy(catalog::driveIds)
                .isInstanceOf(GraphException.class)
                .hasMessageContaining("sharepoint.site-url")
                .hasMessageContaining("no host");
    }

    @Test
    void namesTheSiteAndTheCallWhenGraphRefuses() throws Exception {
        // A 404 from /drives is a configuration problem, and the message has to name the site and the call it
        // tried rather than surfacing a bare status. Provoked by pointing the mock at a directory with no
        // drives fixture, since it serves drives.json for any site id it is given.
        mock = new MockGraphServer(MockGraphServer.Options.defaults(Path.of("src/test/resources")));
        SharePointDriveCatalog catalog = catalog(settings()
                .graphBaseUrl(mock.graphBaseUrl())
                .siteId("contoso.sharepoint.com,site-guid,web-guid")
                .build());

        assertThatThrownBy(catalog::driveIds)
                .isInstanceOf(GraphException.class)
                .hasMessageContaining("document libraries")
                .hasMessageContaining("site-guid");
    }

    @Test
    void theSourceAliasDoesNotDependOnAResolvedDriveList() {
        // The sync derives the qualified source id before anything else, so this must be pure. And deriving it
        // from resolved drives would mean a change in the order Graph returns libraries renames the source,
        // orphaning its cursor and every document already indexed under the old name.
        SharePointConnectorSettings bySite = SharePointConnectorSettings.builder()
                .siteUrl("https://contoso.sharepoint.com/sites/lake")
                .build();

        assertThat(bySite.effectiveSourceId()).isEqualTo("contoso-sharepoint-com-sites-lake");

        SharePointConnectorSettings explicit = SharePointConnectorSettings.builder()
                .siteUrl("https://contoso.sharepoint.com/sites/lake")
                .sourceId("the-demo-site")
                .build();

        assertThat(explicit.effectiveSourceId()).isEqualTo("the-demo-site");
    }

    @Test
    void aConfiguredDriveStillWinsOverASite() throws Exception {
        // An operator who named specific libraries meant it.
        mock = new MockGraphServer(options());
        SharePointDriveCatalog catalog = catalog(settings()
                .driveIds(List.of("b!explicit"))
                .siteUrl("https://contoso.sharepoint.com/sites/lake")
                .build());

        assertThat(catalog.driveIds()).containsExactly("b!explicit");
        assertThat(mock.requestCount()).isZero();
    }
}
