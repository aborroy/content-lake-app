package org.hyland.contentlake.connector.sharepoint;

import org.hyland.contentlake.connector.sharepoint.mock.MockGraphServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Scoping a pass to chosen folders, which is a different thing from filtering one.
 *
 * <p>{@code include-paths} filters after enumeration, so a run configured for one folder of a large library
 * still enumerates the whole library and pays the resource units. {@code folder-paths} starts the walk at the
 * folders instead. These tests are mostly about that distinction being real rather than nominal.</p>
 */
class SharePointFolderScopeTest {

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

    private SharePointConnectorClient clientFor(List<String> folderPaths) throws IOException {
        mock = new MockGraphServer(options());
        SharePointConnectorSettings settings = SharePointConnectorSettings.builder()
                .graphBaseUrl(mock.graphBaseUrl())
                .authMode(SharePointConnectorSettings.AuthMode.STATIC_TOKEN)
                .accessToken("mock-token")
                .driveIds(List.of(DRIVE))
                .folderPaths(folderPaths)
                .resourceUnitsPerMinute(0)
                .resourceUnitBurst(1)
                .build();
        return new SharePointConnectorClient(settings,
                new GraphHttpClient(mock.graphBaseUrl(), settings.tokenProvider(),
                        ResourceUnitMeter.unmetered()));
    }

    @Test
    void startsThePassAtTheChosenFolderRatherThanAtTheDriveRoot() throws Exception {
        SharePointConnectorClient client = clientFor(List.of("/public"));

        // The whole point: the root the host walks from is the folder's item, so nothing outside it is ever
        // enumerated and nothing outside it costs a resource unit.
        assertThat(client.getRootNodeIds()).containsExactly(DRIVE + ":f-public");
    }

    @Test
    void acceptsAPathWithOrWithoutLeadingAndTrailingSlashes() throws Exception {
        assertThat(clientFor(List.of("public")).getRootNodeIds()).containsExactly(DRIVE + ":f-public");
        mock.close();
        assertThat(clientFor(List.of("/public/")).getRootNodeIds()).containsExactly(DRIVE + ":f-public");
    }

    @Test
    void usesTheDriveRootWhenNoFolderIsConfigured() throws Exception {
        SharePointConnectorClient client = clientFor(List.of());

        assertThat(client.getRootNodeIds()).containsExactly(DRIVE + ":root");
    }

    @Test
    void resolvesTheFoldersOnceBecauseTheHostAsksOnEveryPass() throws Exception {
        SharePointConnectorClient client = clientFor(List.of("/public"));

        client.getRootNodeIds();
        int afterFirst = mock.requestCount();
        client.getRootNodeIds();
        client.getRootNodeIds();

        assertThat(mock.requestCount()).isEqualTo(afterFirst);
    }

    @Test
    void refusesWhenNoConfiguredFolderResolvesAnywhere() throws Exception {
        // Falling back to the drive root here would silently widen a deliberately narrow scope to the whole
        // library, which is the opposite of what the operator asked for.
        SharePointConnectorClient client = clientFor(List.of("/no-such-folder"));

        assertThatThrownBy(client::getRootNodeIds)
                .isInstanceOf(GraphException.class)
                .hasMessageContaining("sharepoint.folder-paths")
                .hasMessageContaining("no-such-folder");
    }

    @Test
    void skipsAFolderMissingFromOneDriveWhenAnotherResolved() throws Exception {
        // A site with several libraries will not have the same folder in each, so refusing the whole run over
        // one absent combination would make the setting unusable on a real site.
        mock = new MockGraphServer(options());
        SharePointConnectorSettings settings = SharePointConnectorSettings.builder()
                .graphBaseUrl(mock.graphBaseUrl())
                .authMode(SharePointConnectorSettings.AuthMode.STATIC_TOKEN)
                .accessToken("mock-token")
                .driveIds(List.of(DRIVE))
                .folderPaths(List.of("/public", "/absent"))
                .resourceUnitsPerMinute(0)
                .resourceUnitBurst(1)
                .build();
        SharePointConnectorClient client = new SharePointConnectorClient(settings,
                new GraphHttpClient(mock.graphBaseUrl(), settings.tokenProvider(),
                        ResourceUnitMeter.unmetered()));

        assertThat(client.getRootNodeIds()).containsExactly(DRIVE + ":f-public");
    }

    @Test
    void addressesTheFolderThroughGraphsPathFormRatherThanByGuessingAnItemId() throws Exception {
        SharePointConnectorClient client = clientFor(List.of("/public"));
        client.getRootNodeIds();

        assertThat(mock.requestLog()).anyMatch(entry -> entry.contains("/root:/public"));
    }
}
