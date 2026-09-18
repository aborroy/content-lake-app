package org.hyland.contentlake.connector.sharepoint.mock;

import com.fasterxml.jackson.databind.JsonNode;
import org.hyland.contentlake.connector.sharepoint.GraphException;
import org.hyland.contentlake.connector.sharepoint.GraphHttpClient;
import org.hyland.contentlake.connector.sharepoint.ResourceUnitMeter;
import org.hyland.contentlake.connector.sharepoint.StaticTokenProvider;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The real {@link GraphHttpClient} against the mock, which is the pairing everything else is built on.
 *
 * <p>Worth having as its own test rather than trusting the two in isolation: the value of the mock is that
 * it is unhelpful in the same places Graph is, and that only shows up when a real client talks to it.</p>
 */
class MockGraphServerTest {

    private static final String DRIVE = "b!mock-drive-id";

    private MockGraphServer mock;

    @AfterEach
    void stopMock() {
        if (mock != null) {
            mock.close();
        }
    }

    private GraphHttpClient clientFor(MockGraphServer.Options options) throws Exception {
        mock = new MockGraphServer(options);
        return new GraphHttpClient(mock.graphBaseUrl(), new StaticTokenProvider("mock-token"),
                ResourceUnitMeter.unmetered());
    }

    private static MockGraphServer.Options options() {
        return MockGraphServer.Options.defaults(Path.of("src/test/resources/fixtures"));
    }

    @Test
    void pagesTheWholeDeltaFeedFollowingNextLinkToDeltaLink() throws Exception {
        GraphHttpClient client = clientFor(options().withPageSize(3));

        List<String> ids = new ArrayList<>();
        String url = "/drives/" + DRIVE + "/root/delta";
        String deltaLink = null;
        int pages = 0;
        while (url != null) {
            GraphHttpClient.GraphResponse response =
                    client.getJson(url, ResourceUnitMeter.DELTA_WITH_TOKEN, List.of());
            pages++;
            for (JsonNode item : GraphHttpClient.array(response.body(), "value")) {
                ids.add(item.get("id").asText());
            }
            url = GraphHttpClient.text(response.body(), "@odata.nextLink");
            deltaLink = GraphHttpClient.text(response.body(), "@odata.deltaLink");
        }

        // 14 fixtures at 3 per page: the feed has to be drained, not sampled.
        assertThat(pages).isEqualTo(5);
        assertThat(ids).hasSize(14).contains("root", "i-quarterly", "i-deep");
        // The last page, and only the last page, carries the cursor to store.
        assertThat(deltaLink).isNotNull().contains("token=delta-");
    }

    @Test
    void seedsACursorWithoutReportingTheCorpusThatAlreadyExists() throws Exception {
        GraphHttpClient client = clientFor(options());

        GraphHttpClient.GraphResponse response = client.getJson(
                "/drives/" + DRIVE + "/root/delta?token=latest", ResourceUnitMeter.DELTA_WITH_TOKEN,
                List.of());

        // This is what makes initialCursor() safe: a feed opened at "now" says nothing about what is
        // already there, so the host walks first and stores this afterwards.
        assertThat(GraphHttpClient.array(response.body(), "value")).isEmpty();
        assertThat(GraphHttpClient.text(response.body(), "@odata.deltaLink")).contains("token=delta-");
    }

    @Test
    void reportsADeletionOnTheNextIncrementalCall() throws Exception {
        GraphHttpClient client = clientFor(options());

        String cursor = GraphHttpClient.text(client.getJson(
                "/drives/" + DRIVE + "/root/delta?token=latest",
                ResourceUnitMeter.DELTA_WITH_TOKEN, List.of()).body(), "@odata.deltaLink");

        GraphHttpClient.GraphResponse changes =
                client.getJson(cursor, ResourceUnitMeter.DELTA_WITH_TOKEN, List.of());

        List<JsonNode> items = GraphHttpClient.array(changes.body(), "value");
        assertThat(items).hasSize(1);
        assertThat(items.get(0).get("id").asText()).isEqualTo("i-removed");
        // The deleted facet is the only signal, and it is what has to become a SourceTombstone.
        assertThat(items.get(0).get("deleted").get("state").asText()).isEqualTo("deleted");
    }

    @Test
    void answersAnUnknownDeltaTokenWithGoneRatherThanAnEmptyPage() throws Exception {
        GraphHttpClient client = clientFor(options());

        GraphHttpClient.GraphResponse response = client.getJson(
                "/drives/" + DRIVE + "/root/delta?token=delta-from-a-previous-run",
                ResourceUnitMeter.DELTA_WITH_TOKEN, List.of());

        // An empty page here would be indistinguishable from "nothing changed", and a host that treated
        // its empty deleted list as authoritative would delete nothing while believing it had checked.
        assertThat(response.body().get("error").get("code").asText())
                .isEqualTo("resyncChangesApplyDifferences");
    }

    @Test
    void ignoresDollarSkipOnChildrenExactlyAsGraphDoes() throws Exception {
        GraphHttpClient client = clientFor(options());

        GraphHttpClient.GraphResponse first = client.getJson(
                "/drives/" + DRIVE + "/items/f-public/children?$top=2",
                ResourceUnitMeter.MULTI_ITEM_QUERY, List.of());
        GraphHttpClient.GraphResponse skipped = client.getJson(
                "/drives/" + DRIVE + "/items/f-public/children?$top=2&$skip=2",
                ResourceUnitMeter.MULTI_ITEM_QUERY, List.of());

        List<String> firstIds = idsOf(first);
        // Identical, because $skip does nothing. This is the trap the connector's paging has to survive:
        // a skip-based pager reads page one forever.
        assertThat(idsOf(skipped)).isEqualTo(firstIds);
        assertThat(GraphHttpClient.text(first.body(), "@odata.nextLink")).isNotNull();
    }

    @Test
    void pagesChildrenWithAnOpaqueSkipToken() throws Exception {
        GraphHttpClient client = clientFor(options());

        List<String> ids = new ArrayList<>();
        String url = "/drives/" + DRIVE + "/items/root/children?$top=2";
        while (url != null) {
            GraphHttpClient.GraphResponse response =
                    client.getJson(url, ResourceUnitMeter.MULTI_ITEM_QUERY, List.of());
            ids.addAll(idsOf(response));
            url = GraphHttpClient.text(response.body(), "@odata.nextLink");
        }

        assertThat(ids).containsExactly("f-public", "f-user", "f-group", "f-orglink", "f-nested");
    }

    @Test
    void downloadsContentThroughThePreAuthenticatedRedirect() throws Exception {
        GraphHttpClient client = clientFor(options());

        try (InputStream content = client.getStream("/drives/" + DRIVE + "/items/i-quarterly/content",
                ResourceUnitMeter.CONTENT_DOWNLOAD)) {
            assertThat(new String(content.readAllBytes(), StandardCharsets.UTF_8))
                    .contains("pangolin-ledger-quarterly");
        }

        // The mock refuses a signed URL called with an Authorization header, so this passing is the proof
        // that the connector does not forward its bearer token to storage.
        assertThat(mock.requestLog()).anySatisfy(entry -> assertThat(entry).contains("/mock-storage/"));
    }

    @Test
    void servesPermissionPayloadsVerbatimIncludingTheShapesThatMatter() throws Exception {
        GraphHttpClient client = clientFor(options());

        JsonNode group = client.getJson("/drives/" + DRIVE + "/items/i-group/permissions",
                ResourceUnitMeter.PERMISSIONS, List.of()).body();
        JsonNode orgLink = client.getJson("/drives/" + DRIVE + "/items/i-orgwide/permissions",
                ResourceUnitMeter.PERMISSIONS, List.of()).body();
        JsonNode inherited = client.getJson("/drives/" + DRIVE + "/items/i-quarterly/permissions",
                ResourceUnitMeter.PERMISSIONS, List.of()).body();

        assertThat(group.get("value").get(0).get("grantedToV2").get("group").get("id").asText())
                .isEqualTo("group-guid-finance");
        assertThat(orgLink.get("value").get(0).get("link").get("scope").asText())
                .isEqualTo("organization");
        assertThat(orgLink.get("value").get(1).get("link").get("scope").asText())
                .isEqualTo("anonymous");
        // inheritedFrom is what a permission-hierarchy cache keys on, so it has to be present and shaped
        // like Graph's.
        assertThat(inherited.get("value").get(0).get("inheritedFrom").get("id").asText())
                .isEqualTo("root");
    }

    @Test
    void honoursOnlyThePreferencesItWasConfiguredToHonour() throws Exception {
        GraphHttpClient client = clientFor(options().withHonouredPreferences(Set.of("hierarchicalsharing")));

        GraphHttpClient.GraphResponse response = client.getJson("/drives/" + DRIVE + "/root/delta",
                ResourceUnitMeter.DELTA_WITH_TOKEN,
                List.of("hierarchicalsharing", "deltashowremovedasdeleted"));

        assertThat(response.appliedPreferences()).containsExactly("hierarchicalsharing");
        assertThat(response.unappliedPreferences()).containsExactly("deltashowremovedasdeleted");
    }

    @Test
    void canBeConfiguredAsATenantThatHonoursNoPreferenceAtAll() throws Exception {
        GraphHttpClient client = clientFor(options().withHonouredPreferences(Set.of()));

        GraphHttpClient.GraphResponse response = client.getJson("/drives/" + DRIVE + "/root/delta",
                ResourceUnitMeter.DELTA_WITH_TOKEN, List.of("hierarchicalsharing"));

        // The deployment that cannot get Sites.FullControl.All. The connector has to be able to see this
        // and decide, rather than quietly paying five units per document.
        assertThat(response.allPreferencesApplied()).isFalse();
    }

    @Test
    void throttlesOnDemandAndTheClientRecovers() throws Exception {
        GraphHttpClient client = clientFor(options().withThrottleEveryNthRequest(2, 1));

        // Four calls against a server that refuses every second request: all four have to succeed, with
        // the client waiting rather than failing.
        for (int i = 0; i < 4; i++) {
            GraphHttpClient.GraphResponse response = client.getJson("/drives/" + DRIVE + "/items/root",
                    ResourceUnitMeter.SINGLE_ITEM, List.of());
            assertThat(response.body().get("id").asText()).isEqualTo("root");
        }
        assertThat(mock.requestCount()).isGreaterThan(4);
    }

    @Test
    void refusesARequestThatCarriesNoBearerToken() throws Exception {
        mock = new MockGraphServer(options());

        // Called without the client, because the client always decorates. This asserts the mock is
        // unhelpful in the same way Graph is, so a connector that stopped sending the header would fail
        // here rather than in a tenant.
        java.net.http.HttpResponse<String> response = java.net.http.HttpClient.newHttpClient().send(
                java.net.http.HttpRequest.newBuilder(
                        java.net.URI.create(mock.graphBaseUrl() + "/drives/" + DRIVE)).GET().build(),
                java.net.http.HttpResponse.BodyHandlers.ofString());

        assertThat(response.statusCode()).isEqualTo(401);
        assertThat(response.body()).contains("InvalidAuthenticationToken");
    }

    @Test
    void answers404ForAnItemWithNoFixture() throws Exception {
        GraphHttpClient client = clientFor(options());

        assertThatThrownBy(() -> client.getJson("/drives/" + DRIVE + "/items/does-not-exist",
                ResourceUnitMeter.SINGLE_ITEM, List.of()))
                .isInstanceOf(GraphException.class)
                .hasMessageContaining("404")
                .hasMessageContaining("itemNotFound");
    }

    private static List<String> idsOf(GraphHttpClient.GraphResponse response) {
        return GraphHttpClient.array(response.body(), "value").stream()
                .map(node -> node.get("id").asText())
                .toList();
    }
}
