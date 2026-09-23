package org.hyland.contentlake.pluginhost.batch.controller;

import org.hyland.contentlake.connector.DefaultScopeResolver;
import org.hyland.contentlake.pluginhost.batch.config.SelectedConnector;
import org.hyland.contentlake.pluginhost.batch.model.IngestionJob;
import org.hyland.contentlake.pluginhost.batch.service.ConnectorBatchIngestionService;
import org.hyland.contentlake.spi.ContentSourceClient;
import org.hyland.contentlake.spi.SourceAuthState;
import org.hyland.contentlake.spi.SourceNode;
import org.hyland.contentlake.spi.TextExtractor;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.core.io.Resource;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * What the status endpoint says about the connector's credential.
 *
 * <p>Exercised directly rather than through MockMvc: none of this involves the servlet stack, and
 * authentication is the host's default-deny rule covered by {@code ConnectorBatchSecurityConfigTest}.</p>
 */
class BatchStatusControllerTest {

    @Test
    void reportsNothingForAConnectorWithNoAuthStateToShow() {
        // The default for every source whose credential is deployment configuration: it worked, or the
        // container failed to start. A screen has to render its absence cleanly rather than treat it as a gap.
        BatchStatusController.BatchStatus status = controller(new StubSource(null)).status();

        assertThat(status.auth()).isNull();
        assertThat(status.state()).isEqualTo("IDLE");
    }

    @Test
    void passesTheConnectorsAuthStateThroughUnchanged() {
        SourceAuthState reported = SourceAuthState.signedInAs(
                "device-code", "crawler@example.invalid", OffsetDateTime.parse("2026-09-23T08:00:00Z"), false);

        BatchStatusController.BatchStatus status = controller(new StubSource(reported)).status();

        assertThat(status.auth()).isEqualTo(reported);
        // False matters: a development shortcut reaching production unremarked is the thing this flag prevents.
        assertThat(status.auth().supportedInProduction()).isFalse();
    }

    /**
     * A connector is a third-party jar, and this is the endpoint an operator uses to work out what is wrong.
     *
     * <p>The SPI says answering must be cheap and must not throw, but a contract is not an enforcement
     * mechanism. A connector that breaks it must not be able to take down the status response as well, because
     * that removes the one thing left that was working.</p>
     */
    @Test
    void degradesToNoAuthStateWhenTheConnectorThrowsInsteadOfFailingTheWholeResponse() {
        BatchStatusController.BatchStatus status = controller(new ThrowingSource()).status();

        assertThat(status.auth()).isNull();
        assertThat(status.sourceType()).isEqualTo("sample");
        assertThat(status.state()).isEqualTo("IDLE");
    }

    private static BatchStatusController controller(ContentSourceClient client) {
        SelectedConnector selected = new SelectedConnector("sample", "sample connector", "sample.jar",
                client, new DefaultScopeResolver(), Mockito.mock(TextExtractor.class), false);
        ConnectorBatchIngestionService service = Mockito.mock(ConnectorBatchIngestionService.class);
        Mockito.when(service.getAllJobs()).thenReturn(Map.<String, IngestionJob>of());
        return new BatchStatusController(service, selected);
    }

    /** Answers a fixed auth state and nothing else; the status endpoint reads no other method. */
    private static class StubSource implements ContentSourceClient {

        private final SourceAuthState state;

        StubSource(SourceAuthState state) {
            this.state = state;
        }

        @Override
        public SourceAuthState authState() {
            return state;
        }

        @Override
        public String getSourceId() {
            return "instance-1";
        }

        @Override
        public String getSourceType() {
            return "sample";
        }

        @Override
        public SourceNode getNode(String nodeId) {
            return null;
        }

        @Override
        public List<SourceNode> getChildren(String containerId, int skip, int maxItems) {
            return List.of();
        }

        @Override
        public Resource downloadContent(String nodeId, String fileName) {
            return null;
        }

        @Override
        public byte[] getContent(String nodeId) {
            return new byte[0];
        }
    }

    /** A connector that breaks the SPI's "must not throw" contract, which is the case worth a test. */
    private static final class ThrowingSource extends StubSource {

        ThrowingSource() {
            super(null);
        }

        @Override
        public SourceAuthState authState() {
            throw new IllegalStateException("the token cache is on fire");
        }
    }
}
