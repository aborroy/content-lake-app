package org.hyland.connector.contentlake.batch.config;

import org.hyland.contentlake.connector.ConnectorRegistry;
import org.hyland.contentlake.connector.DefaultScopeResolver;
import org.hyland.contentlake.connector.LoadedConnector;
import org.hyland.contentlake.spi.ConnectorSchema;
import org.hyland.contentlake.spi.ContentSourceClient;
import org.hyland.contentlake.spi.ScopeResolver;
import org.hyland.contentlake.spi.SourceNode;
import org.hyland.contentlake.spi.TextExtractor;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.Resource;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Which connector this host runs, and what it fills in when the connector supplies only a client (#132).
 */
class SelectedConnectorTest {

    private static final String PLUGIN_DIR = "/opt/content-lake/connectors";

    private final AtomicInteger hostExtractorBuilds = new AtomicInteger();

    @Test
    void takesTheOnlyConnectorWhenNoneIsNamed() {
        ConnectorRegistry registry = registry(connector("cmis", null, null));

        SelectedConnector selected = select(registry, null);

        assertThat(selected.sourceType()).isEqualTo("cmis");
        assertThat(selected.client().getSourceType()).isEqualTo("cmis");
    }

    @Test
    void takesTheNamedConnectorWhenSeveralAreLoaded() {
        ConnectorRegistry registry = registry(
                connector("cmis", null, null),
                connector("sharepoint", null, null));

        assertThat(select(registry, "sharepoint").sourceType()).isEqualTo("sharepoint");
    }

    /** Trimmed, because a source type that came from an environment variable often carries whitespace. */
    @Test
    void trimsTheNamedSourceType() {
        ConnectorRegistry registry = registry(connector("cmis", null, null));

        assertThat(select(registry, "  cmis  ").sourceType()).isEqualTo("cmis");
    }

    /**
     * An empty registry fails startup. This service has no source of its own, so it would otherwise run
     * as a sync API that reports zero documents on every call.
     */
    @Test
    void failsWhenNoConnectorIsLoaded() {
        assertThatThrownBy(() -> select(ConnectorRegistry.empty(), null))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("No connector plugin was loaded")
                .hasMessageContaining(PLUGIN_DIR);
    }

    /**
     * "Nothing loaded" and "a jar was refused" look identical from outside and have entirely different
     * fixes, so the loader's problems are part of the failure.
     */
    @Test
    void reportsLoadProblemsAlongsideAnEmptyRegistry() {
        ConnectorRegistry registry = new ConnectorRegistry(List.of(),
                List.of("Connector plugin 'cmis' from cmis.jar failed to build: BindException"));

        assertThatThrownBy(() -> select(registry, null))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("cmis.jar failed to build");
    }

    /** Picking one of several would make the ingested corpus depend on jar file names. */
    @Test
    void failsWhenSeveralAreLoadedAndNoneIsNamed() {
        ConnectorRegistry registry = registry(
                connector("cmis", null, null),
                connector("sharepoint", null, null));

        assertThatThrownBy(() -> select(registry, null))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("connector.source-type")
                .hasMessageContaining("cmis")
                .hasMessageContaining("sharepoint");
    }

    @Test
    void failsWhenTheNamedConnectorIsNotLoaded() {
        ConnectorRegistry registry = registry(connector("cmis", null, null));

        assertThatThrownBy(() -> select(registry, "sharepoint"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("'sharepoint'")
                .hasMessageContaining("[cmis]");
    }

    @Test
    void fillsInTheHostScopeResolverWhenTheConnectorSuppliesNone() {
        SelectedConnector selected = select(registry(connector("cmis", null, null)), null);

        assertThat(selected.scopeResolver()).isInstanceOf(DefaultScopeResolver.class);
    }

    @Test
    void keepsTheConnectorScopeResolverWhenItSuppliesOne() {
        ScopeResolver own = new ScopeResolver() {
            @Override
            public boolean isInScope(SourceNode node) {
                return false;
            }

            @Override
            public boolean shouldTraverse(SourceNode node) {
                return false;
            }
        };

        SelectedConnector selected = select(registry(connector("cmis", own, null)), null);

        assertThat(selected.scopeResolver()).isSameAs(own);
    }

    @Test
    void fillsInTheHostExtractionChainWhenTheConnectorSuppliesNone() {
        SelectedConnector selected = select(registry(connector("cmis", null, null)), null);

        assertThat(selected.ownExtractor()).isFalse();
        assertThat(selected.textExtractor()).isNotNull();
        assertThat(hostExtractorBuilds).hasValue(1);
    }

    /**
     * A connector that converts content at the source does not pay for a chain it will not call, so the
     * host's chain is never even constructed.
     */
    @Test
    void keepsTheConnectorExtractorAndDoesNotBuildTheHostChain() {
        TextExtractor own = new TextExtractor() {
            @Override
            public boolean supports(String mimeType) {
                return true;
            }

            @Override
            public String extractText(Resource content, String mimeType) {
                return "from the connector";
            }
        };

        SelectedConnector selected = select(registry(connector("cmis", null, own)), null);

        assertThat(selected.ownExtractor()).isTrue();
        assertThat(selected.textExtractor()).isSameAs(own);
        assertThat(hostExtractorBuilds).hasValue(0);
    }

    private SelectedConnector select(ConnectorRegistry registry, String requestedType) {
        return SelectedConnector.from(registry, requestedType, PLUGIN_DIR, () -> {
            hostExtractorBuilds.incrementAndGet();
            return new TextExtractor() {
                @Override
                public boolean supports(String mimeType) {
                    return true;
                }

                @Override
                public String extractText(Resource content, String mimeType) {
                    return "from the host chain";
                }
            };
        });
    }

    private static ConnectorRegistry registry(LoadedConnector... connectors) {
        return new ConnectorRegistry(List.of(connectors), List.of());
    }

    private static LoadedConnector connector(String sourceType,
                                             ScopeResolver scopeResolver,
                                             TextExtractor textExtractor) {
        return new LoadedConnector(sourceType, sourceType + " connector", sourceType + ".jar",
                ConnectorSchema.empty(sourceType), new StubClient(sourceType), scopeResolver, textExtractor);
    }

    private record StubClient(String sourceType) implements ContentSourceClient {

        @Override
        public String getSourceId() {
            return sourceType + "-instance";
        }

        @Override
        public String getSourceType() {
            return sourceType;
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
}
