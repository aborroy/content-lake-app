package org.hyland.contentlake.connector;

import org.hyland.contentlake.spi.ConnectorSchema;
import org.hyland.contentlake.spi.ContentSourceClient;
import org.hyland.contentlake.spi.SourceNode;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.core.env.MapPropertySource;
import org.springframework.core.env.StandardEnvironment;
import org.springframework.core.io.Resource;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Fail-fast validation of connector configuration (#123). The point is that a deployment mistake is
 * stated at startup rather than arriving later as a downstream symptom, so these tests are mostly about
 * what the failure says and when it is allowed not to fail.
 */
class ConnectorConfigurationValidatorTest {

    private final Map<String, Object> properties = new HashMap<>();

    private ConnectorConfigurationValidator validator(String mode, ContentSourceClient... clients) {
        StandardEnvironment environment = new StandardEnvironment();
        environment.getPropertySources().addFirst(new MapPropertySource("test", properties));
        return new ConnectorConfigurationValidator(provider(clients), environment, mode);
    }

    @Test
    void failsStartupNamingTheMissingSetting() {
        ConnectorConfigurationValidator validator = validator("fail", source());

        assertThatThrownBy(validator::afterSingletonsInstantiated)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("src.url")
                .hasMessageContaining("Base URL of the source");
    }

    /** The message has to be actionable on its own: it says how to start anyway. */
    @Test
    void theFailureSaysHowToOverrideIt() {
        ConnectorConfigurationValidator validator = validator("fail", source());

        assertThatThrownBy(validator::afterSingletonsInstantiated)
                .hasMessageContaining("content-lake.connector.validation=warn");
    }

    @Test
    void aSatisfiedConfigurationStartsSilently() {
        properties.put("src.url", "http://repo:8080");
        properties.put("src.password", "hunter2");

        assertThatCode(validator("fail", source())::afterSingletonsInstantiated)
                .doesNotThrowAnyException();
    }

    @Test
    void everyProblemAppearsInTheMessage() {
        properties.put("src.page-size", "many");
        ConnectorConfigurationValidator validator = validator("fail", source());

        assertThatThrownBy(validator::afterSingletonsInstantiated)
                .hasMessageContaining("src.url")
                .hasMessageContaining("src.password")
                .hasMessageContaining("src.page-size");
    }

    /** A credential must not reach a startup message, whatever is wrong with it. */
    @Test
    void aSecretValueIsNotEchoedInTheFailure() {
        properties.put("src.password", "hunter2");
        ConnectorConfigurationValidator validator = validator("fail", source());

        assertThatThrownBy(validator::afterSingletonsInstantiated)
                .hasMessageNotContaining("hunter2");
    }

    /** For a deployment whose mount or endpoint appears after the ingester does. */
    @Test
    void warnModeStartsAnyway() {
        assertThatCode(validator("warn", source())::afterSingletonsInstantiated)
                .doesNotThrowAnyException();
    }

    @Test
    void offModeDoesNotCheckAtAll() {
        assertThatCode(validator("off", source())::afterSingletonsInstantiated)
                .doesNotThrowAnyException();
    }

    /** A typo in the mode must not silently disable the check. */
    @Test
    void anUnknownModeFallsBackToFailing() {
        ConnectorConfigurationValidator validator = validator("lenient", source());

        assertThatThrownBy(validator::afterSingletonsInstantiated)
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void anUnsetModeFailsByDefault() {
        assertThatThrownBy(validator(null, source())::afterSingletonsInstantiated)
                .isInstanceOf(IllegalStateException.class);
    }

    /** The RAG service carries no connector, so there is nothing to validate and nothing to fail. */
    @Test
    void anApplicationWithNoSourceClientValidatesNothing() {
        assertThatCode(validator("fail")::afterSingletonsInstantiated).doesNotThrowAnyException();
    }

    /** An adapter written before this feature publishes an empty schema and is left alone. */
    @Test
    void aSourceWithNoSchemaIsSkipped() {
        assertThatCode(validator("fail", new StubSource("legacy", ConnectorSchema.empty("legacy")))
                ::afterSingletonsInstantiated)
                .doesNotThrowAnyException();
    }

    @Test
    void everyConnectorInTheApplicationIsChecked() {
        properties.put("src.url", "http://repo:8080");
        properties.put("src.password", "hunter2");
        ContentSourceClient second = new StubSource("other", ConnectorSchema.builder("other")
                .required("other.root", ConnectorSchema.FieldType.STRING, "Where to read from")
                .build());

        ConnectorConfigurationValidator validator = validator("fail", source(), second);

        assertThatThrownBy(validator::afterSingletonsInstantiated)
                .hasMessageContaining("other.root");
    }

    @Test
    void validateReportsProblemsWithoutThrowing() {
        assertThat(validator("fail", source()).validate()).hasSize(2);
    }

    private static ContentSourceClient source() {
        return new StubSource("src", ConnectorSchema.builder("src")
                .required("src.url", ConnectorSchema.FieldType.URL, "Base URL of the source")
                .secret("src.password", "Password for the account", true)
                .optional("src.page-size", ConnectorSchema.FieldType.INTEGER, "Entries per page")
                .build());
    }

    /** Only the two methods the validator uses; the rest of the SPI is irrelevant here. */
    private record StubSource(String sourceType, ConnectorSchema schema) implements ContentSourceClient {

        @Override
        public String getSourceId() {
            return sourceType;
        }

        @Override
        public String getSourceType() {
            return sourceType;
        }

        @Override
        public ConnectorSchema connectorSchema() {
            return schema;
        }

        @Override
        public SourceNode getNode(String nodeId) {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<SourceNode> getChildren(String containerId, int skip, int maxItems) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Resource downloadContent(String nodeId, String fileName) {
            throw new UnsupportedOperationException();
        }

        @Override
        public byte[] getContent(String nodeId) {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * The iterable half of {@link ObjectProvider}, which is all the validator uses. A fresh iterator per
     * call, so validating twice sees the same connectors.
     */
    private static ObjectProvider<ContentSourceClient> provider(ContentSourceClient... clients) {
        List<ContentSourceClient> list = Arrays.asList(clients);
        return new ObjectProvider<>() {

            @Override
            public Iterator<ContentSourceClient> iterator() {
                return list.iterator();
            }

            @Override
            public ContentSourceClient getObject() {
                if (list.size() != 1) {
                    throw new UnsupportedOperationException();
                }
                return list.getFirst();
            }

            @Override
            public ContentSourceClient getObject(Object... args) {
                return getObject();
            }

            @Override
            public ContentSourceClient getIfAvailable() {
                return list.isEmpty() ? null : list.getFirst();
            }

            @Override
            public ContentSourceClient getIfUnique() {
                return list.size() == 1 ? list.getFirst() : null;
            }
        };
    }
}
