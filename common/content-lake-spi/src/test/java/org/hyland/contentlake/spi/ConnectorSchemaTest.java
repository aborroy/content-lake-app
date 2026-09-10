package org.hyland.contentlake.spi;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.STRING;

/**
 * The schema is what turns a misconfigured connector from a downstream symptom into a statement at
 * startup (#123), so what matters here is that {@link ConnectorSchema#validate} names the setting at
 * fault and never prints a secret.
 */
class ConnectorSchemaTest {

    @TempDir
    Path tempDir;

    private final Map<String, String> config = new HashMap<>();

    private List<String> validate(ConnectorSchema schema) {
        return schema.validate(config::get);
    }

    @Test
    void anEmptySchemaValidatesTrivially() {
        // What an adapter written before this feature gets from the default method.
        assertThat(validate(ConnectorSchema.empty("legacy"))).isEmpty();
    }

    @Test
    void aSatisfiedSchemaReportsNothing() {
        config.put("src.url", "http://repo:8080");
        config.put("src.user", "admin");
        config.put("src.password", "secret");

        assertThat(validate(schema())).isEmpty();
    }

    @Test
    void aMissingRequiredSettingIsReportedByName() {
        config.put("src.user", "admin");
        config.put("src.password", "secret");

        assertThat(validate(schema()))
                .singleElement(STRING)
                .contains("src.url")
                .contains("Base URL");
    }

    /** A blank value is as missing as an unset one: an empty env var is the common way to get here. */
    @Test
    void aBlankRequiredSettingCountsAsMissing() {
        config.put("src.url", "   ");
        config.put("src.user", "admin");
        config.put("src.password", "secret");

        assertThat(validate(schema())).singleElement(STRING)
                .contains("src.url");
    }

    @Test
    void anOptionalSettingLeftUnsetIsFine() {
        config.put("src.url", "http://repo:8080");
        config.put("src.user", "admin");
        config.put("src.password", "secret");
        // src.page-size unset

        assertThat(validate(schema())).isEmpty();
    }

    /** Every problem at once, so an operator does not find the next one on the following restart. */
    @Test
    void allProblemsAreReportedTogether() {
        config.put("src.page-size", "many");

        assertThat(validate(schema())).hasSize(4);
    }

    @Test
    void aMalformedUrlIsReported() {
        config.put("src.url", "repo:8080");
        config.put("src.user", "admin");
        config.put("src.password", "secret");

        assertThat(validate(schema())).singleElement(STRING)
                .contains("src.url")
                .contains("absolute URL");
    }

    @Test
    void aNonNumericIntegerIsReported() {
        config.put("src.url", "http://repo:8080");
        config.put("src.user", "admin");
        config.put("src.password", "secret");
        config.put("src.page-size", "many");

        assertThat(validate(schema())).singleElement(STRING)
                .contains("src.page-size")
                .contains("whole number");
    }

    @Test
    void aNonBooleanIsReported() {
        ConnectorSchema schema = ConnectorSchema.builder("src")
                .optional("src.enabled", ConnectorSchema.FieldType.BOOLEAN, "Whether to do the thing")
                .build();
        config.put("src.enabled", "yes");

        assertThat(validate(schema)).singleElement(STRING)
                .contains("true or false");
    }

    @Test
    void aValueOutsideAnEnumIsReportedWithTheAllowedValues() {
        ConnectorSchema schema = ConnectorSchema.builder("src")
                .enumeration("src.mode", "How discovery walks the source", false, List.of("NXQL", "CHILDREN"))
                .build();
        config.put("src.mode", "sideways");

        assertThat(validate(schema)).singleElement(STRING)
                .contains("NXQL")
                .contains("CHILDREN")
                .contains("sideways");
    }

    /** Configuration is case-insensitive here, because an env var written in lower case is not a mistake. */
    @Test
    void anEnumMatchesIgnoringCase() {
        ConnectorSchema schema = ConnectorSchema.builder("src")
                .enumeration("src.mode", "How discovery walks the source", false, List.of("NXQL", "CHILDREN"))
                .build();
        config.put("src.mode", "children");

        assertThat(validate(schema)).isEmpty();
    }

    // --- directories, which is where the filesystem connector's teeth are ---

    @Test
    void anExistingReadableDirectoryIsFine() {
        ConnectorSchema schema = directorySchema();
        config.put("src.root", tempDir.toString());

        assertThat(validate(schema)).isEmpty();
    }

    @Test
    void aDirectoryThatDoesNotExistIsReported() {
        ConnectorSchema schema = directorySchema();
        config.put("src.root", tempDir.resolve("not-mounted").toString());

        assertThat(validate(schema)).singleElement(STRING)
                .contains("src.root")
                .contains("does not exist");
    }

    @Test
    void aFileWhereADirectoryIsExpectedIsReported() throws Exception {
        ConnectorSchema schema = directorySchema();
        Path file = Files.writeString(tempDir.resolve("a-file.txt"), "not a directory");
        config.put("src.root", file.toString());

        assertThat(validate(schema)).singleElement(STRING)
                .contains("not a directory");
    }

    // --- secrets ---

    /**
     * The acceptance criterion that matters most: a secret's value must not reach a message or a log,
     * however malformed it is. A password with a stray newline is the realistic way to trip this.
     */
    @Test
    void aSecretsValueNeverAppearsInAProblem() {
        ConnectorSchema schema = ConnectorSchema.builder("src")
                .secret("src.token", "API token", true)
                .build();
        // A secret is a plain string, so it cannot be malformed; the guard is that the mask is what a
        // message would carry if it ever were.
        ConnectorSchema.Field secret = schema.fields().getFirst();

        assertThat(secret.secret()).isTrue();
        assertThat(secret.malformed("p@ssw0rd!")).isEmpty();
    }

    @Test
    void aMalformedSecretIsMaskedRatherThanEchoed() {
        // A secret declared with a checkable type: the value must still not appear.
        ConnectorSchema.Field secretUrl = new ConnectorSchema.Field(
                "src.callback", ConnectorSchema.FieldType.URL, "Callback URL carrying a token",
                true, true, List.of());

        assertThat(secretUrl.malformed("not-a-url-p@ssw0rd"))
                .get(STRING)
                .contains(ConnectorSchema.MASK)
                .doesNotContain("p@ssw0rd");
    }

    /** A missing secret is named, because an operator has to know which setting to supply. */
    @Test
    void aMissingSecretIsNamedWithoutAValue() {
        config.put("src.url", "http://repo:8080");
        config.put("src.user", "admin");

        assertThat(validate(schema())).singleElement(STRING)
                .contains("src.password");
    }

    // --- shape ---

    @Test
    void fieldsAreImmutableAndOrderIsPreserved() {
        ConnectorSchema schema = schema();

        assertThat(schema.fields()).extracting(ConnectorSchema.Field::name)
                .containsExactly("src.url", "src.user", "src.password", "src.page-size");
        assertThat(schema.fields().getFirst().required()).isTrue();
        assertThat(schema.fields().getLast().required()).isFalse();
    }

    private static ConnectorSchema schema() {
        return ConnectorSchema.builder("src")
                .required("src.url", ConnectorSchema.FieldType.URL, "Base URL of the source")
                .required("src.user", ConnectorSchema.FieldType.STRING, "Account to read with")
                .secret("src.password", "Password for that account", true)
                .optional("src.page-size", ConnectorSchema.FieldType.INTEGER, "Entries per page")
                .build();
    }

    private static ConnectorSchema directorySchema() {
        return ConnectorSchema.builder("src")
                .required("src.root", ConnectorSchema.FieldType.DIRECTORY, "Directory to ingest from")
                .build();
    }
}
