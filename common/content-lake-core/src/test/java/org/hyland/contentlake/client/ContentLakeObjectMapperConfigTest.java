package org.hyland.contentlake.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.time.OffsetDateTime;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The shared Jackson 2 bean must handle {@code java.time}. Boot's own mapper always did; this one
 * replaces it (Boot 4 autoconfigures only the Jackson 3 mapper), and a replacement without
 * {@code JavaTimeModule} throws on the first {@code OffsetDateTime} anything asks it to write, which
 * is how the Nuxeo audit cursor stopped being persisted (#126).
 */
class ContentLakeObjectMapperConfigTest {

    private final ObjectMapper mapper = new ContentLakeObjectMapperConfig().contentLakeObjectMapper();

    @Test
    void writesAndReadsAnOffsetDateTime() throws Exception {
        OffsetDateTime timestamp = OffsetDateTime.parse("2026-03-26T16:48:41.235Z");

        String json = mapper.writeValueAsString(new Holder(timestamp));

        assertThat(json).contains("2026-03-26T16:48:41.235Z");
        assertThat(mapper.readValue(json, Holder.class).timestamp()).isEqualTo(timestamp);
    }

    record Holder(OffsetDateTime timestamp) {
    }
}
