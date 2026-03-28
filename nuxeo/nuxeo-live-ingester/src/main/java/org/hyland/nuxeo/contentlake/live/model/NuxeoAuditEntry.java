package org.hyland.nuxeo.contentlake.live.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.OffsetDateTime;

@JsonIgnoreProperties(ignoreUnknown = true)
public record NuxeoAuditEntry(
        @JsonProperty("entity-type") String entityType,
        long id,
        String eventId,
        String repositoryId,
        String docUUID,
        String docPath,
        String docType,
        OffsetDateTime eventDate,
        OffsetDateTime logDate
) {
}
