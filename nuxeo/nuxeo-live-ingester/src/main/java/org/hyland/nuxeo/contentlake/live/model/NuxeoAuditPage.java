package org.hyland.nuxeo.contentlake.live.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.util.List;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class NuxeoAuditPage {

    @JsonProperty("entity-type")
    private String entityType;

    private List<NuxeoAuditEntry> entries = List.of();

    @JsonProperty("isNextPageAvailable")
    private boolean nextPageAvailable;

    public boolean hasMore() {
        return nextPageAvailable;
    }
}
