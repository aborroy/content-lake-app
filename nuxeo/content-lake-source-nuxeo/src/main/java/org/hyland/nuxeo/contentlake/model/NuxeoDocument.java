package org.hyland.nuxeo.contentlake.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Minimal representation of the Nuxeo REST API document payload used by the adapter.
 */
@Slf4j
@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class NuxeoDocument {

    @JsonProperty("entity-type")
    private String entityType;

    private String uid;
    private String path;
    private String type;
    private String title;
    private String state;
    private Map<String, Object> properties = Collections.emptyMap();

    @JsonProperty("facets")
    private List<String> facets = Collections.emptyList();

    @JsonProperty("contextParameters")
    private ContextParameters contextParameters;

    public String getStringProperty(String key) {
        Object value = properties != null ? properties.get(key) : null;
        return value instanceof String stringValue ? stringValue : null;
    }

    @SuppressWarnings("unchecked")
    public Map<String, Object> getBlobProperty(String blobXpath) {
        if (properties == null || blobXpath == null || blobXpath.isBlank()) {
            return Collections.emptyMap();
        }
        Object blob = properties.get(blobXpath);
        if (blob instanceof Map<?, ?> map) {
            return (Map<String, Object>) map;
        }
        return Collections.emptyMap();
    }

    public String getBlobMimeType(String blobXpath) {
        Object mimeType = getBlobProperty(blobXpath).get("mime-type");
        return mimeType instanceof String stringValue ? stringValue : null;
    }

    public OffsetDateTime getModifiedAt() {
        String modifiedAt = getStringProperty("dc:modified");
        if (modifiedAt == null || modifiedAt.isBlank()) {
            return null;
        }
        try {
            return OffsetDateTime.parse(modifiedAt);
        } catch (DateTimeParseException e) {
            log.warn("Could not parse dc:modified value '{}' for document {}; staleness check will be skipped", modifiedAt, uid);
            return null;
        }
    }

    public String getDisplayName() {
        if (title != null && !title.isBlank()) {
            return title;
        }
        String dcTitle = getStringProperty("dc:title");
        if (dcTitle != null && !dcTitle.isBlank()) {
            return dcTitle;
        }
        return getPathLeaf();
    }

    public String getPathLeaf() {
        if (path == null || path.isBlank() || "/".equals(path)) {
            return uid;
        }
        int lastSlash = path.lastIndexOf('/');
        return lastSlash >= 0 && lastSlash < path.length() - 1
                ? path.substring(lastSlash + 1)
                : uid;
    }

    public String getParentPath() {
        if (path == null || path.isBlank() || "/".equals(path)) {
            return "/";
        }
        int lastSlash = path.lastIndexOf('/');
        if (lastSlash <= 0) {
            return "/";
        }
        return path.substring(0, lastSlash);
    }

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class ContextParameters {
        private List<Acl> acls = List.of();
    }

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Acl {
        private String name;
        private List<Ace> aces = List.of();
    }

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Ace {
        private String username;
        private String permission;
        private Boolean granted;
        private String status;
        private Boolean externalUser;
    }

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Page {
        private List<NuxeoDocument> entries = List.of();

        @JsonProperty("isNextPageAvailable")
        private boolean nextPageAvailable;

        public boolean hasMore() {
            return nextPageAvailable;
        }
    }
}
