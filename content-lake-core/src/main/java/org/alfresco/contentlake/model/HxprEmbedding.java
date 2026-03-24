package org.alfresco.contentlake.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.util.List;

/**
 * Model for embedding data stored inline in parent documents.
 *
 * Embeddings are stored as an array in the parent document's sysembed_embeddings field.
 * The parent document must have the SysEmbed mixin.
 *
 * Structure in HXPR:
 * - Parent document has mixin: SysEmbed
 * - Embeddings stored in field: sysembed_embeddings (array)
 * - Each embedding object contains: type, text, vector, location
 */
@Data
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class HxprEmbedding {

    /**
     * The embedding model/type identifier (e.g., "text-embedding-nomic-embed-text-v1.5").
     */
    @JsonProperty("type")
    private String type;

    /**
     * The text chunk that was embedded.
     */
    @JsonProperty("text")
    private String text;

    /**
     * The embedding vector as a list of doubles.
     */
    @JsonProperty("vector")
    private List<Double> vector;

    /**
     * Location metadata for where this chunk came from in the source document.
     */
    @JsonProperty("location")
    private EmbeddingLocation location;

    /**
     * Score from vector similarity search (populated by query results).
     */
    @JsonIgnore
    private Double score;

    /**
     * Internal chunk ID for tracking (not sent to API).
     */
    @JsonIgnore
    private String chunkId;

    /**
     * Location metadata for the embedding within the source document.
     */
    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class EmbeddingLocation {
        @JsonProperty("text")
        private TextLocation text;
        @JsonProperty("position")
        private PositionLocation position;
        @JsonProperty("timestamp")
        private TimestampLocation timestamp;
        @JsonProperty("spreadsheet")
        private SpreadsheetLocation spreadSheet;

        @Data
        @JsonIgnoreProperties(ignoreUnknown = true)
        @JsonInclude(JsonInclude.Include.NON_NULL)
        public static class TextLocation {
            private Integer page;
            private Integer paragraph;
        }

        @Data
        @JsonIgnoreProperties(ignoreUnknown = true)
        @JsonInclude(JsonInclude.Include.NON_NULL)
        public static class PositionLocation {
            private Integer left;
            private Integer top;
            private Integer right;
            private Integer bottom;
        }

        @Data
        @JsonIgnoreProperties(ignoreUnknown = true)
        @JsonInclude(JsonInclude.Include.NON_NULL)
        public static class TimestampLocation {
            private Double start;
            private Double end;
        }

        @Data
        @JsonIgnoreProperties(ignoreUnknown = true)
        @JsonInclude(JsonInclude.Include.NON_NULL)
        public static class SpreadsheetLocation {
            private Integer column;
            private Integer row;
            private String sheet;
        }
    }
}
