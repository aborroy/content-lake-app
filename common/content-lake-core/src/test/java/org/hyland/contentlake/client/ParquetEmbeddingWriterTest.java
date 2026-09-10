package org.hyland.contentlake.client;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.hyland.contentlake.model.HxprEmbedding;
import org.hyland.contentlake.model.HxprEmbedding.EmbeddingLocation;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class ParquetEmbeddingWriterTest {

    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    void serializeLocation_null_returnsNull() throws Exception {
        assertThat(ParquetEmbeddingWriter.serializeLocation(null)).isNull();
    }

    @Test
    void serializeLocation_textOnly_writesParagraph() throws Exception {
        EmbeddingLocation location = new EmbeddingLocation();
        EmbeddingLocation.TextLocation text = new EmbeddingLocation.TextLocation();
        text.setParagraph(7);
        location.setText(text);

        String json = ParquetEmbeddingWriter.serializeLocation(location);

        JsonNode node = mapper.readTree(json);
        assertThat(node.get("text").get("paragraph").asInt()).isEqualTo(7);
        // page is null -> omitted (NON_NULL); other location kinds absent
        assertThat(node.get("text").has("page")).isFalse();
        assertThat(node.has("position")).isFalse();
        assertThat(node.has("timestamp")).isFalse();
        assertThat(node.has("spreadsheet")).isFalse();
    }

    @Test
    void serializeLocation_allKinds_usesWireFieldNames() throws Exception {
        EmbeddingLocation location = new EmbeddingLocation();

        EmbeddingLocation.TextLocation text = new EmbeddingLocation.TextLocation();
        text.setPage(2);
        text.setParagraph(4);
        location.setText(text);

        EmbeddingLocation.PositionLocation position = new EmbeddingLocation.PositionLocation();
        position.setLeft(1);
        position.setTop(2);
        position.setRight(3);
        position.setBottom(4);
        location.setPosition(position);

        EmbeddingLocation.TimestampLocation timestamp = new EmbeddingLocation.TimestampLocation();
        timestamp.setStart(0.5);
        timestamp.setEnd(1.5);
        location.setTimestamp(timestamp);

        EmbeddingLocation.SpreadsheetLocation spreadsheet = new EmbeddingLocation.SpreadsheetLocation();
        spreadsheet.setColumn(1);
        spreadsheet.setRow(2);
        spreadsheet.setSheet("Sheet1");
        location.setSpreadSheet(spreadsheet);

        String json = ParquetEmbeddingWriter.serializeLocation(location);

        JsonNode node = mapper.readTree(json);
        assertThat(node.get("text").get("page").asInt()).isEqualTo(2);
        assertThat(node.get("position").get("bottom").asInt()).isEqualTo(4);
        assertThat(node.get("timestamp").get("end").asDouble()).isEqualTo(1.5);
        // Field name is the wire form "spreadsheet", not the Java field "spreadSheet".
        assertThat(node.has("spreadsheet")).isTrue();
        assertThat(node.get("spreadsheet").get("sheet").asText()).isEqualTo("Sheet1");
    }

    /**
     * Every row carries the embedding type of the file, not whatever the caller happened to set on
     * each embedding.
     *
     * <p>The two used to disagree: the child document is named {@code _e_{embeddingType}} from the
     * derived type ({@code ai-mxbai-embed-large}) while each row's {@code type} came from the raw
     * configured model ({@code ai/mxbai-embed-large}). {@code sysembed_type} is what an embeddings
     * query matches on, so a query naming the derived type would have matched nothing at all. The
     * divergence is invisible while the read path substitutes the {@code *} wildcard, which is why it
     * survived until a query needed to name a type (#121).</p>
     */
    @Test
    void everyRowCarriesTheEmbeddingTypeOfTheFile(@TempDir java.nio.file.Path tempDir) throws Exception {
        HxprEmbedding first = embedding("chunk-0", "first chunk");
        HxprEmbedding second = embedding("chunk-1", "second chunk");
        // The raw configured model, which is what the write path used to record per row.
        first.setType("ai/mxbai-embed-large");
        second.setType(null);

        byte[] parquet = ParquetEmbeddingWriter.writeToParquet(
                List.of(first, second), "ai-mxbai-embed-large");

        assertThat(readTypes(tempDir, parquet))
                .containsExactly("ai-mxbai-embed-large", "ai-mxbai-embed-large");
    }

    private static HxprEmbedding embedding(String chunkId, String text) {
        HxprEmbedding embedding = new HxprEmbedding();
        embedding.setChunkId(chunkId);
        embedding.setText(text);
        embedding.setVector(List.of(0.1d, 0.2d, 0.3d));
        return embedding;
    }

    private static List<String> readTypes(java.nio.file.Path tempDir, byte[] parquet) throws Exception {
        java.nio.file.Path file = tempDir.resolve("embeddings.parquet");
        Files.write(file, parquet);

        List<String> types = new ArrayList<>();
        try (ParquetReader<GenericRecord> reader = AvroParquetReader
                .<GenericRecord>builder(new Path(file.toUri()))
                .withConf(new Configuration())
                .build()) {
            GenericRecord record;
            while ((record = reader.read()) != null) {
                Object type = record.get("type");
                types.add(type == null ? null : type.toString());
            }
        }
        return types;
    }
}
