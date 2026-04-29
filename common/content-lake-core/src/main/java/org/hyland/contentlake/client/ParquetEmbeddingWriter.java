package org.hyland.contentlake.client;

import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.hyland.contentlake.model.HxprEmbedding;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;

/**
 * Generates Parquet files containing embeddings for HXPR Content Lake storage.
 *
 * <p>This approach stores embeddings as Parquet files attached to child documents,
 * bypassing MongoDB's 16MB document size limit. Based on HXPR Content Lake architecture
 * described in CIN-6680.</p>
 */
@Slf4j
public class ParquetEmbeddingWriter {

    // Avro schema for HXPR Content Lake embeddings (com.hyland.hxpr.embeddings.avro.Embedding)
    private static final String EMBEDDING_SCHEMA = """
            {
              "type": "record",
              "name": "Embedding",
              "namespace": "com.hyland.hxpr.embeddings.avro",
              "fields": [
                {
                  "name": "id",
                  "type": "string",
                  "doc": "Unique identifier for this embedding row"
                },
                {
                  "name": "type",
                  "type": ["null", "string"],
                  "default": null,
                  "doc": "Embedding type (not used for indexing, derived from document name)"
                },
                {
                  "name": "vector",
                  "type": {
                    "type": "array",
                    "items": "double"
                  },
                  "doc": "The embedding vector (max 1024 dimensions)"
                },
                {
                  "name": "text",
                  "type": ["null", "string"],
                  "default": null,
                  "doc": "The text chunk that was embedded"
                },
                {
                  "name": "blobref",
                  "type": ["null", "string"],
                  "default": null,
                  "doc": "Reference to a blob on the parent document"
                },
                {
                  "name": "location",
                  "type": ["null", "string"],
                  "default": null,
                  "doc": "Location metadata as JSON string"
                }
              ]
            }
            """;

    private static final Schema SCHEMA = new Schema.Parser().parse(EMBEDDING_SCHEMA);

    /**
     * Writes embeddings to a Parquet file and returns the file content as byte array.
     *
     * @param embeddings list of embeddings to write
     * @param embeddingType identifier for the embedding model (e.g., "mxbai-embed-large")
     * @return byte array containing the Parquet file content
     * @throws IOException if file generation fails
     */
    public static byte[] writeToParquet(List<HxprEmbedding> embeddings, String embeddingType) throws IOException {
        if (embeddings == null || embeddings.isEmpty()) {
            throw new IllegalArgumentException("Embeddings list cannot be null or empty");
        }

        // Create temporary file for Parquet generation with unique name
        File tempFile = Files.createTempFile("embeddings-" + embeddingType + "-" + System.currentTimeMillis(), ".parquet").toFile();

        // Ensure cleanup even if JVM doesn't exit normally
        tempFile.deleteOnExit();

        try {
            // Delete if somehow already exists (from previous failed attempt)
            if (tempFile.exists()) {
                tempFile.delete();
            }

            // Write embeddings to Parquet file
            try (ParquetWriter<GenericRecord> writer = AvroParquetWriter
                    .<GenericRecord>builder(new Path(tempFile.toURI()))
                    .withSchema(SCHEMA)
                    .withConf(new Configuration())
                    .withCompressionCodec(CompressionCodecName.SNAPPY)
                    .build()) {

                int index = 0;
                for (HxprEmbedding embedding : embeddings) {
                    GenericRecord record = new GenericData.Record(SCHEMA);

                    // Required: id
                    String id = embedding.getChunkId() != null ? embedding.getChunkId() : String.valueOf(index);
                    record.put("id", id);

                    // Optional: type (derived from document name, so can be null)
                    record.put("type", embedding.getType());

                    // Required: vector
                    List<Double> vectorList = embedding.getVector();
                    if (vectorList != null) {
                        record.put("vector", vectorList);
                    } else {
                        record.put("vector", List.of());
                    }

                    // Optional: text
                    record.put("text", embedding.getText());

                    // Optional: blobref
                    record.put("blobref", null);

                    // Optional: location (convert to JSON string if present)
                    if (embedding.getLocation() != null) {
                        // TODO: Serialize location to JSON string
                        record.put("location", null);
                    } else {
                        record.put("location", null);
                    }

                    writer.write(record);
                    index++;
                }

                log.debug("Written {} embeddings to Parquet file: {}", embeddings.size(), tempFile.getName());
            }

            // Read file content into byte array
            try (FileInputStream fis = new FileInputStream(tempFile)) {
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                byte[] buffer = new byte[8192];
                int bytesRead;
                while ((bytesRead = fis.read(buffer)) != -1) {
                    baos.write(buffer, 0, bytesRead);
                }
                byte[] content = baos.toByteArray();

                log.info("Generated Parquet file for {} embeddings: {} bytes", embeddings.size(), content.length);
                return content;
            }

        } finally {
            // Clean up temp file - try multiple times if needed
            if (tempFile.exists()) {
                boolean deleted = tempFile.delete();
                if (!deleted) {
                    // Force deletion attempt
                    try {
                        Files.deleteIfExists(tempFile.toPath());
                    } catch (IOException e) {
                        log.warn("Failed to delete temporary Parquet file: {} - {}",
                                tempFile.getAbsolutePath(), e.getMessage());
                    }
                }
            }
        }
    }
}
