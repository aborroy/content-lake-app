package org.hyland.contentlake.model;

import java.util.List;

/**
 * Measured evidence about whether a source node is actually retrievable, as opposed to the sync
 * status the ingester recorded.
 *
 * <p>{@code contentLake_syncStatus=INDEXED} is a claim, not evidence: a document can be present with
 * metadata and an ACL, report {@code INDEXED}, and hold zero embeddings, in which case it is invisible
 * to semantic and hybrid search while looking finished to monitoring. Distinguishing "never embedded"
 * from "embedded but not retrievable" needs the chunk count read off the embeddings index, so
 * {@link Measured} and {@link Claimed} are separate: everything in {@code claimed} is a value some
 * writer asserted, and everything in {@code measured} was counted at read time.</p>
 *
 * @param nodeId   the source node this proof concerns
 * @param verdict  the single summary judgement, {@code null} when a measurement degraded
 * @param measured values counted from hxpr at read time
 * @param claimed  values previously recorded by a writer, for comparison against {@code measured}
 * @param error    populated when a measurement could not be taken, in which case {@code verdict} and
 *                 {@link Measured#chunkCount()} are {@code null} rather than guessed
 */
public record IndexProof(
        String nodeId,
        Verdict verdict,
        Measured measured,
        Claimed claimed,
        String error
) {

    /** The three states a synced node can be in, ordered from healthiest to worst. */
    public enum Verdict {
        /** The document exists and the embeddings index holds at least one chunk for it. */
        INDEXED_WITH_EMBEDDINGS,
        /** The document exists but the embeddings index holds no chunks: it cannot be retrieved. */
        METADATA_ONLY,
        /** No document exists for this node under any known source-id variant. */
        ABSENT
    }

    /**
     * Values counted at read time.
     *
     * @param exists              whether an hxpr document was found
     * @param documentId          the hxpr {@code sys_id} of that document
     * @param cinSourceId         the {@code cin_sourceId} variant the document was actually found
     *                            under, which differs from the queried value for documents indexed
     *                            before the {@code type:rawId} format
     * @param chunkCount          chunks the embeddings index holds, counted by walking the rows,
     *                            {@code null} when the count could not be taken
     * @param chunkCountTruncated whether the count is a floor rather than the total, because the walk
     *                            stopped at its row ceiling or broke down part way
     * @param embeddingTypes      every type the document has an embedding child for, so a child left
     *                            behind by a retired model is visible
     * @param embeddingChildren   the embedding children themselves, name and id
     * @param chunkSample         a bounded sample of chunks, so the response size does not grow with
     *                            the document
     */
    public record Measured(
            boolean exists,
            String documentId,
            String cinSourceId,
            Long chunkCount,
            Boolean chunkCountTruncated,
            List<String> embeddingTypes,
            List<EmbeddingChildRef> embeddingChildren,
            List<ChunkRef> chunkSample
    ) {
    }

    /**
     * Values a writer recorded, returned so they can be compared against the measured ones.
     *
     * @param syncStatus       {@code contentLake_syncStatus} on the hxpr document
     * @param syncError        {@code contentLake_syncError} on the hxpr document
     * @param sourceModifiedAt {@code source_modifiedAt} on the hxpr document
     * @param sourceNodeResolved whether the node could be read from the source at all. False means
     *                         either that it is gone or that the caller cannot read it, which are
     *                         deliberately indistinguishable; in that case no chunk text is returned,
     *                         but the measured index state still is, because a document whose source
     *                         node is gone is exactly the phantom this endpoint exists to find.
     * @param nodeSyncStatus   the status written back to the source node, when the source has one
     * @param sectionMapChunks chunk count implied by {@code contentLake_sectionMap}, which is what
     *                         ingestion believed it produced. A section map with chunks and a
     *                         measured count of zero is the signature of an embedding phase that
     *                         never completed.
     */
    public record Claimed(
            String syncStatus,
            String syncError,
            String sourceModifiedAt,
            boolean sourceNodeResolved,
            String nodeSyncStatus,
            Integer sectionMapChunks
    ) {
    }

    /**
     * One embedding child of the document.
     *
     * @param sysId         hxpr identifier
     * @param sysName       child name, {@code _e_{embeddingType}}
     * @param embeddingType the type recovered from the name
     */
    public record EmbeddingChildRef(String sysId, String sysName, String embeddingType) {
    }

    /**
     * One sampled chunk.
     *
     * @param chunkId       the embedding identifier
     * @param embeddingType the type this chunk was embedded under
     * @param textPrefix    a truncated prefix of the chunk text, enough to recognise it
     * @param page          page location when the extractor recorded one
     */
    public record ChunkRef(String chunkId, String embeddingType, String textPrefix, Integer page) {
    }
}
