package org.hyland.contentlake.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.hyland.contentlake.client.HxprService;
import org.hyland.contentlake.hxpr.api.model.Embedding;
import org.hyland.contentlake.hxpr.api.model.VectorSearchResult;
import org.hyland.contentlake.model.ContentLakeIngestProperties;
import org.hyland.contentlake.model.HxprDocument;
import org.hyland.contentlake.model.IndexProof;
import org.hyland.contentlake.model.SectionMap;
import org.hyland.contentlake.security.AclFilterBuilder;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

/**
 * Measures whether a node is actually retrievable, rather than reporting the sync status a writer
 * recorded.
 *
 * <p>Source-agnostic on purpose: it knows only {@code cin_id}, {@code cin_sourceId} and the hxpr
 * client, so any source adapter can expose it. Callers add their own source-side claims.</p>
 *
 * <h3>Why the chunk count is taken with a vector query</h3>
 * <p>hxpr has no aggregation endpoint over the embeddings index: {@code termsAggregation} covers the
 * document index only, and {@code GET /api/documents/{docId}/embedding/{embeddingId}} needs an id the
 * caller does not have yet. A vector query filtered to one {@code cin_id} returns the document's chunk
 * rows, with their ids, types and text. The vector itself is irrelevant to the count because the HXQL
 * filter does the selecting, so a fixed probe string is embedded once and cached: correct dimensionality
 * by construction, no per-request model call, and a deterministic result.</p>
 *
 * <p>The count is the number of rows returned, walked a page at a time, rather than the response's
 * {@code totalCount} -- which reports {@code limit + offset} rather than the true total (#128).</p>
 */
@Slf4j
public class IndexProofService {

    private static final ObjectMapper SECTION_MAP_MAPPER = new ObjectMapper();

    /**
     * Text embedded once to obtain a probe vector of the right dimensionality. Its content is
     * immaterial: the HXQL filter selects the chunks and the resulting scores are discarded.
     */
    private static final String PROBE_TEXT = "content lake index proof probe";

    /** Default number of sampled chunks. */
    public static final int DEFAULT_SAMPLE_SIZE = 5;

    /** Ceiling on the sample, so a caller cannot ask for an unbounded response. */
    public static final int MAX_SAMPLE_SIZE = 25;

    /** Chunk text is truncated to this many characters: enough to recognise a chunk, no more. */
    public static final int TEXT_PREFIX_CHARS = 200;

    /**
     * Rows requested per counting page. Above any realistic chunk count for one document, so the count
     * costs a single call in normal operation, and small enough that the discarded rows beyond the
     * sample are not a large response.
     */
    static final int COUNT_PAGE_SIZE = 100;

    /** Ceiling on the walk, so one pathological document cannot make a proof unbounded. */
    static final int MAX_COUNT_PAGES = 100;

    private final HxprService hxprService;
    private final EmbeddingService embeddingService;

    /** Lazily embedded and then reused; the probe never changes for a given model. */
    private volatile List<Double> probeVector;

    public IndexProofService(HxprService hxprService, EmbeddingService embeddingService) {
        this.hxprService = hxprService;
        this.embeddingService = embeddingService;
    }

    /**
     * Measures a node's index state.
     *
     * @param nodeId     source node identifier, matched against {@code cin_id}
     * @param sourceId   {@code "<sourceType>:<sourceId>"}; the legacy raw form is matched too
     * @param sampleSize requested chunk sample, clamped to {@link #MAX_SAMPLE_SIZE}
     * @param nodeClaims source-side claims to carry through, may be {@code null}
     * @return the proof; never throws, a failed measurement is reported in
     *         {@link IndexProof#error()} with a {@code null} verdict rather than as an exception
     */
    public IndexProof prove(String nodeId, String sourceId, int sampleSize, NodeClaims nodeClaims) {
        // A caller who could not resolve the node in the source gets counts but no chunk text: the
        // measured index state is what diagnoses a phantom, while the text belongs to a document
        // whose readability could not be established.
        boolean sourceResolved = nodeClaims == null || nodeClaims.sourceNodeResolved();
        int sample = sourceResolved ? Math.clamp(sampleSize, 1, MAX_SAMPLE_SIZE) : 0;

        HxprDocument document;
        try {
            document = hxprService.findByNodeId(nodeId, sourceId);
        } catch (Exception e) {
            log.warn("Index proof for {}: document lookup failed: {}", nodeId, e.getMessage());
            return degraded(nodeId, "Document lookup failed: " + e.getMessage(), nodeClaims);
        }

        if (document == null) {
            return new IndexProof(
                    nodeId,
                    IndexProof.Verdict.ABSENT,
                    new IndexProof.Measured(false, null, null, 0L, false,
                            List.of(), List.of(), List.of()),
                    claimed(null, nodeClaims),
                    null);
        }

        String documentId = document.getSysId();

        List<IndexProof.EmbeddingChildRef> children;
        try {
            children = hxprService.listEmbeddingChildren(documentId).stream()
                    .map(c -> new IndexProof.EmbeddingChildRef(c.sysId(), c.sysName(), c.embeddingType()))
                    .toList();
        } catch (Exception e) {
            // The child enumeration is exact and cheap; losing it costs the orphan-type view but not
            // the chunk count, so degrade this part rather than the whole proof.
            log.warn("Index proof for {}: embedding-child enumeration failed: {}", nodeId, e.getMessage());
            children = List.of();
        }

        ChunkMeasurement chunks = measureChunks(nodeId, documentId, sample);

        List<String> embeddingTypes = new ArrayList<>(new LinkedHashSet<>(
                children.stream().map(IndexProof.EmbeddingChildRef::embeddingType).toList()));
        for (IndexProof.ChunkRef chunk : chunks.sample()) {
            if (chunk.embeddingType() != null && !embeddingTypes.contains(chunk.embeddingType())) {
                embeddingTypes.add(chunk.embeddingType());
            }
        }

        IndexProof.Measured measured = new IndexProof.Measured(
                true,
                documentId,
                document.getCinSourceId(),
                chunks.count(),
                chunks.truncated(),
                embeddingTypes,
                children,
                chunks.sample());

        // No verdict when the count could not be taken: a guessed verdict is worse than none, since
        // the whole point of this endpoint is that a confident-looking status can be wrong.
        IndexProof.Verdict verdict = chunks.count() == null
                ? null
                : (chunks.count() > 0
                    ? IndexProof.Verdict.INDEXED_WITH_EMBEDDINGS
                    : IndexProof.Verdict.METADATA_ONLY);

        return new IndexProof(nodeId, verdict, measured, claimed(document, nodeClaims), chunks.error());
    }

    /**
     * Counts and samples the document's chunks off the embeddings index.
     *
     * <p>The count is the number of rows walked, not {@code totalCount}. Probing
     * {@code POST /api/query/embeddings} against a 587-row index returned a {@code totalCount} equal to
     * {@code limit + offset} in every case, with {@code trackTotalCountUpTo: -1} making no difference, so
     * the reported total says how much was asked for rather than how much exists (#128). Trusting it
     * capped a document's chunk count at the sample size, which inverts the diagnostic this service
     * exists for: a document with more chunks than the sample would show fewer measured chunks than its
     * section map claims, and so read as embedded-but-unretrievable when it is healthy. Counting rows is
     * correct whether or not the platform ever starts reporting a true total.</p>
     *
     * <p>{@code POST /api/query/embeddings} returns intermittent 500s, so a failure here degrades to
     * a null count plus an error rather than failing the whole proof.</p>
     */
    private ChunkMeasurement measureChunks(String nodeId, String documentId, int sample) {
        String hxqlFilter = String.format(
                "SELECT * FROM SysFile WHERE cin_id = '%s'", AclFilterBuilder.escapeLiteral(nodeId));

        long counted = 0;
        boolean truncated = false;
        List<IndexProof.ChunkRef> chunkSample = new ArrayList<>();

        try {
            for (int page = 0; page < MAX_COUNT_PAGES; page++) {
                // embeddingType null so hxpr substitutes the * wildcard: the count must span every type,
                // including one left behind by a retired model (#113).
                VectorSearchResult result = hxprService.vectorSearch(
                        probeVector(), null, hxqlFilter, null, COUNT_PAGE_SIZE, page * COUNT_PAGE_SIZE);

                if (result == null) {
                    if (page == 0) {
                        return new ChunkMeasurement(null, null, List.of(),
                                "Embeddings query returned no result for document " + documentId);
                    }
                    // Rows were counted before the walk broke down, so the count is a floor rather than
                    // nothing. Saying so is more useful than discarding what was measured.
                    truncated = true;
                    break;
                }

                List<Embedding> rows = result.getEmbeddings();
                if (rows == null || rows.isEmpty()) {
                    break;
                }
                counted += rows.size();

                for (Embedding embedding : rows) {
                    if (chunkSample.size() >= sample) {
                        break;
                    }
                    chunkSample.add(toChunkRef(embedding));
                }

                if (rows.size() < COUNT_PAGE_SIZE) {
                    break;
                }
                if (page == MAX_COUNT_PAGES - 1) {
                    // A document with more chunks than the walk covers: report the floor as approximate
                    // rather than a total that happens to be where the walk stopped.
                    truncated = true;
                    log.warn("Index proof for {}: chunk count stopped at the {}-row ceiling",
                            nodeId, MAX_COUNT_PAGES * COUNT_PAGE_SIZE);
                }
            }

            return new ChunkMeasurement(counted, truncated, chunkSample, null);

        } catch (Exception e) {
            log.warn("Index proof for {}: embeddings query failed: {}", nodeId, e.getMessage());
            return new ChunkMeasurement(null, null, List.of(),
                    "Chunk count unavailable: " + e.getMessage());
        }
    }

    private static IndexProof.ChunkRef toChunkRef(Embedding embedding) {
        String text = embedding.getSysembedText();
        String prefix = text == null
                ? null
                : (text.length() <= TEXT_PREFIX_CHARS ? text : text.substring(0, TEXT_PREFIX_CHARS));

        Integer page = null;
        if (embedding.getSysembedLocation() != null && embedding.getSysembedLocation().getText() != null) {
            page = embedding.getSysembedLocation().getText().getPage();
        }

        return new IndexProof.ChunkRef(
                embedding.getSysembedId(), embedding.getSysembedType(), prefix, page);
    }

    private List<Double> probeVector() {
        List<Double> cached = probeVector;
        if (cached == null) {
            synchronized (this) {
                cached = probeVector;
                if (cached == null) {
                    cached = embeddingService.embed(PROBE_TEXT);
                    probeVector = cached;
                }
            }
        }
        return cached;
    }

    private IndexProof degraded(String nodeId, String error, NodeClaims nodeClaims) {
        return new IndexProof(
                nodeId,
                null,
                new IndexProof.Measured(false, null, null, null, null, List.of(), List.of(), List.of()),
                claimed(null, nodeClaims),
                error);
    }

    private IndexProof.Claimed claimed(HxprDocument document, NodeClaims nodeClaims) {
        Map<String, Object> props = document != null ? document.getCinIngestProperties() : null;

        return new IndexProof.Claimed(
                stringProperty(props, ContentLakeIngestProperties.CONTENT_LAKE_SYNC_STATUS),
                stringProperty(props, ContentLakeIngestProperties.CONTENT_LAKE_SYNC_ERROR),
                resolveSourceModifiedAt(props),
                nodeClaims == null || nodeClaims.sourceNodeResolved(),
                nodeClaims != null ? nodeClaims.nodeSyncStatus() : null,
                sectionMapChunks(props));
    }

    /** {@code source_modifiedAt}, falling back to the legacy Alfresco-specific key. */
    private static String resolveSourceModifiedAt(Map<String, Object> props) {
        String value = stringProperty(props, ContentLakeIngestProperties.SOURCE_MODIFIED_AT);
        return value != null ? value : stringProperty(props, ContentLakeIngestProperties.ALFRESCO_MODIFIED_AT);
    }

    /**
     * Chunk count implied by the persisted section map, which is what ingestion believed it produced.
     * Compared against the measured count it separates "never embedded" from "embedded but not
     * retrievable", which is the distinction the status field cannot express.
     */
    private static Integer sectionMapChunks(Map<String, Object> props) {
        String json = stringProperty(props, ContentLakeIngestProperties.CONTENT_LAKE_SECTION_MAP);
        if (json == null) {
            return null;
        }
        try {
            SectionMap map = SECTION_MAP_MAPPER.readValue(json, SectionMap.class);
            return map.chunkSections() != null ? map.chunkSections().size() : null;
        } catch (Exception e) {
            log.debug("Index proof: section map could not be parsed: {}", e.getMessage());
            return null;
        }
    }

    private static String stringProperty(Map<String, Object> props, String key) {
        if (props == null) {
            return null;
        }
        Object value = props.get(key);
        return value != null ? value.toString() : null;
    }

    /**
     * Source-side claims the adapter supplies, kept out of core so it stays source-agnostic.
     *
     * @param nodeSyncStatus     status the ingester wrote back onto the source node, when the source
     *                           supports it (Alfresco {@code cl:syncStatusValue})
     * @param sourceNodeResolved whether the adapter could read the node from the source. False
     *                           suppresses the chunk sample: the index state is still measured, because
     *                           a document whose source node is gone is the phantom worth finding, but
     *                           its text is not returned for a node whose readability is unestablished.
     */
    public record NodeClaims(String nodeSyncStatus, boolean sourceNodeResolved) {

        /** Claims for a node the adapter read successfully. */
        public static NodeClaims resolved(String nodeSyncStatus) {
            return new NodeClaims(nodeSyncStatus, true);
        }

        /** Claims for a node the adapter could not read, whether gone or not readable by the caller. */
        public static NodeClaims unresolved() {
            return new NodeClaims(null, false);
        }
    }

    private record ChunkMeasurement(Long count, Boolean truncated,
                                    List<IndexProof.ChunkRef> sample, String error) {
    }
}
