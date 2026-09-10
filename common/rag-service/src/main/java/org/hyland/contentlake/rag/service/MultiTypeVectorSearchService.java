package org.hyland.contentlake.rag.service;

import lombok.extern.slf4j.Slf4j;
import org.hyland.contentlake.client.EmbeddingTypeCatalog;
import org.hyland.contentlake.client.HxprService;
import org.hyland.contentlake.hxpr.api.model.Embedding;
import org.hyland.contentlake.hxpr.api.model.VectorSearchResult;
import org.hyland.contentlake.service.EmbeddingService;
import org.hyland.contentlake.service.EmbeddingTypeResolver;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 * The vector leg, over however many embedding types the corpus holds (#121).
 *
 * <h3>Why one search per type</h3>
 * <p>hxpr partitions vectors by {@code sysembed_type} and its embeddings query accepts an
 * {@code embeddingType} that defaults to the {@code *} wildcard, so a single call already returns
 * candidates from every type. What that call cannot do is score them meaningfully: a query vector
 * produced by one model has no interpretable similarity to vectors produced by another, so under the
 * wildcard a retired type's chunks are ranked by a number computed in the wrong space. Embedding the
 * query once per model and restricting each search to that model's type is the only way to get a
 * comparison that means anything.</p>
 *
 * <h3>Why the scores are merged raw</h3>
 * <p>Every type's vectors live in the same {@code sysembed_vector} field of the same index, so every
 * type's score comes from the same similarity metric and is already on one scale. Rescaling per type
 * would not make the scores more comparable; it would destroy the only comparison available. Dividing
 * each type's candidates by that type's own best maps every active type's top hit to exactly 1.0, so
 * with N types the merged head becomes one hit per type regardless of relevance, a retired type's best
 * chunk is promoted to tie the current type's best, and {@code minScore} can no longer filter any
 * type's best hit at all. Rank-based fusion across types has the same flaw for the same reason: it too
 * gives every type's first place an identical contribution.</p>
 *
 * <p>What the scores cannot survive is types whose vectors are <em>not</em> in one metric, which in this
 * index means not in one field. That is the same constraint as sharing dimensionality, and it belongs to
 * the index rather than to this class.</p>
 *
 * <h3>The single-type case is untouched</h3>
 * <p>With one active type this delegates to exactly the call the services made before, wildcard and
 * raw scores included. That is deliberate: a single-model corpus is the normal case and its ranking,
 * its score scale and its {@code minScore} calibration must not move because this class exists. Merging
 * raw is what extends that guarantee to the multi-type case: the scale no longer changes the moment a
 * second type appears, so {@code SEARCH_HYBRID_MIN_SCORE} and {@code RAG_RETRIEVAL_GRADING_MIN_SCORE}
 * keep their meaning throughout a migration.</p>
 */
@Slf4j
public class MultiTypeVectorSearchService {

    private final HxprService hxprService;
    private final EmbeddingTypeCatalog catalog;
    private final EmbeddingService primaryEmbeddingService;

    /**
     * Embedders by the type they can query, resolved once at startup from the configured models.
     *
     * <p>Keyed on both the raw model name and its derived type, because a corpus can carry either
     * form: rows written before the type and the child name were reconciled recorded the raw
     * {@code ai/mxbai-embed-large} rather than the derived {@code ai-mxbai-embed-large}.</p>
     */
    private final Map<String, EmbeddingService> embeddersByType;

    /** Types already reported as having no configured model, so the warning is logged once each. */
    private final Set<String> warnedUnmappedTypes = java.util.concurrent.ConcurrentHashMap.newKeySet();

    public MultiTypeVectorSearchService(HxprService hxprService,
                                        EmbeddingTypeCatalog catalog,
                                        EmbeddingService primaryEmbeddingService,
                                        List<String> additionalModels) {
        this.hxprService = hxprService;
        this.catalog = catalog;
        this.primaryEmbeddingService = primaryEmbeddingService;
        this.embeddersByType = buildEmbedders(primaryEmbeddingService, additionalModels);
    }

    private static Map<String, EmbeddingService> buildEmbedders(EmbeddingService primary,
                                                                List<String> additionalModels) {
        Map<String, EmbeddingService> byType = new LinkedHashMap<>();
        register(byType, primary.getModelName(), primary);

        if (additionalModels != null) {
            for (String model : additionalModels) {
                if (model == null || model.isBlank()) {
                    continue;
                }
                String modelName = model.trim();
                EmbeddingService embedder = primary.forModel(modelName);
                if (embedder == null) {
                    // forModel already logged why. Registering the primary here instead would score
                    // that type's vectors in the wrong space while looking configured.
                    continue;
                }
                register(byType, modelName, embedder);
                log.info("Additional embedding model configured for multi-type retrieval: {}", modelName);
            }
        }
        return Map.copyOf(byType);
    }

    /** Registers an embedder under both the raw model name and the derived embedding type. */
    private static void register(Map<String, EmbeddingService> byType, String modelName,
                                EmbeddingService embedder) {
        if (modelName == null || modelName.isBlank()) {
            return;
        }
        byType.putIfAbsent(modelName, embedder);
        try {
            byType.putIfAbsent(EmbeddingTypeResolver.toEmbeddingType(modelName), embedder);
        } catch (IllegalArgumentException e) {
            log.warn("Model name '{}' does not derive an embedding type: {}", modelName, e.getMessage());
        }
    }

    /**
     * Runs the vector leg and returns the merged candidates.
     *
     * @param request what to search for and how far to look
     * @return the merged, ranked candidates, or {@code null} when hxpr returned nothing at all
     */
    public VectorSearchResult search(Request request) {
        // A caller that named a type is asking for that type only. Honour it exactly: pinning is how a
        // diagnostic or a migration check isolates one model.
        if (request.embeddingType() != null && !request.embeddingType().isBlank()) {
            return searchOneType(request, request.embeddingType(), request.primaryVector());
        }

        List<String> types = catalog.activeTypes();
        if (types.size() <= 1) {
            // The wildcard, exactly as before: same call, same ordering, same score scale.
            return searchOneType(request, null, request.primaryVector());
        }

        List<TypedCandidates> perType = new ArrayList<>(types.size());
        for (String type : types) {
            List<Double> vector = vectorFor(type, request);
            if (vector == null || vector.isEmpty()) {
                continue;
            }
            VectorSearchResult result = searchOneType(request, type, vector);
            List<Embedding> embeddings = result == null ? null : result.getEmbeddings();
            if (embeddings != null && !embeddings.isEmpty()) {
                perType.add(new TypedCandidates(type, embeddings,
                        result.getTotalCount() == null ? 0L : result.getTotalCount()));
            }
        }

        return merge(perType, request.limit());
    }

    private VectorSearchResult searchOneType(Request request, String embeddingType, List<Double> vector) {
        return (request.chunkFts() != null && !request.chunkFts().isBlank())
                ? hxprService.vectorSearch(vector, embeddingType, request.hxqlFilter(),
                        request.chunkFts(), request.limit())
                : hxprService.vectorSearch(vector, embeddingType, request.hxqlFilter(), request.limit());
    }

    /**
     * The query vector to search a given type with: the one already computed for the configured model,
     * or a fresh one from that type's own model.
     *
     * <p>A type with no configured model falls back to the primary vector, with a warning logged once.
     * That is what the wildcard already does for it, so this is not a regression; it is also not a
     * meaningful comparison, and naming the model in {@code rag.embedding.additional-models} is what
     * makes it one.</p>
     */
    private List<Double> vectorFor(String type, Request request) {
        EmbeddingService embedder = embeddersByType.get(type);
        if (embedder == null) {
            if (warnedUnmappedTypes.add(type)) {
                log.warn("Embedding type '{}' is present in the corpus but no configured model produces "
                                + "it, so it is queried with '{}' and its scores are not a comparison in "
                                + "its own vector space. Add the model to rag.embedding.additional-models.",
                        type, primaryEmbeddingService.getModelName());
            }
            return request.primaryVector();
        }
        if (embedder == primaryEmbeddingService) {
            return request.primaryVector();
        }
        try {
            return request.embedder().apply(embedder);
        } catch (Exception e) {
            log.warn("Failed to embed the query for embedding type '{}', skipping it: {}",
                    type, e.getMessage());
            return null;
        }
    }

    /**
     * Merges per-type candidates on their own scores, keeping the best occurrence of each chunk.
     *
     * <p>Scores are left exactly as hxpr reported them, so the number the ranking uses is the number
     * every downstream stage sees: leg fusion, the {@code minScore} threshold and the score on a hit.
     * The only thing this does beyond concatenating is collapse a chunk found under more than one type,
     * which happens whenever a document has been re-embedded but the previous type's child has not been
     * cleared yet, and which would otherwise occupy two of the caller's slots with the same text.</p>
     */
    private VectorSearchResult merge(List<TypedCandidates> perType, int limit) {
        if (perType.isEmpty()) {
            return null;
        }
        if (perType.size() == 1) {
            TypedCandidates only = perType.get(0);
            return result(only.embeddings(), only.totalCount());
        }

        Map<String, Embedding> best = new LinkedHashMap<>();
        long totalCount = 0;

        for (TypedCandidates candidates : perType) {
            totalCount += candidates.totalCount();
            for (Embedding embedding : candidates.embeddings()) {
                String key = chunkKey(embedding);
                Embedding incumbent = best.get(key);
                if (incumbent == null || score(embedding) > score(incumbent)) {
                    best.put(key, embedding);
                }
            }
        }

        List<Embedding> merged = best.values().stream()
                .sorted(Comparator.comparingDouble(MultiTypeVectorSearchService::score).reversed())
                .limit(Math.max(1, limit))
                .toList();

        log.debug("Merged {} candidates across {} embedding types", merged.size(), perType.size());
        return result(merged, totalCount);
    }

    private static double score(Embedding embedding) {
        return embedding.getSysembedScore() == null ? 0.0 : embedding.getSysembedScore();
    }

    /**
     * Identity of a chunk across types: the document plus the chunk's position in it.
     *
     * <p>Not {@code sysembed_id}, which is the chunk id the pipeline generated when it chunked, so it is
     * a fresh value on every re-chunk and therefore differs between two types covering the same chunk.
     * Keying on it means the collapse never happens and the caller's head fills with the same text once
     * per type, which is exactly what a two-type corpus produced before this used the position.</p>
     *
     * <p>Falls back to {@code sysembed_id} when a row carries no text position, so a row this cannot
     * place is left as its own candidate rather than colliding with an unrelated one.</p>
     */
    private static String chunkKey(Embedding embedding) {
        String docId = embedding.getSysembedDocId() == null ? "?" : embedding.getSysembedDocId();
        Integer paragraph = embedding.getSysembedLocation() == null
                || embedding.getSysembedLocation().getText() == null
                ? null
                : embedding.getSysembedLocation().getText().getParagraph();
        if (paragraph != null) {
            return docId + "::p" + paragraph;
        }
        return docId + "::" + (embedding.getSysembedId() == null ? "?" : embedding.getSysembedId());
    }

    private static VectorSearchResult result(List<Embedding> embeddings, long totalCount) {
        VectorSearchResult result = new VectorSearchResult();
        result.setEmbeddings(new ArrayList<>(embeddings));
        result.setTotalCount(totalCount);
        return result;
    }

    /** The types the corpus holds, for diagnostics and for the backfill to target. */
    public List<String> activeTypes() {
        return catalog.activeTypes();
    }

    /** Candidates from one embedding type, before any cross-type normalisation. */
    private record TypedCandidates(String embeddingType, List<Embedding> embeddings, long totalCount) {
    }

    /**
     * One vector-leg request.
     *
     * @param primaryVector  the query already embedded with the configured model, so the common case
     *                       costs no extra embedding call
     * @param embedder       produces the query vector for a non-primary model; called at most once per
     *                       additional active type, and only when the corpus actually holds one
     * @param embeddingType  a type to pin the search to, or {@code null} to search every active type
     * @param hxqlFilter     the permission-scoped HXQL filter
     * @param chunkFts       chunk-level terms hxpr matches against chunk text, or {@code null}
     * @param limit          candidates per type, and the size of the merged result
     */
    public record Request(List<Double> primaryVector,
                          Function<EmbeddingService, List<Double>> embedder,
                          String embeddingType,
                          String hxqlFilter,
                          String chunkFts,
                          int limit) {

        public static Request of(List<Double> primaryVector,
                                 Function<EmbeddingService, List<Double>> embedder,
                                 String embeddingType,
                                 String hxqlFilter,
                                 int limit) {
            return new Request(primaryVector, embedder, embeddingType, hxqlFilter, null, limit);
        }
    }
}
