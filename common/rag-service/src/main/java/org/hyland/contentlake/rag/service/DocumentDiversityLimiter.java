package org.hyland.contentlake.rag.service;

import org.hyland.contentlake.rag.model.SemanticSearchResponse.SearchHit;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Keeps one document from consuming a whole result set.
 *
 * <p>{@code topK} is a budget of chunks, and a document contributes every chunk it has to the ranking,
 * so a long document can fill the budget on its own. Measured on the E2E fixture corpus, a {@code topK}
 * of 10 returned 10 chunks belonging to just 2 documents, and a document that ranks first on its own
 * merits was absent from a query quoting it nearly verbatim, purely because another document's chunks
 * were drawn ahead of it. To a caller that is indistinguishable from the document not being indexed.</p>
 *
 * <p>Two hits over the cap are deferred, not dropped: once the capped pass has taken what it can, the
 * remainder are appended in their original order until the limit is reached. So this can only reorder a
 * result set, never shorten one, and a corpus where every hit comes from a single document still returns
 * as many results as before.</p>
 *
 * <p>Ranks are reassigned from 1 and each hit keeps the {@code score} its own retrieval pass gave it,
 * the same convention {@link RrfFusion} follows: that field is a cosine value callers threshold and
 * display, so a diversity-adjusted number in it would mislead everything downstream.</p>
 */
final class DocumentDiversityLimiter {

    private DocumentDiversityLimiter() {
    }

    /**
     * Applies the per-document cap and trims to {@code limit}.
     *
     * @param hits            hits ordered best first
     * @param limit           maximum hits to return; non-positive means no limit
     * @param maxPerDocument  most hits one document may contribute before others are preferred;
     *                        non-positive disables the cap and leaves the order alone
     * @return reordered hits, never longer than {@code limit} and never shorter than the input trimmed
     *         to {@code limit}
     */
    static List<SearchHit> limit(List<SearchHit> hits, int limit, int maxPerDocument) {
        return renumber(cap(hits, limit, maxPerDocument, DocumentDiversityLimiter::documentKey));
    }

    /**
     * Applies the per-document cap and trims to {@code limit}, for any hit type.
     *
     * <p>Generic because the two search paths carry different types: the semantic endpoint ranks
     * {@link SearchHit}, the hybrid endpoint ranks its own fused results, and both need the same cap.
     * Fixing only one of them measures as a no-op on whichever harness queries the other.</p>
     *
     * @param ordered      hits ordered best first
     * @param limit        maximum hits to return; non-positive means no limit
     * @param maxPerGroup  most hits one document may contribute before others are preferred;
     *                     non-positive disables the cap and leaves the order alone
     * @param keyFn        the document a hit belongs to
     */
    static <T> List<T> cap(List<T> ordered, int limit, int maxPerGroup, Function<T, String> keyFn) {
        if (ordered == null || ordered.isEmpty()) {
            return List.of();
        }

        int cap = limit > 0 ? Math.min(limit, ordered.size()) : ordered.size();
        if (maxPerGroup <= 0) {
            return new ArrayList<>(ordered.subList(0, cap));
        }

        List<T> selected = new ArrayList<>(cap);
        List<T> deferred = new ArrayList<>();
        Map<String, Integer> perDocument = new HashMap<>();

        for (T hit : ordered) {
            if (hit == null) {
                continue;
            }
            if (selected.size() >= cap) {
                break;
            }
            String key = keyFn.apply(hit);
            int taken = perDocument.getOrDefault(key, 0);
            if (taken < maxPerGroup) {
                perDocument.put(key, taken + 1);
                selected.add(hit);
            } else {
                deferred.add(hit);
            }
        }

        // Backfill from the deferred hits so the cap cannot cost a caller results. This is why the cap
        // is safe to raise or lower: it changes which chunks come first, not how many arrive.
        for (T hit : deferred) {
            if (selected.size() >= cap) {
                break;
            }
            selected.add(hit);
        }

        return selected;
    }

    /**
     * The document a hit belongs to.
     *
     * <p>Prefers the hxpr document id and falls back to the source node id, then to the document name.
     * A hit with none of the three counts as its own document rather than joining a shared "unknown"
     * bucket, because grouping unrelated hits together would cap them against each other and drop real
     * results from the capped pass.</p>
     */
    private static String documentKey(SearchHit hit) {
        if (hit.getSourceDocument() != null) {
            String documentId = hit.getSourceDocument().getDocumentId();
            if (documentId != null && !documentId.isBlank()) {
                return "doc:" + documentId;
            }
            String nodeId = hit.getSourceDocument().getNodeId();
            if (nodeId != null && !nodeId.isBlank()) {
                return "node:" + nodeId;
            }
            String name = hit.getSourceDocument().getName();
            if (name != null && !name.isBlank()) {
                return "name:" + name;
            }
        }
        return "unkeyed:" + System.identityHashCode(hit);
    }

    private static List<SearchHit> renumber(List<SearchHit> hits) {
        List<SearchHit> results = new ArrayList<>(hits.size());
        for (int i = 0; i < hits.size(); i++) {
            SearchHit hit = hits.get(i);
            results.add(SearchHit.builder()
                    .rank(i + 1)
                    .score(hit.getScore())
                    .chunkText(hit.getChunkText())
                    .sourceDocument(hit.getSourceDocument())
                    .chunkMetadata(hit.getChunkMetadata())
                    .vector(hit.getVector())
                    .build());
        }
        return results;
    }
}
