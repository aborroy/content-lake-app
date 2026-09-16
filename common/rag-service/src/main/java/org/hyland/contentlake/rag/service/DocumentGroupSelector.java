package org.hyland.contentlake.rag.service;

import org.hyland.contentlake.rag.model.SemanticSearchResponse.SearchHit;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 * Selects whole documents from a ranked pool of chunks, so a caller can ask for a number of documents
 * rather than a number of chunks (#135).
 *
 * <p>{@code topK} is a budget of chunks and a document contributes every chunk it has, so ten results
 * can be two documents. {@link DocumentDiversityLimiter} stops one document from taking the whole
 * budget, but it cannot promise how many documents arrive, because it is still spending a chunk budget.
 * {@code topDocuments} is that promise, and it needs a different selection: take the best
 * {@code maxDocuments} documents, and up to {@code maxPerDocument} chunks from each.</p>
 *
 * <p>Output is <strong>grouped</strong>. A document sits at the position of its best chunk and its
 * chunks follow in the relative order the ranking gave them, so a caller reading a flat result list
 * gets each document's chunks together. The consequence to know is that {@code score} is therefore no
 * longer monotonically decreasing down the list: a document's second chunk precedes the next document's
 * best one. That is already true of a diversity-capped response, and {@code score} stays the cosine
 * value its own retrieval pass produced, which is the convention {@link RrfFusion} and
 * {@link DocumentDiversityLimiter} both follow.</p>
 *
 * <h3>No backfill, unlike the sibling</h3>
 *
 * <p>{@link DocumentDiversityLimiter#cap} appends what the cap deferred, so it can only reorder a
 * result set and never shorten one. This deliberately does not: re-admitting chunks from documents
 * already at {@code maxPerDocument}, or from documents past {@code maxDocuments}, would break the one
 * guarantee {@code topDocuments} exists to give. So a selected result set may be shorter than
 * {@code maxDocuments * maxPerDocument}, which is correct rather than a shortfall, and is why the
 * response reports {@code documentCount}: without it a caller cannot tell "only three documents
 * matched" from "the budget was miscomputed".</p>
 */
final class DocumentGroupSelector {

    private DocumentGroupSelector() {
    }

    /**
     * Selects the best {@code maxDocuments} documents and renumbers ranks from 1.
     *
     * @param hits           hits ordered best first
     * @param maxDocuments   distinct documents to select; non-positive selects every document present
     * @param maxPerDocument most chunks to take from one document; non-positive takes all of them
     */
    static List<SearchHit> select(List<SearchHit> hits, int maxDocuments, int maxPerDocument) {
        return renumber(selectGroups(hits, maxDocuments, maxPerDocument, DocumentDiversityLimiter::documentKey));
    }

    /**
     * Selects the best {@code maxDocuments} groups, for any hit type.
     *
     * <p>Generic for the same reason {@link DocumentDiversityLimiter#cap} is: the semantic endpoint ranks
     * {@link SearchHit} and the hybrid endpoint ranks its own fused results, and a selection implemented
     * for one of them measures as a no-op on the other.</p>
     *
     * <p>A hit whose key is null is dropped rather than filed under a shared "unknown" document. Lumping
     * them together would let unrelated hits consume one document's slot and one document's chunk
     * allowance between them, so a single unkeyable hit could cost a real document its place.</p>
     *
     * @param ordered        hits ordered best first
     * @param maxDocuments   distinct groups to select; non-positive selects every group present
     * @param maxPerDocument most hits to take from one group; non-positive takes all of them
     * @param keyFn          the document a hit belongs to; a null key drops the hit
     */
    static <T> List<T> selectGroups(List<T> ordered,
                                    int maxDocuments,
                                    int maxPerDocument,
                                    Function<T, String> keyFn) {
        if (ordered == null || ordered.isEmpty()) {
            return List.of();
        }

        // Insertion-ordered, so iterating the groups at the end walks documents in best-chunk order.
        Map<String, List<T>> groups = new LinkedHashMap<>();

        for (T hit : ordered) {
            if (hit == null) {
                continue;
            }
            String key = keyFn.apply(hit);
            if (key == null) {
                continue;
            }

            List<T> group = groups.get(key);
            if (group == null) {
                // A new document only gets in while there is a slot. Stopping the whole walk here instead
                // would be wrong: later chunks of already-selected documents are still wanted.
                if (maxDocuments > 0 && groups.size() >= maxDocuments) {
                    continue;
                }
                group = new ArrayList<>();
                groups.put(key, group);
            }

            if (maxPerDocument > 0 && group.size() >= maxPerDocument) {
                continue;
            }
            group.add(hit);
        }

        List<T> selected = new ArrayList<>();
        for (List<T> group : groups.values()) {
            selected.addAll(group);
        }
        return selected;
    }

    /** The number of distinct documents a selected result set covers. */
    static int documentCount(List<SearchHit> hits) {
        return distinctGroups(hits, DocumentDiversityLimiter::documentKey);
    }

    /** The number of distinct groups present, for any hit type. Hits with a null key do not count. */
    static <T> int distinctGroups(List<T> hits, Function<T, String> keyFn) {
        if (hits == null || hits.isEmpty()) {
            return 0;
        }
        Set<String> keys = new HashSet<>();
        for (T hit : hits) {
            if (hit == null) {
                continue;
            }
            String key = keyFn.apply(hit);
            if (key != null) {
                keys.add(key);
            }
        }
        return keys.size();
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
