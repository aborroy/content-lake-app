package org.hyland.contentlake.rag.service;

import java.util.List;

/**
 * One query formulation to run a retrieval pass for.
 *
 * <p>A search may run several of these and fuse the result sets. The vector and keyword sides carry
 * separate text because the two legs want different things from an expansion:</p>
 * <ul>
 *   <li>A multi-query paraphrase is a question, so it drives both legs.</li>
 *   <li>A HyDE passage is answer-shaped prose. It belongs on the vector leg, where proximity to real
 *       answer chunks is the point, and not on the keyword leg, where its incidental vocabulary would
 *       flood the BM25 term list and displace the terms the user actually asked about.</li>
 * </ul>
 *
 * @param label        short identifier for logging and diagnostics (e.g. {@code original}, {@code hyde})
 * @param vectorText   text embedded for the vector leg
 * @param vectorVector pre-computed embedding for {@code vectorText}, or {@code null} to embed it
 *                     query-side at search time. HyDE supplies one because its passage must be
 *                     embedded document-side, without the query instruction prefix.
 * @param keywordText  text used for keyword/fulltext matching, or {@code null} to skip the keyword leg
 * @param forceChunkFts when true, this variant's keyword terms are pushed into the vector call as
 *                      {@code VectorQuery.chunkFTS} so hxpr filters at the <em>chunk</em> level,
 *                      whatever the global chunk-FTS mode is set to. Only the verbatim-identifier
 *                      variant uses it, where restricting to chunks containing the token is the entire
 *                      point of the pass
 */
public record QueryVariant(String label,
                           String vectorText,
                           List<Double> vectorVector,
                           String keywordText,
                           boolean forceChunkFts) {

    /** The user's query, unmodified, embedded query-side and driving both legs. */
    public static final String LABEL_ORIGINAL = "original";

    /** The identifier-restricted pass (#122). */
    public static final String LABEL_VERBATIM = "verbatim";

    /** The variant every search runs, with or without expansion enabled. */
    public static QueryVariant original(String query) {
        return new QueryVariant(LABEL_ORIGINAL, query, null, query, false);
    }

    /** An alternative phrasing of the question; drives both legs, embedded query-side. */
    public static QueryVariant rephrased(String label, String text) {
        return new QueryVariant(label, text, null, text, false);
    }

    /** A vector-only variant carrying its own document-side embedding. */
    public static QueryVariant vectorOnly(String label, String text, List<Double> vector) {
        return new QueryVariant(label, text, vector, null, false);
    }

    /**
     * The verbatim pass for an identifier-like query (#122): the same question on the vector side, but
     * with the search restricted to chunks that contain the identifier itself.
     *
     * <p>A short alphanumeric identifier embeds poorly, because the token is rare rather than
     * ambiguous, which is why query expansion does not rescue it. Restricting to chunks holding the
     * literal token is what does. It carries the query's own vector so no second embedding call is made,
     * and its keyword text is the identifiers alone, so the term ranking is about them rather than about
     * the surrounding prose.</p>
     *
     * @param query       the user's query, embedded exactly as the original variant embeds it
     * @param queryVector that query's vector, already computed
     * @param identifiers the identifier-like tokens found in the query
     */
    public static QueryVariant verbatim(String query, List<Double> queryVector, List<String> identifiers) {
        return new QueryVariant(LABEL_VERBATIM, query, queryVector, String.join(" ", identifiers), true);
    }

    /** True when this variant should contribute to the keyword leg. */
    public boolean hasKeywordLeg() {
        return keywordText != null && !keywordText.isBlank();
    }

    /**
     * This variant with an already-computed vector attached, or itself when it has one.
     *
     * <p>Used when another pass has already embedded the same text: without it, a search that adds the
     * verbatim pass (#122) would embed the user's query twice, since the pass has to embed it to be
     * built and the original variant would then embed it again. The query cache masks that, but it is
     * off by default.</p>
     */
    public QueryVariant withVector(List<Double> vector) {
        if (vectorVector != null || vector == null || vector.isEmpty()) {
            return this;
        }
        return new QueryVariant(label, vectorText, vector, keywordText, forceChunkFts);
    }
}
