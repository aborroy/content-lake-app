package org.hyland.contentlake.rag.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Request payload for the semantic search endpoint.
 *
 * <p>Accepts a natural-language query that is embedded using the same model
 * used during ingestion, then searched against the hxpr embeddings index via kNN.</p>
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class SemanticSearchRequest {

    /** Free-text query to embed and search. */
    private String query;

    /** Maximum number of chunks to return (default 5, max 50). */
    @Builder.Default
    private int topK = 5;

    /**
     * Distinct source documents to return chunks from (max 50), or {@code null} for chunk-oriented paging.
     *
     * <p>{@link #topK} is a budget of chunks and a document contributes every chunk it has, so ten results
     * can be two documents (#135). When this is set it owns the budget and {@code topK} is ignored: the
     * response carries chunks of up to this many documents, at most {@link #chunksPerDocument} from each.</p>
     *
     * <p>Nullable rather than a primitive so "I said nothing about documents" is distinguishable from
     * "zero documents". Every existing caller sends {@code topK} with its default, so a rule where the
     * smaller of the two binds would have made {@code topDocuments: 20} silently return 5 chunks.</p>
     */
    private Integer topDocuments;

    /**
     * Most chunks to take from any one document (max 10), or {@code null} to use
     * {@code rag.retrieval.document-diversity.max-chunks-per-document}.
     *
     * <p>Ships with {@link #topDocuments} because without it that field has no defined meaning when the
     * configured cap is disabled: nothing would bound how many chunks one document occupies. With it the
     * budget is the product of two request-visible numbers.</p>
     */
    private Integer chunksPerDocument;

    /** Optional HXQL filter to scope the search (appended to the permission filter). */
    private String filter;

    /**
     * Optional name of a hxpr named query, an alternative to the inline {@link #filter}: its
     * matching documents scope the search. Null (the default) leaves retrieval unchanged.
     */
    private String namedQuery;

    /** Optional source type filter (`alfresco` or `nuxeo`). */
    private String sourceType;

    /** Embedding type to match. Defaults to wildcard ("*") which matches all types. */
    private String embeddingType;

    /** Minimum similarity score threshold (0.0 – 1.0). Results below this score are excluded. */
    @Builder.Default
    private double minScore = 0.0;

    /**
     * Opts this search out of the per-document cap (#134). Not part of the wire format: it separates a
     * caller browsing results from the RAG pipeline gathering evidence.
     *
     * <p>An endpoint caller asking for ten results and getting ten chunks of two documents cannot tell
     * a crowded-out document from an unindexed one, so the cap is right for them. The generator wants
     * the best chunks for the question whatever document they came from, and capping it measurably costs
     * answer quality: on the 78-question golden set {@code faithfulness} fell 0.806 to 0.762 at a cap of
     * two and to 0.710 at three, and {@code citation_accuracy} 0.808 to 0.779 and 0.695, because a long
     * document that <em>is</em> the answer loses the chunks that supported it.</p>
     *
     * <p><strong>Negative on purpose.</strong> {@code false} is what a request has when nothing sets it,
     * however it was constructed, so the endpoint behaviour is the zero value and only
     * {@code HxprDocumentRetriever} has to say anything. A positive {@code applyDocumentDiversity=true}
     * would put the correct behaviour behind a default surviving Lombok's builder and Jackson's choice of
     * creator, and it did not: deserialised request objects arrived with it unset and the cap was silently
     * inert on both endpoints.</p>
     */
    @com.fasterxml.jackson.annotation.JsonIgnore
    private boolean skipDocumentDiversity;
}
