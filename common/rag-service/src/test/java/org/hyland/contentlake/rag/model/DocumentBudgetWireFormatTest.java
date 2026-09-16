package org.hyland.contentlake.rag.model;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The wire format of the document budget (#135).
 *
 * <p>The whole resolution rests on absence being expressible: a request that says nothing about documents
 * must behave exactly as before, and it can only do that if the server can tell "no {@code topDocuments}"
 * from "{@code topDocuments} of zero". Lombok's builder defaults and Jackson's choice of creator have
 * already broken that distinction once on these two classes, for {@code skipDocumentDiversity}, so the
 * distinction is asserted rather than assumed.</p>
 */
class DocumentBudgetWireFormatTest {

    private final ObjectMapper mapper = new ObjectMapper();

    @Nested
    class Requests {

        /** Absence is the default however the request was built, so no construction path can lose it. */
        @Test
        void aRequestThatSaysNothingAboutDocumentsHasNoBudget() {
            assertThat(SemanticSearchRequest.builder().query("q").build().getTopDocuments()).isNull();
            assertThat(new SemanticSearchRequest().getTopDocuments()).isNull();
            assertThat(SemanticSearchRequest.builder().query("q").build().getChunksPerDocument()).isNull();

            assertThat(HybridSearchRequest.builder().query("q").build().getTopDocuments()).isNull();
            assertThat(new HybridSearchRequest().getTopDocuments()).isNull();
            assertThat(HybridSearchRequest.builder().query("q").build().getChunksPerDocument()).isNull();
        }

        /** @JsonInclude(NON_NULL) keeps the request shape unchanged for every existing caller. */
        @Test
        void anAbsentBudgetDoesNotAppearInTheSerialisedRequest() throws Exception {
            assertThat(mapper.writeValueAsString(SemanticSearchRequest.builder().query("q").build()))
                    .doesNotContain("topDocuments")
                    .doesNotContain("chunksPerDocument");
            assertThat(mapper.writeValueAsString(HybridSearchRequest.builder().query("q").build()))
                    .doesNotContain("topDocuments")
                    .doesNotContain("chunksPerDocument");
        }

        @Test
        void aSuppliedBudgetBindsFromARequestBody() throws Exception {
            SemanticSearchRequest semantic = mapper.readValue(
                    "{\"query\":\"q\",\"topDocuments\":10,\"chunksPerDocument\":3}",
                    SemanticSearchRequest.class);
            HybridSearchRequest hybrid = mapper.readValue(
                    "{\"query\":\"q\",\"topDocuments\":10,\"chunksPerDocument\":3}",
                    HybridSearchRequest.class);

            assertThat(semantic.getTopDocuments()).isEqualTo(10);
            assertThat(semantic.getChunksPerDocument()).isEqualTo(3);
            assertThat(hybrid.getTopDocuments()).isEqualTo(10);
            assertThat(hybrid.getChunksPerDocument()).isEqualTo(3);
        }

        /**
         * The distinction the nullable type exists for. A primitive would make these two requests
         * indistinguishable, and since the controller rejects a zero budget, the second one would then be
         * rejected on behalf of every caller that never mentioned documents at all.
         */
        @Test
        void zeroIsDistinguishableFromUnsupplied() throws Exception {
            assertThat(mapper.readValue("{\"query\":\"q\",\"topDocuments\":0}", SemanticSearchRequest.class)
                    .getTopDocuments()).isZero();
            assertThat(mapper.readValue("{\"query\":\"q\"}", SemanticSearchRequest.class)
                    .getTopDocuments()).isNull();

            assertThat(mapper.readValue("{\"query\":\"q\",\"topDocuments\":0}", HybridSearchRequest.class)
                    .getTopDocuments()).isZero();
            assertThat(mapper.readValue("{\"query\":\"q\"}", HybridSearchRequest.class)
                    .getTopDocuments()).isNull();
        }

        /** topK keeps its own default and its own meaning: the two budgets do not interact on the wire. */
        @Test
        void aDocumentBudgetDoesNotDisturbTheChunkBudget() throws Exception {
            SemanticSearchRequest semantic = mapper.readValue(
                    "{\"query\":\"q\",\"topDocuments\":20}", SemanticSearchRequest.class);

            assertThat(semantic.getTopK()).isEqualTo(5);
            assertThat(semantic.getTopDocuments()).isEqualTo(20);
        }
    }

    @Nested
    class Responses {

        @Test
        void theAppliedBudgetRoundTrips() throws Exception {
            String json = mapper.writeValueAsString(SemanticSearchResponse.builder()
                    .query("q")
                    .resultCount(7)
                    .documentCount(4)
                    .appliedTopDocuments(10)
                    .appliedChunksPerDocument(2)
                    .build());

            SemanticSearchResponse parsed = mapper.readValue(json, SemanticSearchResponse.class);

            assertThat(parsed.getDocumentCount()).isEqualTo(4);
            assertThat(parsed.getAppliedTopDocuments()).isEqualTo(10);
            assertThat(parsed.getAppliedChunksPerDocument()).isEqualTo(2);
        }

        @Test
        void theHybridAppliedBudgetRoundTrips() throws Exception {
            String json = mapper.writeValueAsString(HybridSearchResponse.builder()
                    .query("q")
                    .resultCount(7)
                    .documentCount(4)
                    .appliedTopDocuments(10)
                    .appliedChunksPerDocument(2)
                    .build());

            HybridSearchResponse parsed = mapper.readValue(json, HybridSearchResponse.class);

            assertThat(parsed.getDocumentCount()).isEqualTo(4);
            assertThat(parsed.getAppliedTopDocuments()).isEqualTo(10);
            assertThat(parsed.getAppliedChunksPerDocument()).isEqualTo(2);
        }

        /**
         * A chunk-oriented response keeps its shape: the applied budget is absent because none was asked
         * for, while {@code documentCount} is reported unconditionally because it is what such a caller
         * otherwise recomputes by grouping the hits itself.
         */
        @Test
        void anUnrequestedBudgetIsAbsentButDocumentCountIsNot() throws Exception {
            String json = mapper.writeValueAsString(SemanticSearchResponse.builder()
                    .query("q")
                    .resultCount(3)
                    .documentCount(2)
                    .build());

            assertThat(json)
                    .doesNotContain("appliedTopDocuments")
                    .doesNotContain("appliedChunksPerDocument")
                    .contains("\"documentCount\":2");
        }

        /** A response built without any of the three is still the pre-change shape. */
        @Test
        void aResponseCarryingNoneOfThemSerialisesAsBefore() throws Exception {
            assertThat(mapper.writeValueAsString(
                    SemanticSearchResponse.builder().query("q").resultCount(0).build()))
                    .doesNotContain("documentCount")
                    .doesNotContain("appliedTopDocuments")
                    .doesNotContain("appliedChunksPerDocument");
        }
    }
}
