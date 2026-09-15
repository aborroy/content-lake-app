package org.hyland.contentlake.rag.service;

import org.hyland.contentlake.rag.model.HybridSearchRequest;
import org.hyland.contentlake.rag.model.SemanticSearchRequest;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Who the per-document cap applies to (#134).
 *
 * <p>The cap is on by default, and it is deliberately not applied to every retrieval. Measured on the
 * 78-question golden set with the local judge, capping the RAG pipeline's own retrieval raises document
 * recall ({@code recall@10} 0.8429 to 0.8714) and lowers answer quality at the same time
 * ({@code faithfulness} 0.806 to 0.762 at a cap of 2 and 0.710 at 3, {@code citation_accuracy} 0.808 to
 * 0.779 and 0.695, {@code unsupported_claim_rate} 0.194 to 0.238 and 0.290): a long document that is
 * itself the answer loses the chunks that supported it, and other documents' best chunks take the slots.
 * A caller browsing search results wants the opposite, because ten chunks of two documents is
 * indistinguishable from eight documents not being indexed.</p>
 *
 * <p>So the scope is a property of the request, and these are the assertions that keep it that way. The
 * flag is not part of the wire format, which is why it is asserted here rather than through a controller
 * test: a client must not be able to turn the generator's context capping back on.</p>
 */
class DocumentDiversityScopeTest {

    @Test
    void aSemanticRequestAppliesTheCapWhateverBuiltIt() {
        // The zero value is the endpoint behaviour, so no construction path can lose it. A positive flag
        // did lose it: request objects reached the service with it unset and the cap was inert on both
        // endpoints, which an A/B against one fixed index caught and no unit test would have.
        assertThat(SemanticSearchRequest.builder().query("q").build().isSkipDocumentDiversity()).isFalse();
        assertThat(new SemanticSearchRequest().isSkipDocumentDiversity()).isFalse();
        assertThat(new SemanticSearchRequest("q", 5, null, null, null, null, 0.0, false)
                .isSkipDocumentDiversity()).isFalse();
    }

    @Test
    void aHybridRequestAppliesTheCapWhateverBuiltIt() {
        assertThat(HybridSearchRequest.builder().query("q").build().isSkipDocumentDiversity()).isFalse();
        assertThat(new HybridSearchRequest().isSkipDocumentDiversity()).isFalse();
    }

    @Test
    void theFlagIsNotPartOfTheWireFormat() throws Exception {
        // A caller who could set this could re-impose the cap on the generator's context, which is the
        // one configuration the measurement rules out. @JsonIgnore is what prevents it, and a rename or
        // a Lombok change could silently drop that.
        var semantic = SemanticSearchRequest.class.getDeclaredField("skipDocumentDiversity");
        var hybrid = HybridSearchRequest.class.getDeclaredField("skipDocumentDiversity");

        assertThat(semantic.isAnnotationPresent(com.fasterxml.jackson.annotation.JsonIgnore.class)).isTrue();
        assertThat(hybrid.isAnnotationPresent(com.fasterxml.jackson.annotation.JsonIgnore.class)).isTrue();
    }

    @Test
    void jacksonDoesNotBindTheFlagFromARequestBody() throws Exception {
        var mapper = new com.fasterxml.jackson.databind.ObjectMapper();

        SemanticSearchRequest semantic = mapper.readValue(
                "{\"query\":\"q\",\"skipDocumentDiversity\":true}", SemanticSearchRequest.class);
        HybridSearchRequest hybrid = mapper.readValue(
                "{\"query\":\"q\",\"skipDocumentDiversity\":true}", HybridSearchRequest.class);

        // A caller must not be able to take the generator's uncapped path for itself, nor to turn the cap
        // back on for the generator.
        assertThat(semantic.isSkipDocumentDiversity()).isFalse();
        assertThat(hybrid.isSkipDocumentDiversity()).isFalse();
    }

    @Test
    void theFlagIsNotSerialisedIntoAResponseOrEcho() throws Exception {
        var mapper = new com.fasterxml.jackson.databind.ObjectMapper();

        assertThat(mapper.writeValueAsString(SemanticSearchRequest.builder().query("q").build()))
                .doesNotContain("skipDocumentDiversity");
        assertThat(mapper.writeValueAsString(HybridSearchRequest.builder().query("q").build()))
                .doesNotContain("skipDocumentDiversity");
    }
}
