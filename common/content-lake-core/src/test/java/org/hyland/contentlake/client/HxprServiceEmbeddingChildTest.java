package org.hyland.contentlake.client;

import org.hyland.contentlake.hxpr.api.model.Query;
import org.hyland.contentlake.model.HxprDocument;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.InOrder;
import org.springframework.web.client.RestClient;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Regression coverage for two issues in the same code.
 *
 * <p>#80: re-syncing a node duplicated its embeddings because the embedding-child lookup ran with
 * the default {@code limit=0} (which hxpr treats as "return no rows"), so the stale child was never
 * found or deleted before a new one was created.</p>
 *
 * <p>#113: the embedding type was a hardcoded constant rather than derived from the configured
 * model, so changing the model left children the clear path could no longer name. They survived a
 * re-sync and kept answering queries through the {@code *} wildcard the read path substitutes.</p>
 */
@ExtendWith(MockitoExtension.class)
class HxprServiceEmbeddingChildTest {

    private static final String DOC_ID = "parent-doc-id";
    private static final String EMBEDDING_TYPE = "ai-mxbai-embed-large";
    private static final String CHILD_NAME = "_e_" + EMBEDDING_TYPE;

    /** The name written before the type was derived from the configured model (#113). */
    private static final String LEGACY_CHILD_NAME = "_e_mxbai-embed-large";

    @Mock
    private HxprDocumentApi documentApi;
    @Mock
    private HxprQueryApi queryApi;
    @Mock
    private RestClient restClient;

    @Test
    void deleteEmbeddings_queriesWithPositiveLimit_soChildrenAreActuallyFound() {
        HxprService service = service();
        when(queryApi.query(any(Query.class))).thenReturn(emptyResult());
        when(documentApi.getById(DOC_ID)).thenReturn(new HxprDocument());

        service.deleteEmbeddings(DOC_ID);

        ArgumentCaptor<Query> queryCaptor = ArgumentCaptor.forClass(Query.class);
        verify(queryApi).query(queryCaptor.capture());
        Query issued = queryCaptor.getValue();
        // The limit=0 default is the root cause of #80; the child lookup must ask for rows.
        assertThat(issued.getLimit()).isGreaterThan(0L);
        assertThat(issued.getQuery()).contains("sys_parentId = '" + DOC_ID + "'");
    }

    @Test
    void deleteEmbeddings_deletesEveryMatchingChild_notJustTheFirst() {
        HxprService service = service();
        // hxpr auto-suffixes duplicate child names; a re-sync leaves the base child plus a
        // suffixed sibling. All of them must be removed, not only results.get(0).
        when(queryApi.query(any(Query.class))).thenReturn(
                result(child("child-1", CHILD_NAME), child("child-2", CHILD_NAME + ".123456789")));
        when(documentApi.getById(DOC_ID)).thenReturn(new HxprDocument());

        service.deleteEmbeddings(DOC_ID);

        verify(documentApi).deleteById("child-1");
        verify(documentApi).deleteById("child-2");
    }

    @Test
    void deleteEmbeddings_waitsForIndexBeforeQuerying_soRecentChildrenAreVisible() {
        HxprService service = service();
        when(queryApi.query(any(Query.class))).thenReturn(emptyResult());
        when(documentApi.getById(DOC_ID)).thenReturn(new HxprDocument());

        service.deleteEmbeddings(DOC_ID);

        // The index wait must happen BEFORE the lookup, otherwise a child created on a prior
        // sync may not yet be visible and a duplicate would be created on re-sync (#80).
        InOrder inOrder = inOrder(queryApi);
        inOrder.verify(queryApi).waitForFullTextSearchIndexing(anyBoolean(), anyInt());
        inOrder.verify(queryApi).query(any(Query.class));
    }

    @Test
    void deleteEmbeddings_clearsChildrenOfEveryType_notOnlyTheConfiguredOne() {
        HxprService service = service();
        // The orphan condition of #113: a document carrying children from a previously configured
        // model. Naming only the current type would leave them behind, still searchable.
        when(queryApi.query(any(Query.class))).thenReturn(result(
                child("current", CHILD_NAME),
                child("retired", "_e_nomic-embed-text")));
        when(documentApi.getById(DOC_ID)).thenReturn(new HxprDocument());

        service.deleteEmbeddings(DOC_ID);

        verify(documentApi).deleteById("current");
        verify(documentApi).deleteById("retired");
    }

    @Test
    void deleteEmbeddings_clearsAChildCarryingTheLiteralWrittenBeforeTheTypeWasDerived() {
        HxprService service = service();
        when(queryApi.query(any(Query.class))).thenReturn(result(child("legacy", LEGACY_CHILD_NAME)));
        when(documentApi.getById(DOC_ID)).thenReturn(new HxprDocument());

        service.deleteEmbeddings(DOC_ID);

        verify(documentApi).deleteById("legacy");
    }

    @Test
    void listEmbeddingChildren_ignoresChildrenThatAreNotEmbeddings() {
        HxprService service = service();
        // The lookup selects by sys_parentId alone and filters on the _e_ prefix in Java, because
        // '_' is a single-character wildcard in HXQL LIKE and escapeLiteral does not escape it, so
        // LIKE '_e_%' would match any three-characters-then-anything name.
        when(queryApi.query(any(Query.class))).thenReturn(result(
                child("emb", CHILD_NAME),
                child("rendition", "abc-rendition"),
                child("other", "notes.txt")));

        List<HxprService.EmbeddingChild> children = service.listEmbeddingChildren(DOC_ID);

        assertThat(children).singleElement().satisfies(c -> {
            assertThat(c.sysId()).isEqualTo("emb");
            assertThat(c.embeddingType()).isEqualTo(EMBEDDING_TYPE);
        });
    }

    @Test
    void listEmbeddingChildren_recoversTheTypeFromTheChildName() {
        HxprService service = service();
        when(queryApi.query(any(Query.class))).thenReturn(result(
                child("a", CHILD_NAME),
                child("b", "_e_nomic-embed-text")));

        assertThat(service.listEmbeddingChildren(DOC_ID))
                .extracting(HxprService.EmbeddingChild::embeddingType)
                .containsExactly(EMBEDDING_TYPE, "nomic-embed-text");
    }

    @Test
    void listEmbeddingChildren_doesNotQueryByLikeOnTheUnderscorePrefix() {
        HxprService service = service();
        when(queryApi.query(any(Query.class))).thenReturn(emptyResult());

        service.listEmbeddingChildren(DOC_ID);

        ArgumentCaptor<Query> queryCaptor = ArgumentCaptor.forClass(Query.class);
        verify(queryApi).query(queryCaptor.capture());
        assertThat(queryCaptor.getValue().getQuery()).doesNotContain("LIKE");
    }

    @Test
    void deleteEmbeddings_leavesNonEmbeddingChildrenAlone() {
        HxprService service = service();
        when(queryApi.query(any(Query.class))).thenReturn(result(child("other", "notes.txt")));
        when(documentApi.getById(DOC_ID)).thenReturn(new HxprDocument());

        service.deleteEmbeddings(DOC_ID);

        verify(documentApi, never()).deleteById("other");
    }

    @Test
    void getEmbeddingType_reportsTheConfiguredType_soWritersAndReadersCanAgree() {
        assertThat(service().getEmbeddingType()).isEqualTo(EMBEDDING_TYPE);
    }

    // ---------------------------------------------------------------
    // Which children a type-narrowed delete may remove (#121)
    // ---------------------------------------------------------------

    @Test
    void aNarrowedDeleteRemovesTheExactTypeItNames() {
        assertThat(HxprService.isDeletable(EMBEDDING_TYPE, EMBEDDING_TYPE)).isTrue();
    }

    @Test
    void aNullTypeRemovesEverything_whichIsWhatClearingADocumentMeans() {
        assertThat(HxprService.isDeletable(EMBEDDING_TYPE, null)).isTrue();
        assertThat(HxprService.isDeletable("ai-nomic-embed-text", null)).isTrue();
    }

    /**
     * The defect this replaced. A prefix match deleted a sibling type whose name merely extends the one
     * being written, so a sync under one model destroyed another model's vectors while reporting
     * success. Coexisting types are the entire point of multi-type retrieval.
     */
    @Test
    void aNarrowedDeleteLeavesASiblingTypeThatMerelyExtendsTheName() {
        assertThat(HxprService.isDeletable("ai-mxbai-embed-large-v2", EMBEDDING_TYPE)).isFalse();
        assertThat(HxprService.isDeletable("ai-mxbai-embed-large-latest", EMBEDDING_TYPE)).isFalse();
    }

    /**
     * Why no prefix rule is safe, not even one requiring a separator: real model versions prefix one
     * another, and the derivation keeps the dot.
     */
    @Test
    void aNarrowedDeleteLeavesAPointVersionOfTheTypeItNames() {
        assertThat(HxprService.isDeletable("nomic-embed-text-v1.5", "nomic-embed-text-v1")).isFalse();
    }

    /**
     * A child hxpr auto-suffixed for a name collision is no longer in scope either. It cannot be created
     * any more, since the child create has enforced sys_name uniqueness since #80 and 409s instead, and
     * one left by an older index is removed by clearing the document rather than by inferring ownership
     * from a name. That inference is what deleted live data.
     */
    @Test
    void aNarrowedDeleteLeavesAnAutoSuffixedChildToTheTypeAgnosticClearPath() {
        assertThat(HxprService.isDeletable(EMBEDDING_TYPE + ".123456789", EMBEDDING_TYPE)).isFalse();
        assertThat(HxprService.isDeletable(EMBEDDING_TYPE + ".123456789", null))
                .as("still removed when the whole document is cleared")
                .isTrue();
    }

    @Test
    void aNarrowedDeleteLeavesAnUnrelatedType() {
        assertThat(HxprService.isDeletable("ai-nomic-embed-text", EMBEDDING_TYPE)).isFalse();
    }

    private HxprService service() {
        return new HxprService(documentApi, queryApi, restClient, EMBEDDING_TYPE);
    }

    private static HxprDocument child(String id, String name) {
        HxprDocument doc = new HxprDocument();
        doc.setSysId(id);
        doc.setSysName(name);
        return doc;
    }

    private static HxprDocument.QueryResult emptyResult() {
        return result();
    }

    private static HxprDocument.QueryResult result(HxprDocument... docs) {
        HxprDocument.QueryResult qr = new HxprDocument.QueryResult();
        qr.setDocuments(List.of(docs));
        return qr;
    }
}
