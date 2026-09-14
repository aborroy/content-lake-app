package org.hyland.contentlake.client;

import org.hyland.contentlake.model.HxprDocument;
import org.hyland.contentlake.model.HxprEmbedding;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestClient;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * A re-sync must not make a document briefly unretrievable (#100).
 *
 * <p>Storing embeddings used to delete the child and then create a new one, so for the length of a Parquet
 * upload plus a create the document had no embeddings at all while still being indexed and reporting
 * INDEXED. Both ingesters sync most nodes, so that window was entered repeatedly, and two writers racing it
 * against one child can end with the child deleted and no create -- the "0 chunks until a second sync" state
 * the issue reports. Replacing the existing child's content has no such window.</p>
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class HxprServiceEmbeddingReplaceTest {

    private static final String DOC_ID = "parent-doc-id";
    private static final String EMBEDDING_TYPE = "ai-mxbai-embed-large";
    private static final String CHILD_NAME = "_e_" + EMBEDDING_TYPE;

    @Mock
    private HxprDocumentApi documentApi;
    @Mock
    private HxprQueryApi queryApi;
    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private RestClient restClient;

    // A deep stub returns one spec per method regardless of the argument, so every POST would otherwise
    // share a single stubbed outcome and a failure meant for the child create would be thrown by the
    // Parquet upload instead. One spec per endpoint keeps the three POSTs of a write distinguishable.
    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private RestClient.RequestBodySpec uploadSlotSpec;
    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private RestClient.RequestBodySpec uploadSpec;
    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private RestClient.RequestBodySpec createChildSpec;

    @SuppressWarnings("unchecked")
    private HxprService service() {
        when(restClient.post().uri(contains("/api/upload/create"))).thenReturn(uploadSlotSpec);
        when(uploadSlotSpec.retrieve().body(any(ParameterizedTypeReference.class)))
                .thenReturn(Map.of("id", "upload-1"));
        when(restClient.post().uri(contains("/api/upload?id="))).thenReturn(uploadSpec);
        when(restClient.post().uri(contains("enforceSysName=true"))).thenReturn(createChildSpec);
        return new HxprService(documentApi, queryApi, restClient, EMBEDDING_TYPE);
    }

    /** Stubs the child create, the only POST of a write that these tests make fail. */
    private org.mockito.stubbing.OngoingStubbing<org.springframework.http.ResponseEntity<Void>> whenChildCreated() {
        return when(createChildSpec.contentType(any()).body(any(Object.class)).retrieve().toBodilessEntity());
    }

    /** The point of the fix: the child that is serving queries is never removed. */
    @Test
    void anExistingChildIsUpdatedInPlaceRatherThanDeletedAndRecreated() {
        HxprService service = service();

        service.updateEmbeddings(DOC_ID, embeddings(),
                List.of(new HxprService.EmbeddingChild("child-1", CHILD_NAME, EMBEDDING_TYPE)));

        verify(documentApi, never()).deleteById(anyString());
        verify(restClient.put()).uri("/api/documents/child-1");
    }

    /** With nothing to replace, the create path is unchanged, including its sys_name enforcement. */
    @Test
    void aDocumentWithNoChildOfThisTypeStillGetsOneCreated() {
        HxprService service = service();

        service.updateEmbeddings(DOC_ID, embeddings(), List.of());

        verify(restClient.post()).uri(contains("/api/documents/" + DOC_ID + "?enforceSysName=true"));
        verify(documentApi, never()).deleteById(anyString());
    }

    /**
     * A child of another type belongs to another model and is not this method's business: only the clear
     * path removes those, and deleting one here would drop a model's vectors on an unrelated re-sync.
     */
    @Test
    void aChildOfAnotherEmbeddingTypeIsLeftAlone() {
        HxprService service = service();

        service.updateEmbeddings(DOC_ID, embeddings(),
                List.of(new HxprService.EmbeddingChild("legacy-1", "_e_other-model", "other-model")));

        verify(documentApi, never()).deleteById(anyString());
        verify(restClient.post()).uri(contains("enforceSysName=true"));
    }

    /**
     * Two children of one type should not exist. If they do, the extras go and the one just written stays,
     * so the invariant is restored without reopening the window this fix closes.
     */
    @Test
    void duplicateChildrenOfTheSameTypeAreRemovedExceptTheOneJustWritten() {
        HxprService service = service();

        service.updateEmbeddings(DOC_ID, embeddings(), List.of(
                new HxprService.EmbeddingChild("child-1", CHILD_NAME, EMBEDDING_TYPE),
                new HxprService.EmbeddingChild("child-2", CHILD_NAME, EMBEDDING_TYPE)));

        verify(restClient.put()).uri("/api/documents/child-1");
        verify(documentApi).deleteById("child-2");
        verify(documentApi, never()).deleteById("child-1");
    }

    private static List<HxprEmbedding> embeddings() {
        HxprEmbedding embedding = new HxprEmbedding();
        embedding.setVector(List.of(0.1d, 0.2d, 0.3d));
        embedding.setText("a chunk");
        embedding.setType(EMBEDDING_TYPE);
        return List.of(embedding);
    }

    /**
     * The 422 that made documents permanently unretrievable (#100): the parent lost the mixin to a
     * concurrent writer between the check and the create, so the create is refused. Retrying after adding
     * the mixin unconditionally is what turns that from a FAILED document into a synced one.
     */
    @Test
    void aCreateRefusedForALostParentMixinIsRetriedAfterReAddingIt() {
        HxprService service = service();
        when(documentApi.getById(DOC_ID)).thenReturn(new HxprDocument());
        whenChildCreated()
                .thenThrow(parentMissingMixin())
                .thenReturn(null);

        service.updateEmbeddings(DOC_ID, embeddings(), List.of());

        // Twice: the mixin check before the first create, then the unconditional add before the retry.
        // That the call returned at all is the other half of the assertion, since a create refused twice,
        // or one that was never retried, throws.
        verify(documentApi, org.mockito.Mockito.times(2)).updateById(eq(DOC_ID), any());
    }

    /** A 422 for anything else is not a race and must not be retried into a second failure. */
    @Test
    void anUnrelatedSchemaViolationIsNotRetried() {
        HxprService service = service();
        whenChildCreated()
                .thenThrow(HttpClientErrorException.create(HttpStatus.UNPROCESSABLE_CONTENT, "Unprocessable",
                        HttpHeaders.EMPTY, "{\"errors\":[{\"messageKey\":\"something.else\"}]}"
                                .getBytes(StandardCharsets.UTF_8), StandardCharsets.UTF_8));

        assertThatThrownBy(() -> service.updateEmbeddings(DOC_ID, embeddings(), List.of()))
                .isInstanceOf(RuntimeException.class);

        verify(documentApi, never()).updateById(anyString(), any());
    }

    /** The match is on the violation key, so a different 422 body is left alone. */
    @Test
    void theParentMissingMatchIsOnTheViolationKey() {
        assertThat(HxprService.isParentMissingEmbeddingMixin(parentMissingMixin())).isTrue();
        assertThat(HxprService.isParentMissingEmbeddingMixin(new RuntimeException("boom"))).isFalse();
        assertThat(HxprService.isParentMissingEmbeddingMixin(
                new RuntimeException("wrapped", parentMissingMixin()))).isTrue();
    }

    /**
     * The other direction of the same race (#100): the concurrent writer created the child this sync was
     * about to create, so the create is refused with a 409. The embeddings go into the child that now
     * exists, which is what the sync would have done had its child list been current. Abandoning the write
     * marked the document FAILED although it was indexed -- the false failure the issue reports.
     */
    @Test
    void aCreateRefusedForADuplicateChildNameWritesIntoTheExistingChild() {
        HxprService service = service();
        whenChildCreated()
                .thenThrow(duplicateChildName());
        indexReturnsChild("child-created-by-the-other-writer", CHILD_NAME);

        service.updateEmbeddings(DOC_ID, embeddings(), List.of());

        verify(restClient.put()).uri("/api/documents/child-created-by-the-other-writer");
        verify(documentApi, never()).deleteById(anyString());
    }

    /**
     * With the child still invisible to the index there is nowhere to put the write, so the sync fails and
     * a later one carries it. Silently reporting success would leave the other writer's embeddings standing
     * in for this sync's without anything recording that.
     */
    @Test
    void aDuplicateWhoseChildCannotBeListedFailsTheSync() {
        HxprService service = service();
        whenChildCreated()
                .thenThrow(duplicateChildName());
        when(queryApi.query(any())).thenReturn(new HxprDocument.QueryResult());

        assertThatThrownBy(() -> service.updateEmbeddings(DOC_ID, embeddings(), List.of()))
                .isInstanceOf(RuntimeException.class);

        verify(restClient, never()).put();
    }

    /**
     * Both halves of the race can hit one sync: the concurrent writer stripped the mixin, and by the time
     * the mixin is back and the create is retried it has also created the child.
     */
    @Test
    void aRetryAfterAReAddedMixinThatThenConflictsAlsoWritesIntoTheExistingChild() {
        HxprService service = service();
        when(documentApi.getById(DOC_ID)).thenReturn(new HxprDocument());
        whenChildCreated()
                .thenThrow(parentMissingMixin())
                .thenThrow(duplicateChildName());
        indexReturnsChild("child-1", CHILD_NAME);

        service.updateEmbeddings(DOC_ID, embeddings(), List.of());

        verify(restClient.put()).uri("/api/documents/child-1");
    }

    /** 409 is the only refusal {@code enforceSysName=true} produces, so the status is the whole match. */
    @Test
    void theDuplicateNameMatchIsOnTheConflictStatus() {
        assertThat(HxprService.isDuplicateChildName(duplicateChildName())).isTrue();
        assertThat(HxprService.isDuplicateChildName(
                new RuntimeException("wrapped", duplicateChildName()))).isTrue();
        assertThat(HxprService.isDuplicateChildName(parentMissingMixin())).isFalse();
        assertThat(HxprService.isDuplicateChildName(new RuntimeException("boom"))).isFalse();
    }

    /** Makes the child lookup that follows a conflict return one child of the given name. */
    private void indexReturnsChild(String sysId, String sysName) {
        HxprDocument child = new HxprDocument();
        child.setSysId(sysId);
        child.setSysName(sysName);
        HxprDocument.QueryResult result = new HxprDocument.QueryResult();
        result.setDocuments(List.of(child));
        when(queryApi.query(any())).thenReturn(result, new HxprDocument.QueryResult());
    }

    private static HttpClientErrorException duplicateChildName() {
        return HttpClientErrorException.create(HttpStatus.CONFLICT, "Conflict", HttpHeaders.EMPTY,
                ("{\"message\":\"Duplicate name in parent: " + CHILD_NAME + "\"}")
                        .getBytes(StandardCharsets.UTF_8), StandardCharsets.UTF_8);
    }

    private static HttpClientErrorException parentMissingMixin() {
        return HttpClientErrorException.create(HttpStatus.UNPROCESSABLE_CONTENT, "Unprocessable",
                HttpHeaders.EMPTY,
                ("{\"errors\":[{\"message\":\"Parent is missing mixin SysHasEmbeddings.\","
                        + "\"messageKey\":\"label.schema.application.violation.SysEmbeddingsViolation."
                        + "parentMissing\"}]}").getBytes(StandardCharsets.UTF_8), StandardCharsets.UTF_8);
    }
}
