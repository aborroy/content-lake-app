package org.hyland.alfresco.contentlake.client;

import org.alfresco.core.handler.NodesApi;
import org.alfresco.core.model.NodeChildAssociation;
import org.alfresco.core.model.NodeChildAssociationEntry;
import org.alfresco.core.model.NodeChildAssociationPaging;
import org.alfresco.core.model.NodeChildAssociationPagingList;
import org.alfresco.core.model.Pagination;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.ResponseEntity;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * How {@code getAllChildren} decides a folder is exhausted (#136).
 *
 * <p>Not on whether the page came back full: the repository's own {@code hasMoreItems} is the answer, so a
 * page that arrives short for any reason does not end the folder.</p>
 */
@ExtendWith(MockitoExtension.class)
class AlfrescoClientChildPagingTest {

    private static final int PAGE_SIZE = 100;

    @Mock
    private NodesApi nodesApi;

    private AlfrescoClient client;

    @BeforeEach
    void setUp() {
        client = new AlfrescoClient(nodesApi, null);
    }

    @Test
    void getAllChildren_pagesUntilTheRepositoryReportsNoMoreItems() {
        stubPage(0, page(names(0, PAGE_SIZE), true));
        stubPage(PAGE_SIZE, page(names(PAGE_SIZE, PAGE_SIZE + 30), false));

        assertThat(client.getAllChildren("folder-1")).hasSize(PAGE_SIZE + 30);
        verify(nodesApi).listNodeChildren(eq("folder-1"), eq(PAGE_SIZE), eq(PAGE_SIZE),
                isNull(), isNull(), any(), isNull(), isNull(), isNull());
    }

    /**
     * The case the page-size heuristic gets wrong. A page shorter than what was asked for is allowed, and
     * stopping there would drop the rest of the folder while the pass still reported itself complete.
     */
    @Test
    void getAllChildren_keepsPagingPastAShortPageTheRepositorySaysHasMore() {
        stubPage(0, page(names(0, 3), true));
        stubPage(PAGE_SIZE, page(names(3, 5), false));

        assertThat(client.getAllChildren("folder-1")).hasSize(5);
    }

    /** The cursor advances by what was asked for, so a short page cannot shift the next window. */
    @Test
    void getAllChildren_advancesTheCursorByThePageSize() {
        stubPage(0, page(names(0, 3), true));
        stubPage(PAGE_SIZE, page(names(3, 5), false));

        client.getAllChildren("folder-1");

        verify(nodesApi).listNodeChildren(eq("folder-1"), eq(PAGE_SIZE), eq(PAGE_SIZE),
                isNull(), isNull(), any(), isNull(), isNull(), isNull());
    }

    /**
     * A response with no pagination block is an older or unexpected shape. Falling back to the page-size
     * heuristic keeps a full page from ending the folder; stopping outright would truncate it.
     */
    @Test
    void getAllChildren_fallsBackToThePageSizeHeuristicWhenPaginationIsAbsent() {
        stubPage(0, pageWithoutPagination(names(0, PAGE_SIZE)));
        stubPage(PAGE_SIZE, pageWithoutPagination(names(PAGE_SIZE, PAGE_SIZE + 4)));

        assertThat(client.getAllChildren("folder-1")).hasSize(PAGE_SIZE + 4);
    }

    @Test
    void getAllChildren_handlesAnEmptyFolder() {
        stubPage(0, page(List.of(), false));

        assertThat(client.getAllChildren("folder-1")).isEmpty();
    }

    /** A body without a list is not a folder full of children, and must not be paged forever. */
    @Test
    void getAllChildren_treatsAnEmptyBodyAsExhausted() {
        when(nodesApi.listNodeChildren(eq("folder-1"), eq(0), eq(PAGE_SIZE),
                isNull(), isNull(), any(), isNull(), isNull(), isNull()))
                .thenReturn(ResponseEntity.ok(new NodeChildAssociationPaging()));

        assertThat(client.getAllChildren("folder-1")).isEmpty();
    }

    private void stubPage(int skipCount, NodeChildAssociationPaging response) {
        when(nodesApi.listNodeChildren(eq("folder-1"), eq(skipCount), eq(PAGE_SIZE),
                isNull(), isNull(), any(), isNull(), isNull(), isNull()))
                .thenReturn(ResponseEntity.ok(response));
    }

    private static List<String> names(int fromInclusive, int toExclusive) {
        List<String> ids = new ArrayList<>();
        for (int i = fromInclusive; i < toExclusive; i++) {
            ids.add("child-" + i);
        }
        return ids;
    }

    private static NodeChildAssociationPaging page(List<String> ids, boolean hasMore) {
        return new NodeChildAssociationPaging()._list(list(ids)
                .pagination(new Pagination().hasMoreItems(hasMore)));
    }

    private static NodeChildAssociationPaging pageWithoutPagination(List<String> ids) {
        return new NodeChildAssociationPaging()._list(list(ids));
    }

    private static NodeChildAssociationPagingList list(List<String> ids) {
        List<NodeChildAssociationEntry> entries = new ArrayList<>();
        for (String id : ids) {
            NodeChildAssociation child = new NodeChildAssociation();
            child.setId(id);
            child.setName(id);
            entries.add(new NodeChildAssociationEntry().entry(child));
        }
        return new NodeChildAssociationPagingList().entries(entries);
    }
}
