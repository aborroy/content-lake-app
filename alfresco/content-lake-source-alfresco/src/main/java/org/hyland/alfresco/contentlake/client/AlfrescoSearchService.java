package org.hyland.alfresco.contentlake.client;

import lombok.extern.slf4j.Slf4j;
import org.alfresco.core.model.Node;
import org.alfresco.search.handler.SearchApi;
import org.alfresco.search.model.RequestFacetField;
import org.alfresco.search.model.RequestFacetFields;
import org.alfresco.search.model.RequestPagination;
import org.alfresco.search.model.RequestQuery;
import org.alfresco.search.model.ResultBuckets;
import org.alfresco.search.model.ResultBucketsBuckets;
import org.alfresco.search.model.ResultSetPaging;
import org.alfresco.search.model.ResultSetPagingList;
import org.alfresco.search.model.ResultSetRowEntry;
import org.alfresco.search.model.SearchRequest;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * Executes AFTS (Alfresco Full Text Search) queries via the Search REST API.
 *
 * <p>Replaces recursive {@code getAllChildren} traversal with single paginated
 * queries, reducing HTTP round-trips from O(folders) to O(pages).</p>
 */
@Slf4j
@Component
public class AlfrescoSearchService {

    private static final int PAGE_SIZE = 1000;

    private static final String SYNC_STATUS_FIELD =
            "@{http://www.alfresco.org/model/contentlake/1.0}syncStatusValue";

    private final SearchApi searchApi;
    private final AlfrescoClient alfrescoClient;
    private final Executor statusLookupExecutor;

    public AlfrescoSearchService(SearchApi searchApi,
                                 AlfrescoClient alfrescoClient,
                                 @Qualifier("statusLookupExecutor") Executor statusLookupExecutor) {
        this.searchApi = searchApi;
        this.alfrescoClient = alfrescoClient;
        this.statusLookupExecutor = statusLookupExecutor;
    }

    /**
     * Returns all in-scope descendant files under the given folder.
     *
     * <p>Uses a single paginated AFTS query to collect node IDs, then
     * parallel-fetches full {@link Node} objects (with permissions) from
     * the Nodes REST API. This replaces recursive {@code getAllChildren}
     * traversal.</p>
     *
     * <p>The query filters out nodes with {@code cl:excludeFromLake=true}
     * and any configured excluded aspects.</p>
     *
     * @param folderId       ancestor folder node identifier
     * @param excludedAspects aspect names to exclude (e.g. from config)
     * @return list of in-scope file nodes with properties and permissions populated
     */
    public List<Node> findDescendantFiles(String folderId, Collection<String> excludedAspects) {
        List<String> ids = findDescendantFileIds(folderId, excludedAspects);
        if (ids.isEmpty()) {
            return List.of();
        }
        return parallelFetchNodes(ids);
    }

    /**
     * Returns aggregate sync-status counts for all in-scope files under the folder.
     *
     * <p>Issues a single AFTS query with a facet on {@code cl:syncStatusValue}
     * to obtain INDEXED and FAILED counts. PENDING is derived as
     * {@code total - indexed - failed}.</p>
     *
     * @param folderId       ancestor folder node identifier
     * @param excludedAspects aspect names to exclude
     * @return counts record; returns zeros when folder has no in-scope files
     */
    public FolderStatusCounts getFolderStatusCounts(String folderId, Collection<String> excludedAspects) {
        String aftsQuery = buildDescendantFilesQuery(folderId, excludedAspects);
        log.debug("AFTS folder status counts query for {}: {}", folderId, aftsQuery);

        SearchRequest request = new SearchRequest()
                .query(new RequestQuery()
                        .language(RequestQuery.LanguageEnum.AFTS)
                        .query(aftsQuery))
                .paging(new RequestPagination().maxItems(1).skipCount(0))
                .facetFields(new RequestFacetFields().addFacetsItem(
                        new RequestFacetField().field(SYNC_STATUS_FIELD)));

        try {
            var response = searchApi.search(request);
            ResultSetPaging paging = response != null ? response.getBody() : null;
            if (paging == null || paging.getList() == null) {
                return new FolderStatusCounts(0, 0, 0);
            }

            ResultSetPagingList list = paging.getList();
            long total = list.getPagination() != null && list.getPagination().getTotalItems() != null
                    ? list.getPagination().getTotalItems()
                    : 0;

            long indexed = 0;
            long failed = 0;

            if (list.getContext() != null && list.getContext().getFacetsFields() != null) {
                for (ResultBuckets facetBuckets : list.getContext().getFacetsFields()) {
                    if (facetBuckets.getBuckets() == null) {
                        continue;
                    }
                    for (ResultBucketsBuckets bucket : facetBuckets.getBuckets()) {
                        if (bucket.getLabel() == null || bucket.getCount() == null) {
                            continue;
                        }
                        switch (bucket.getLabel()) {
                            case "INDEXED" -> indexed += bucket.getCount();
                            case "FAILED"  -> failed  += bucket.getCount();
                        }
                    }
                }
            }

            log.debug("AFTS status counts for folder {}: total={} indexed={} failed={}", folderId, total, indexed, failed);
            return new FolderStatusCounts(total, indexed, failed);

        } catch (Exception e) {
            log.warn("AFTS folder status counts failed for {}: {}", folderId, e.getMessage());
            return new FolderStatusCounts(0, 0, 0);
        }
    }

    /**
     * Returns {@code true} when any of the given ancestor folder IDs has the {@code cl:indexed} aspect.
     *
     * <p>Issues a single AFTS query with an {@code ID:} OR predicate over all ancestor IDs.
     * The caller extracts ancestor IDs from {@code node.getPath().getElements()}.</p>
     *
     * <p>TODO(part-2): Solr-commit race. This method races with the Solr commit of a
     * just-added/removed {@code cl:indexed} aspect. Callers reached during the same
     * event that mutated the aspect must avoid this method.</p>
     *
     * @param ancestorIds path element IDs from {@code node.getPath().getElements()}
     */
    public boolean hasIndexedAncestor(Collection<String> ancestorIds) {
        if (ancestorIds == null || ancestorIds.isEmpty()) {
            return false;
        }
        String idPredicate = buildIdOrPredicate(ancestorIds);
        return existsInSearchResult(
                "(" + idPredicate + ") AND ASPECT:\"cl:indexed\"",
                "ancestors", "hasIndexedAncestor");
    }

    /**
     * Returns {@code true} when any of the given ancestor folder IDs has {@code cl:excludeFromLake=true}.
     *
     * <p>TODO(part-2): Solr-commit race. Same caveat as {@link #hasIndexedAncestor}.</p>
     *
     * @param ancestorIds path element IDs from {@code node.getPath().getElements()}
     */
    public boolean hasExcludedAncestor(Collection<String> ancestorIds) {
        if (ancestorIds == null || ancestorIds.isEmpty()) {
            return false;
        }
        String idPredicate = buildIdOrPredicate(ancestorIds);
        return existsInSearchResult(
                "(" + idPredicate + ") AND @cl:excludeFromLake:true",
                "ancestors", "hasExcludedAncestor");
    }

    private static String buildIdOrPredicate(Collection<String> nodeIds) {
        StringBuilder sb = new StringBuilder();
        for (String id : nodeIds) {
            if (id == null || id.isBlank()) continue;
            if (!sb.isEmpty()) sb.append(" OR ");
            sb.append("ID:\"workspace://SpacesStore/").append(id).append("\"");
        }
        return sb.toString();
    }

    // ──────────────────────────────────────────────────────────────────────
    // Internal helpers
    // ──────────────────────────────────────────────────────────────────────

    private boolean existsInSearchResult(String aftsQuery, String nodeId, String context) {
        SearchRequest request = new SearchRequest()
                .query(new RequestQuery()
                        .language(RequestQuery.LanguageEnum.AFTS)
                        .query(aftsQuery))
                .paging(new RequestPagination().maxItems(1).skipCount(0));
        try {
            var response = searchApi.search(request);
            ResultSetPaging paging = response != null ? response.getBody() : null;
            if (paging == null || paging.getList() == null
                    || paging.getList().getPagination() == null) {
                return false;
            }
            Long total = paging.getList().getPagination().getTotalItems();
            return total != null && total > 0;
        } catch (Exception e) {
            log.warn("AFTS {} check failed for node {}: {}", context, nodeId, e.getMessage());
            return false;
        }
    }

    private List<String> findDescendantFileIds(String folderId, Collection<String> excludedAspects) {
        String aftsQuery = buildDescendantFilesQuery(folderId, excludedAspects);
        log.debug("AFTS descendant files query for {}: {}", folderId, aftsQuery);

        List<String> ids = new ArrayList<>();
        int skipCount = 0;

        while (true) {
            SearchRequest request = new SearchRequest()
                    .query(new RequestQuery()
                            .language(RequestQuery.LanguageEnum.AFTS)
                            .query(aftsQuery))
                    .paging(new RequestPagination().maxItems(PAGE_SIZE).skipCount(skipCount));

            try {
                var response = searchApi.search(request);
                ResultSetPaging paging = response != null ? response.getBody() : null;
                if (paging == null || paging.getList() == null
                        || paging.getList().getEntries() == null) {
                    break;
                }

                List<ResultSetRowEntry> entries = paging.getList().getEntries();
                for (ResultSetRowEntry entry : entries) {
                    if (entry.getEntry() != null && entry.getEntry().getId() != null) {
                        ids.add(entry.getEntry().getId());
                    }
                }

                Boolean hasMore = paging.getList().getPagination() != null
                        ? paging.getList().getPagination().isHasMoreItems()
                        : null;
                if (!Boolean.TRUE.equals(hasMore) || entries.isEmpty()) {
                    break;
                }
                skipCount += PAGE_SIZE;

            } catch (Exception e) {
                log.warn("AFTS descendant files query failed for {} at skipCount={}: {}", folderId, skipCount, e.getMessage());
                break;
            }
        }

        log.debug("AFTS found {} descendant files under {}", ids.size(), folderId);
        return ids;
    }

    private List<Node> parallelFetchNodes(List<String> nodeIds) {
        List<CompletableFuture<Node>> futures = nodeIds.stream()
                .map(id -> CompletableFuture.supplyAsync(
                        () -> alfrescoClient.getAlfrescoNode(id), statusLookupExecutor))
                .toList();

        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

        return futures.stream()
                .map(CompletableFuture::join)
                .filter(Objects::nonNull)
                .toList();
    }

    private static String buildDescendantFilesQuery(String folderId, Collection<String> excludedAspects) {
        StringBuilder q = new StringBuilder();
        q.append("ANCESTOR:\"workspace://SpacesStore/").append(folderId).append("\"");
        q.append(" AND TYPE:\"cm:content\"");
        q.append(" AND NOT @cl:excludeFromLake:true");
        if (excludedAspects != null) {
            for (String aspect : excludedAspects) {
                if (aspect != null && !aspect.isBlank()) {
                    q.append(" AND NOT ASPECT:\"").append(aspect).append("\"");
                }
            }
        }
        return q.toString();
    }
}
