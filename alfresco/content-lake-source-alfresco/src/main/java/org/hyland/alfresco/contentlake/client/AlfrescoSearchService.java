package org.hyland.alfresco.contentlake.client;

import lombok.extern.slf4j.Slf4j;
import org.alfresco.core.model.Node;
import org.alfresco.search.handler.SearchApi;
import org.alfresco.search.model.RequestPagination;
import org.alfresco.search.model.RequestQuery;
import org.alfresco.search.model.ResultSetPaging;
import org.alfresco.search.model.ResultSetRowEntry;
import org.alfresco.search.model.SearchRequest;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
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

    private static final String SYNC_STATUS_PROPERTY = "cl:syncStatusValue";
    private static final String EXCLUDE_FROM_LAKE_PROPERTY = "cl:excludeFromLake";

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
     * Like {@link #findDescendantFiles} but retries while the AFTS {@code ANCESTOR:} query
     * returns no files, to absorb search-index commit lag. The Solr backend committed
     * near-real-time; the OpenSearch batch indexer (ACS 26.2+) picks up new content on a
     * polling interval, so a descendant query issued in the same instant as the triggering
     * event (a folder scope/permission change, or freshly uploaded children) can momentarily
     * return empty. Returns as soon as a non-empty result appears, or the (empty) result after
     * the final attempt. See issues #78 and #88.
     *
     * @param maxAttempts     total attempts (clamped to a minimum of 1)
     * @param retryIntervalMs wait between attempts in milliseconds (skipped when {@code <= 0})
     */
    public List<Node> findDescendantFilesWithRetry(String folderId,
                                                   Collection<String> excludedAspects,
                                                   int maxAttempts,
                                                   long retryIntervalMs) {
        int attempts = Math.max(1, maxAttempts);
        List<Node> nodes = List.of();
        for (int attempt = 1; attempt <= attempts; attempt++) {
            nodes = findDescendantFiles(folderId, excludedAspects);
            if (!nodes.isEmpty()) {
                if (attempt > 1) {
                    log.info("Descendant discovery for folder {} returned {} file(s) on attempt {}/{}",
                            folderId, nodes.size(), attempt, attempts);
                }
                return nodes;
            }
            if (attempt < attempts) {
                log.info("Descendant discovery for folder {} found 0 file(s) (attempt {}/{}); retrying in {}ms",
                        folderId, attempt, attempts, retryIntervalMs);
                sleep(retryIntervalMs);
            }
        }
        log.debug("Descendant discovery for folder {} found 0 file(s) after {} attempt(s)", folderId, attempts);
        return nodes;
    }

    private static void sleep(long millis) {
        if (millis <= 0) {
            return;
        }
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Returns aggregate sync-status counts for all in-scope files under the folder.
     *
     * <p>Counts are derived from the {@code cl:syncStatusValue} property of each
     * descendant node as returned by the Nodes REST API, not from an AFTS facet on that
     * property: a facet only sees what the search engine indexed, and the ACS 26.2+
     * OpenSearch batch indexer silently drops properties whose namespace prefix is
     * missing from its static prefix map. A facet would then report every document as
     * PENDING while it is in fact indexed. PENDING is derived as
     * {@code total - indexed - failed - skipped}, so a document that has no status property yet
     * still counts as pending.</p>
     *
     * @param folderId        ancestor folder node identifier
     * @param excludedAspects aspect names to exclude
     * @return counts record; returns zeros when folder has no in-scope files
     */
    public FolderStatusCounts getFolderStatusCounts(String folderId, Collection<String> excludedAspects) {
        long total = 0;
        long indexed = 0;
        long failed = 0;
        long skipped = 0;

        for (Node node : findDescendantFiles(folderId, excludedAspects)) {
            // The AFTS query already asks for this, but the predicate is a no-op on a
            // search engine that did not index cl:excludeFromLake, so re-check here.
            if (isExcludedFromLake(node)) {
                continue;
            }
            total++;
            String status = readProperty(node, SYNC_STATUS_PROPERTY);
            if ("INDEXED".equals(status)) {
                indexed++;
            } else if ("FAILED".equals(status)) {
                failed++;
            } else if ("SKIPPED".equals(status)) {
                skipped++;
            }
        }

        log.debug("Status counts for folder {}: total={} indexed={} failed={} skipped={}",
                folderId, total, indexed, failed, skipped);
        return new FolderStatusCounts(total, indexed, failed, skipped);
    }

    // ──────────────────────────────────────────────────────────────────────
    // Internal helpers
    // ──────────────────────────────────────────────────────────────────────

    private static boolean isExcludedFromLake(Node node) {
        return Boolean.parseBoolean(readProperty(node, EXCLUDE_FROM_LAKE_PROPERTY));
    }

    private static String readProperty(Node node, String propertyName) {
        if (node.getProperties() instanceof Map<?, ?> properties) {
            Object value = properties.get(propertyName);
            return value != null ? value.toString() : null;
        }
        return null;
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
