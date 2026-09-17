package org.hyland.contentlake.rag.mcp;

import lombok.extern.slf4j.Slf4j;
import org.hyland.contentlake.client.HxprService;
import org.hyland.contentlake.model.ContentLakeIngestProperties;
import org.hyland.contentlake.model.HxprDocument;
import org.hyland.contentlake.model.HxprTermsAggregationResult;
import org.hyland.contentlake.security.AclFilterBuilder;
import org.hyland.contentlake.rag.model.SemanticSearchRequest;
import org.hyland.contentlake.rag.model.SemanticSearchResponse;
import org.hyland.contentlake.rag.model.SemanticSearchResponse.SearchHit;
import org.hyland.contentlake.rag.service.SemanticSearchService;
import org.springframework.ai.tool.annotation.Tool;
import org.springframework.ai.tool.annotation.ToolParam;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * Exposes Content Lake search, retrieval, and listing capabilities as Model Context Protocol (MCP)
 * tools so external LLM agents can query hxpr content (#61).
 *
 * <p><strong>ACL model.</strong> The WebMVC MCP transport is synchronous: each tool executes on the
 * authenticated HTTP request thread, so the {@code SecurityContext} populated by the security filter
 * chain is in scope. Tools therefore take only <em>search</em> parameters and never a caller-supplied
 * principal - identity is derived from the authenticated request exactly as the REST search path does,
 * so an agent cannot spoof another user. The MCP endpoint is not in the permit-all list, so it is
 * reachable only with HTTP Basic or Alfresco-ticket credentials (compatible with the official
 * Alfresco MCP Server client model).</p>
 */
@Slf4j
@Service
@ConditionalOnProperty(prefix = "rag.mcp", name = "enabled", havingValue = "true", matchIfMissing = true)
public class ContentLakeMcpServer {

    private static final int DEFAULT_TOP_K = 5;
    private static final int MAX_SOURCE_BUCKETS = 50;
    private static final int MAX_DOCUMENT_TEXT_CHARS = 4000;

    private final SemanticSearchService semanticSearchService;
    private final HxprService hxprService;

    /**
     * Dependencies are injected {@code @Lazy} to break a startup circular dependency: this bean is
     * exposed as a {@code ToolCallbackProvider}, which Spring AI's tool-calling autoconfiguration
     * collects to build the {@code ChatModel} - but {@link SemanticSearchService} transitively depends
     * on that same {@code ChatModel} (via query expansion). Lazy proxies defer resolution until a tool
     * is actually invoked, by which point the context is fully initialized.
     */
    public ContentLakeMcpServer(@Lazy SemanticSearchService semanticSearchService,
                                @Lazy HxprService hxprService) {
        this.semanticSearchService = semanticSearchService;
        this.hxprService = hxprService;
    }

    @Tool(name = "secureSearch", description = "ACL-filtered semantic search over Content Lake. Returns "
            + "the top document chunks the authenticated user is permitted to read.")
    public String secureSearch(
            @ToolParam(description = "The natural-language search query") String query,
            @ToolParam(description = "Maximum results to return (default 5)", required = false) Integer topK,
            @ToolParam(description = "Optional HXQL filter to scope the search", required = false) String filter,
            @ToolParam(description = "Optional source type filter: any ingested source type, such as alfresco, nuxeo, filesystem or a connector plugin type", required = false)
            String sourceType) {
        SemanticSearchRequest request = SemanticSearchRequest.builder()
                .query(query)
                .topK(topK != null && topK > 0 ? topK : DEFAULT_TOP_K)
                .filter(blankToNull(filter))
                .sourceType(blankToNull(sourceType))
                .build();
        SemanticSearchResponse response = semanticSearchService.search(request);
        List<SearchHit> hits = response.getResults();
        log.info("MCP secureSearch(query=\"{}\") -> {} hits", query, hits != null ? hits.size() : 0);
        return formatHits(hits);
    }

    @Tool(name = "getDocument", description = "Fetch the stored text and metadata for a single document "
            + "by its id. Returns the document only if the authenticated user is permitted to read it.")
    public String getDocument(@ToolParam(description = "The document id (cin_id) to fetch") String documentId) {
        String escaped = AclFilterBuilder.escapeLiteral(documentId);
        // ACL-safe: AND the id predicate onto the current user's permission filter; never a raw query.
        String hxql = semanticSearchService.currentUserPermissionFilter(null, "cin_id = '" + escaped + "'");
        HxprDocument.QueryResult result = hxprService.query(hxql, 1, 0);
        int matches = result != null && result.getDocuments() != null ? result.getDocuments().size() : 0;
        log.info("MCP getDocument(id={}) -> {} accessible match(es)", documentId, matches);
        if (matches == 0) {
            return "No accessible document found with id " + documentId + ".";
        }
        return formatDocument(result.getDocuments().get(0));
    }

    @Tool(name = "listSources", description = "List the distinct sources (repositories) the authenticated "
            + "user can retrieve documents from, with the document count for each.")
    public String listSources() {
        String hxql = semanticSearchService.currentUserPermissionFilter(null, null);
        HxprTermsAggregationResult agg =
                hxprService.termsAggregation(hxql, "cin_sourceId", null, MAX_SOURCE_BUCKETS);
        if (agg == null || agg.getAggregationsBuckets() == null || agg.getAggregationsBuckets().isEmpty()) {
            return "No accessible sources found.";
        }
        StringBuilder sb = new StringBuilder("Accessible sources (id: document count):\n");
        for (HxprTermsAggregationResult.Bucket bucket : agg.getAggregationsBuckets()) {
            sb.append("- ").append(bucket.getKey()).append(": ").append(bucket.getDocCount()).append('\n');
        }
        return sb.toString().trim();
    }

    private static String formatHits(List<SearchHit> hits) {
        if (hits == null || hits.isEmpty()) {
            return "No matching documents.";
        }
        StringBuilder sb = new StringBuilder();
        int i = 1;
        for (SearchHit hit : hits) {
            String name = hit.getSourceDocument() != null && hit.getSourceDocument().getName() != null
                    ? hit.getSourceDocument().getName()
                    : "Unknown document";
            String docId = hit.getSourceDocument() != null ? hit.getSourceDocument().getDocumentId() : null;
            sb.append("[Result ").append(i++).append(": ").append(name);
            if (docId != null) {
                sb.append(" (id: ").append(docId).append(')');
            }
            sb.append("]\n").append(hit.getChunkText() != null ? hit.getChunkText() : "").append("\n\n");
        }
        return sb.toString().trim();
    }

    private static String formatDocument(HxprDocument doc) {
        StringBuilder sb = new StringBuilder();
        sb.append("Name: ").append(doc.getSysName()).append('\n');
        sb.append("Source: ").append(doc.getCinSourceId()).append('\n');
        Object text = doc.getCinIngestProperties() != null
                ? doc.getCinIngestProperties().get(ContentLakeIngestProperties.CONTENT_LAKE_EXTRACTED_TEXT)
                : null;
        if (text != null) {
            String s = text.toString();
            if (s.length() > MAX_DOCUMENT_TEXT_CHARS) {
                s = s.substring(0, MAX_DOCUMENT_TEXT_CHARS) + "\n... (truncated)";
            }
            sb.append("Text:\n").append(s);
        } else {
            sb.append("Text: (no extracted text available)");
        }
        return sb.toString();
    }

    private static String blankToNull(String value) {
        return value == null || value.isBlank() ? null : value;
    }
}
