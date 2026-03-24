package org.alfresco.contentlake.client;

import org.alfresco.contentlake.hxpr.api.model.VectorQuery;
import org.alfresco.contentlake.hxpr.api.model.VectorSearchResult;
import org.alfresco.contentlake.model.HxprDocument;
import org.alfresco.contentlake.hxpr.api.model.Query;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.service.annotation.HttpExchange;
import org.springframework.web.service.annotation.PostExchange;

/**
 * Spring HTTP Interface for the HXPR Query REST API.
 * <p>
 * Uses generated {@link Query} and {@link VectorQuery} models for request
 * bodies, ensuring type-safe query construction aligned with the OpenAPI spec.
 */
@HttpExchange("/api/query")
public interface HxprQueryApi {

    @PostExchange
    HxprDocument.QueryResult query(@RequestBody Query query);

    @PostExchange("/embeddings")
    VectorSearchResult vectorSearch(@RequestBody VectorQuery query);
}
