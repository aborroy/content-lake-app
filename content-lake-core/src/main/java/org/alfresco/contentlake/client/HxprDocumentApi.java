package org.alfresco.contentlake.client;

import org.alfresco.contentlake.model.HxprDocument;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.service.annotation.*;

import java.util.List;
import java.util.Map;

/**
 * Spring HTTP Interface for the HXPR Document REST API.
 * <p>
 * Covers ID-based CRUD operations. Path-based operations (which require
 * unencoded slashes in URI path segments) are handled directly by
 * {@link HxprService} using the shared {@code RestClient}.
 * <p>
 * Auth and {@code HXCS-REPOSITORY} headers are injected automatically
 * by the interceptor configured on the underlying {@code RestClient}.
 */
@HttpExchange("/api/documents")
public interface HxprDocumentApi {

    @GetExchange("/{docId}")
    HxprDocument getById(@PathVariable String docId);

    @PutExchange("/{docId}")
    HxprDocument updateById(@PathVariable String docId, @RequestBody Object document);

    @PatchExchange(value = "/{docId}", contentType = "application/json-patch+json")
    HxprDocument patchById(@PathVariable String docId,
                            @RequestBody List<Map<String, Object>> jsonPatch);

    @DeleteExchange("/{docId}")
    void deleteById(@PathVariable String docId);
}
