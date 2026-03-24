package org.alfresco.contentlake.client;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.http.MediaType;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestClient;

import java.io.InputStream;
import java.util.Iterator;
import java.util.Map;

/**
 * Ensures HXPR repository model contains the required schemas/types/mixinTypes.
 *
 * <p>Idempotent:
 * <ul>
 *   <li>Reads current model from {@code GET /api/repository/model}</li>
 *   <li>Applies JSON Patch "add" ops for missing entries only</li>
 * </ul>
 *
 * <p>Designed for one-time application bootstrap, but safe to run multiple times.
 */
@Slf4j
@RequiredArgsConstructor
public class HxprModelProvisioner {

    private static final MediaType JSON_PATCH =
            MediaType.valueOf("application/json-patch+json");

    private final RestClient hxprRestClient;
    private final ResourceLoader resourceLoader;
    private final ObjectMapper objectMapper;

    /**
     * Ensures required model fragments exist in HXPR by applying an add-only JSON Patch.
     *
     * @param fragmentsClasspathLocation classpath location of model JSON, for example
     *                                  {@code classpath:model-fragments.json}
     */
    public void ensureModelPresent(String fragmentsClasspathLocation) {
        JsonNode desired = loadDesiredModel(fragmentsClasspathLocation);
        JsonNode current = fetchCurrentModel();

        ArrayNode patch = buildAddOnlyPatch(current, desired);
        if (patch.isEmpty()) {
            log.info("HXPR model already contains required fragments. No patch needed.");
            return;
        }

        log.warn("HXPR model missing {} entries. Applying JSON Patch to /api/repository/model.", patch.size());
        applyPatch(patch);

        // Optional sanity check
        JsonNode after = fetchCurrentModel();
        ArrayNode remaining = buildAddOnlyPatch(after, desired);
        if (!remaining.isEmpty()) {
            throw new IllegalStateException("HXPR model patch did not fully apply. Remaining ops: " + remaining.size());
        }

        log.info("HXPR model provisioned successfully.");
    }

    private JsonNode fetchCurrentModel() {
        try {
            return hxprRestClient.get()
                    .uri(uriBuilder -> uriBuilder.path("/api/repository/model").build())
                    .retrieve()
                    .body(JsonNode.class);
        } catch (HttpClientErrorException e) {
            throw new IllegalStateException("Failed to GET /api/repository/model from HXPR", e);
        }
    }

    private void applyPatch(ArrayNode patchOps) {
        try {
            hxprRestClient.patch()
                    .uri(uriBuilder -> uriBuilder
                            .path("/api/repository/model")
                            .queryParam("validateOnly", "false")
                            .build())
                    .contentType(JSON_PATCH)
                    .body(patchOps)
                    .retrieve()
                    .toBodilessEntity();
        } catch (HttpClientErrorException e) {
            // If you run multiple instances, a race may occur. Keep strict by default.
            // If HXPR returns 409 for "already applied", you can treat it as success here.
            throw new IllegalStateException("Failed to PATCH /api/repository/model in HXPR: " + e.getStatusCode(), e);
        }
    }

    private JsonNode loadDesiredModel(String location) {
        Resource res = resourceLoader.getResource(location);
        if (!res.exists()) {
            throw new IllegalStateException("Model fragments not found on classpath: " + location);
        }
        try (InputStream in = res.getInputStream()) {
            return objectMapper.readTree(in);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to read model fragments: " + location, e);
        }
    }

    private ArrayNode buildAddOnlyPatch(JsonNode current, JsonNode desired) {
        ArrayNode ops = objectMapper.createArrayNode();
        addMissingSectionEntries(ops, current, desired, "schemas");
        addMissingSectionEntries(ops, current, desired, "types");
        addMissingSectionEntries(ops, current, desired, "mixinTypes");
        return ops;
    }

    private void addMissingSectionEntries(ArrayNode ops, JsonNode current, JsonNode desired, String sectionName) {
        JsonNode desiredSection = desired.path(sectionName);
        if (!desiredSection.isObject()) {
            return;
        }

        JsonNode currentSection = current.path(sectionName);

        // Entire section missing: add whole object
        if (currentSection.isMissingNode() || currentSection.isNull()) {
            ops.add(patchAdd("/" + sectionName, desiredSection));
            return;
        }

        if (!currentSection.isObject()) {
            throw new IllegalStateException("HXPR model section " + sectionName + " is not an object");
        }

        Iterator<Map.Entry<String, JsonNode>> fields = desiredSection.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> e = fields.next();
            String key = e.getKey();
            if (!currentSection.has(key)) {
                ops.add(patchAdd("/" + sectionName + "/" + escapeJsonPointerToken(key), e.getValue()));
            }
        }
    }

    private ObjectNode patchAdd(String path, JsonNode value) {
        ObjectNode op = objectMapper.createObjectNode();
        op.put("op", "add");
        op.put("path", path);
        op.set("value", value);
        return op;
    }

    private String escapeJsonPointerToken(String token) {
        return token.replace("~", "~0").replace("/", "~1");
    }
}