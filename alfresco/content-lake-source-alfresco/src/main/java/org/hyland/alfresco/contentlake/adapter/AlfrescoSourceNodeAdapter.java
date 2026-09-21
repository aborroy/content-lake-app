package org.hyland.alfresco.contentlake.adapter;

import org.hyland.contentlake.model.ContentLakeIngestProperties;
import org.hyland.contentlake.spi.PermissionRule;
import org.hyland.contentlake.spi.SecurityConfig;
import org.hyland.contentlake.spi.SourceNode;
import org.alfresco.core.model.Node;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

/**
 * Converts an Alfresco SDK {@link Node} into a {@link SourceNode}.
 *
 * <p>This adapter lives in {@code content-lake-common} (the Alfresco-specific layer)
 * and must be called at the Alfresco boundary — in the batch/live ingesters —
 * <em>before</em> the node is handed to the shared sync pipeline.</p>
 *
 * <p>The {@code sourceProperties} map is populated with the {@code alfresco_*} keys
 * defined in {@link ContentLakeIngestProperties} so that the pipeline can store them
 * in {@code cin_ingestProperties} without knowing anything about Alfresco.</p>
 */
public final class AlfrescoSourceNodeAdapter {

    private AlfrescoSourceNodeAdapter() {}

    /**
     * Converts an Alfresco {@link Node} to a {@link SourceNode}.
     *
     * @param node           Alfresco node (must include properties, path, and permissions)
     * @param sourceId       identifier of the Alfresco repository instance
     * @param readPrincipals authority identifiers that have read access on this node
     * @return source-agnostic representation of the node
     */
    public static SourceNode toSourceNode(Node node, String sourceId, Set<String> readPrincipals) {
        return new SourceNode(
                node.getId(),
                sourceId,
                "alfresco",
                node.getName(),
                node.getPath() != null ? node.getPath().getName() : null,
                node.getContent() != null ? node.getContent().getMimeType() : null,
                node.getModifiedAt(),
                Boolean.TRUE.equals(node.isIsFolder()),
                readPrincipals,
                Set.of(),
                buildSourceProperties(node, sourceId),
                buildSecurityConfig(node, readPrincipals)
        );
    }

    /**
     * Builds the vendor-neutral {@link SecurityConfig} from the effective read authorities.
     *
     * <p>Alfresco group authorities are prefixed {@code GROUP_}; everything else is a user. Inheritance
     * is read from the node's permission block (treating {@code null} as enabled, matching
     * {@code AlfrescoClient.extractReadAuthorities}). Only ALLOW/READ rules are represented because
     * Alfresco never populates deny principals in this pipeline.</p>
     */
    private static SecurityConfig buildSecurityConfig(Node node, Set<String> readPrincipals) {
        boolean inheritanceEnabled = node.getPermissions() == null
                || !Boolean.FALSE.equals(node.getPermissions().isIsInheritanceEnabled());

        List<PermissionRule> rules = new ArrayList<>();
        if (readPrincipals != null) {
            // Sort for stable, reproducible output.
            for (String authority : new TreeSet<>(readPrincipals)) {
                if (authority == null || authority.isBlank()) {
                    continue;
                }
                String identityType = authority.startsWith("GROUP_") ? "group" : "user";
                rules.add(new PermissionRule(authority, identityType, authority, "READ"));
            }
        }
        return new SecurityConfig(inheritanceEnabled, rules);
    }

    private static Map<String, Object> buildSourceProperties(Node node, String sourceId) {
        String path     = node.getPath()    != null ? node.getPath().getName()       : null;
        String mimeType = node.getContent() != null ? node.getContent().getMimeType() : null;
        String modified = node.getModifiedAt() != null ? node.getModifiedAt().toString() : null;

        Map<String, Object> props = new LinkedHashMap<>();

        // Generic source-agnostic keys (readable by any source-unaware consumer)
        props.put(ContentLakeIngestProperties.SOURCE_NODE_ID,     node.getId());
        props.put(ContentLakeIngestProperties.SOURCE_TYPE,        "alfresco");
        props.put(ContentLakeIngestProperties.SOURCE_NAME,        node.getName());
        props.put(ContentLakeIngestProperties.SOURCE_PATH,        path);
        props.put(ContentLakeIngestProperties.SOURCE_MIME_TYPE,   mimeType);
        // source_modifiedAt is deliberately absent: core seeds it from the record in a fixed-width form,
        // because the modifiedAfter / modifiedBefore filters compare it as text (#149). The value here was
        // OffsetDateTime.toString(), which elides zero seconds, so a node modified on a whole second stored
        // as "2026-09-17T10:00Z" and sorted after any bound carrying seconds.

        // Alfresco-specific keys (preserved for adapter-aware consumers)
        props.put(ContentLakeIngestProperties.ALFRESCO_NODE_ID,       node.getId());
        props.put(ContentLakeIngestProperties.ALFRESCO_REPOSITORY_ID, sourceId);
        props.put(ContentLakeIngestProperties.ALFRESCO_NAME,          node.getName());
        props.put(ContentLakeIngestProperties.ALFRESCO_PATH,          path);
        props.put(ContentLakeIngestProperties.ALFRESCO_MIME_TYPE,     mimeType);
        // The raw source form, on purpose: adapter-aware consumers expect what Alfresco reported, and
        // NodeSyncService.getStoredModifiedAt falls back to this key for documents ingested before the
        // generic one existed. Both parse it, so its width does not matter. Never put it in a range
        // predicate, which is the trap the generic key above exists to avoid.
        props.put(ContentLakeIngestProperties.ALFRESCO_MODIFIED_AT,   modified);

        props.values().removeIf(Objects::isNull);
        return props;
    }
}
