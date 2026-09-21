package org.hyland.nuxeo.contentlake.adapter;

import org.hyland.contentlake.model.ContentLakeIngestProperties;
import org.hyland.nuxeo.contentlake.model.NuxeoDocument;
import org.hyland.contentlake.spi.PermissionRule;
import org.hyland.contentlake.spi.SecurityConfig;
import org.hyland.contentlake.spi.SourceNode;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

/**
 * Converts a Nuxeo REST document payload into a source-agnostic {@link SourceNode}.
 *
 * <p>For file-like documents the shared sync pipeline still expects the parent path,
 * not the full document path, when constructing the Content Lake target path.
 * The adapter therefore stores the full Nuxeo repository path in
 * {@code nuxeo_path} while exposing the parent path through {@link SourceNode#path()}.
 * Folder-like documents keep their full path in both places because they are used
 * for traversal, not for content ingestion.</p>
 */
public final class NuxeoSourceNodeAdapter {

    private static final Set<String> CONTAINER_TYPES = Set.of(
            "Domain", "WorkspaceRoot", "Workspace", "Folder", "OrderedFolder", "Section", "Root"
    );

    private NuxeoSourceNodeAdapter() {}

    public static SourceNode toSourceNode(NuxeoDocument document,
                                          String sourceId,
                                          String blobXpath,
                                          Set<String> readPrincipals,
                                          Set<String> denyPrincipals) {
        boolean folder = isContainerType(document.getType());
        String fullPath = document.getPath();
        String nodePath = folder ? fullPath : document.getParentPath();
        String mimeType = folder ? null : document.getBlobMimeType(blobXpath);

        Map<String, Object> props = new LinkedHashMap<>();
        props.put(ContentLakeIngestProperties.SOURCE_NODE_ID, document.getUid());
        props.put(ContentLakeIngestProperties.SOURCE_TYPE, "nuxeo");
        props.put(ContentLakeIngestProperties.SOURCE_PATH, nodePath);
        props.put(ContentLakeIngestProperties.SOURCE_NAME, document.getDisplayName());
        props.put(ContentLakeIngestProperties.SOURCE_MIME_TYPE, mimeType);
        // source_modifiedAt is deliberately absent: core seeds it from the record in a fixed-width form,
        // because the modifiedAfter / modifiedBefore filters compare it as text (#149). The value here was
        // OffsetDateTime.toString(), which elides zero seconds, so a document modified on a whole second
        // stored as "2026-03-24T09:15Z" and sorted after any bound carrying seconds. There is no
        // nuxeo_modifiedAt to keep the raw form in, and none is needed: dc:modified is in the document's
        // own properties already.
        props.put(ContentLakeIngestProperties.NUXEO_PATH, fullPath);
        props.put(ContentLakeIngestProperties.NUXEO_DOCUMENT_TYPE, document.getType());
        props.put(ContentLakeIngestProperties.NUXEO_LIFECYCLE_STATE, document.getState());
        props.put(ContentLakeIngestProperties.NUXEO_BLOB_XPATH, blobXpath);
        if (document.getFacets() != null && !document.getFacets().isEmpty()) {
            props.put(ContentLakeIngestProperties.NUXEO_FACETS, List.copyOf(document.getFacets()));
        }
        Object excludeRaw = document.getProperties().get("cls:excludeFromScope");
        if (Boolean.TRUE.equals(excludeRaw) || "true".equalsIgnoreCase(String.valueOf(excludeRaw))) {
            props.put(ContentLakeIngestProperties.NUXEO_EXCLUDE_FROM_SCOPE, true);
        }
        props.values().removeIf(Objects::isNull);

        return new SourceNode(
                document.getUid(),
                sourceId,
                "nuxeo",
                document.getDisplayName(),
                nodePath,
                mimeType,
                document.getModifiedAt(),
                folder,
                new LinkedHashSet<>(readPrincipals),
                new LinkedHashSet<>(denyPrincipals),
                props,
                buildSecurityConfig(readPrincipals, denyPrincipals)
        );
    }

    public static boolean isContainerType(String type) {
        return type != null && CONTAINER_TYPES.contains(type);
    }

    /**
     * Builds the vendor-neutral {@link SecurityConfig} from the effective read grants and explicit
     * denies. By the time principals reach this adapter, {@code NuxeoClient} has normalized group
     * authorities to the {@code GROUP_} prefix (matching the Alfresco convention), so identity type is
     * derived from that prefix. Nuxeo effective ACLs already fold in inherited entries, so inheritance
     * is reported as enabled.
     */
    private static SecurityConfig buildSecurityConfig(Set<String> readPrincipals, Set<String> denyPrincipals) {
        List<PermissionRule> rules = new ArrayList<>();
        appendRules(rules, readPrincipals, "READ");
        appendRules(rules, denyPrincipals, "READ_DENY");
        return new SecurityConfig(true, rules);
    }

    private static void appendRules(List<PermissionRule> rules, Set<String> principals, String access) {
        if (principals == null) {
            return;
        }
        // Sort for stable, reproducible output.
        for (String principal : new TreeSet<>(principals)) {
            if (principal == null || principal.isBlank()) {
                continue;
            }
            String identityType = principal.startsWith("GROUP_") ? "group" : "user";
            rules.add(new PermissionRule(principal, identityType, principal, access));
        }
    }
}
