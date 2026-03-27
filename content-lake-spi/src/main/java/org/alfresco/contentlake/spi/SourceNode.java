package org.alfresco.contentlake.spi;

import java.time.OffsetDateTime;
import java.util.Map;
import java.util.Set;

/**
 * Source-agnostic representation of a content node.
 *
 * <p>Each source adapter converts its native document model into this record
 * before handing it to the shared sync pipeline. The pipeline operates solely
 * on {@code SourceNode} — it has no knowledge of Alfresco or Nuxeo types.</p>
 *
 * @param nodeId           unique identifier within the source system
 * @param sourceId         identifies the source system instance (e.g. repository UUID)
 * @param sourceType       short label for the source type: {@code "alfresco"}, {@code "nuxeo"}, …
 * @param name             display name
 * @param path             hierarchical path, or {@code null} for flat repositories
 * @param mimeType         MIME type of the primary content blob; {@code null} for folders
 * @param modifiedAt       last-modified timestamp used for staleness checks
 * @param folder           {@code true} when this node is a container, not a document
 * @param readPrincipals   identities that have effective read access (used to populate {@code sys_acl}
 *                         and {@code cin_read})
 * @param denyPrincipals   identities explicitly denied read access by the source ACL model; stored for
 *                         diagnostics in {@code cin_deny} but not written into {@code sys_acl}
 * @param sourceProperties source-specific metadata stored in {@code cin_ingestProperties}
 *                         alongside the generic keys; keyed by adapter-owned namespaces
 *                         such as {@code alfresco_*} or {@code nuxeo_*}
 */
public record SourceNode(
        String nodeId,
        String sourceId,
        String sourceType,
        String name,
        String path,
        String mimeType,
        OffsetDateTime modifiedAt,
        boolean folder,
        Set<String> readPrincipals,
        Set<String> denyPrincipals,
        Map<String, Object> sourceProperties
) {}
