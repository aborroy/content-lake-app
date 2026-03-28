package org.hyland.contentlake.rag.service;

import lombok.RequiredArgsConstructor;
import org.hyland.contentlake.model.ContentLakeIngestProperties;
import org.hyland.contentlake.model.HxprDocument;
import org.hyland.contentlake.rag.config.RagProperties;
import org.hyland.contentlake.rag.model.SemanticSearchResponse.SourceDocument;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;
import org.springframework.web.util.UriUtils;

import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

@Component
@RequiredArgsConstructor
public class SourceMetadataResolver {

    private static final String SOURCE_TYPE_ALFRESCO = "alfresco";
    private static final String SOURCE_TYPE_NUXEO = "nuxeo";

    private final RagProperties ragProperties;
    private final Environment environment;

    public SourceDocument resolveSourceDocument(String docId, HxprDocument doc) {
        Map<String, Object> ingestProperties = doc.getCinIngestProperties() != null
                ? doc.getCinIngestProperties()
                : Map.of();

        String sourceType = asString(ingestProperties.get(ContentLakeIngestProperties.SOURCE_TYPE));
        String sourceName = asString(ingestProperties.get(ContentLakeIngestProperties.SOURCE_NAME));
        String sourcePath = asString(ingestProperties.get(ContentLakeIngestProperties.SOURCE_PATH));
        String mimeType = asString(ingestProperties.get(ContentLakeIngestProperties.SOURCE_MIME_TYPE));
        String nuxeoPath = asString(ingestProperties.get(ContentLakeIngestProperties.NUXEO_PATH));
        String cinSourceId = doc.getCinSourceId();
        String openInSourceUrl = buildOpenInSourceUrl(
                sourceType,
                doc.getCinId(),
                cinSourceId,
                sourcePath,
                sourceName,
                nuxeoPath
        );

        return SourceDocument.builder()
                .documentId(docId)
                .nodeId(doc.getCinId() != null ? doc.getCinId() : doc.getSysName())
                .sourceId(cinSourceId)
                .sourceType(sourceType)
                .name(sourceName)
                .path(sourcePath)
                .mimeType(mimeType)
                .openInSourceUrl(openInSourceUrl)
                .build();
    }

    private String buildOpenInSourceUrl(String sourceType,
                                        String nodeId,
                                        String cinSourceId,
                                        String sourcePath,
                                        String sourceName,
                                        String nuxeoPath) {
        if (sourceType == null || sourceType.isBlank()) {
            return null;
        }

        String resolvedNuxeoPath = firstNonBlank(nuxeoPath, joinPath(sourcePath, sourceName));
        Map<String, String> placeholders = new LinkedHashMap<>();
        placeholders.put("nodeId", trimToNull(nodeId));
        placeholders.put("sourceId", extractSourceId(cinSourceId));
        placeholders.put("cinSourceId", trimToNull(cinSourceId));
        placeholders.put("sourcePath", encodePath(sourcePath));
        placeholders.put("sourceName", encodePathSegment(sourceName));
        placeholders.put("nuxeoPath", encodePath(resolvedNuxeoPath));

        if (SOURCE_TYPE_ALFRESCO.equalsIgnoreCase(sourceType)) {
            return applyTemplate(
                    ragProperties.getSourceLinks().getAlfrescoTemplate(),
                    placeholders,
                    Set.of("nodeId")
            );
        }

        if (SOURCE_TYPE_NUXEO.equalsIgnoreCase(sourceType)) {
            return applyTemplate(
                    ragProperties.getSourceLinks().getNuxeoTemplate(),
                    placeholders,
                    Set.of("nuxeoPath")
            );
        }

        return null;
    }

    private String applyTemplate(String template, Map<String, String> placeholders, Set<String> requiredKeys) {
        if (template == null || template.isBlank()) {
            return null;
        }
        String resolvedTemplate = environment.resolvePlaceholders(template);

        for (String requiredKey : requiredKeys) {
            if (placeholders.get(requiredKey) == null) {
                return null;
            }
        }

        String resolved = resolvedTemplate;
        for (Map.Entry<String, String> entry : placeholders.entrySet()) {
            String placeholder = "{" + entry.getKey() + "}";
            if (!resolved.contains(placeholder)) {
                continue;
            }
            String value = entry.getValue();
            if (value == null) {
                return null;
            }
            resolved = resolved.replace(placeholder, value);
        }

        return resolved.contains("{") ? null : resolved;
    }

    private String extractSourceId(String cinSourceId) {
        String value = trimToNull(cinSourceId);
        if (value == null) {
            return null;
        }
        int separator = value.indexOf(':');
        return separator >= 0 && separator < value.length() - 1
                ? value.substring(separator + 1)
                : value;
    }

    private String joinPath(String sourcePath, String sourceName) {
        String path = trimToNull(sourcePath);
        String name = trimToNull(sourceName);
        if (path == null || name == null) {
            return null;
        }
        return path.endsWith("/") ? path + name : path + "/" + name;
    }

    private String firstNonBlank(String... candidates) {
        for (String candidate : candidates) {
            String value = trimToNull(candidate);
            if (value != null) {
                return value;
            }
        }
        return null;
    }

    private String encodePath(String value) {
        String normalized = trimToNull(value);
        return normalized != null ? UriUtils.encodePath(normalized, StandardCharsets.UTF_8) : null;
    }

    private String encodePathSegment(String value) {
        String normalized = trimToNull(value);
        return normalized != null ? UriUtils.encodePathSegment(normalized, StandardCharsets.UTF_8) : null;
    }

    private String asString(Object value) {
        return value != null ? trimToNull(value.toString()) : null;
    }

    private String trimToNull(String value) {
        if (value == null) {
            return null;
        }
        String trimmed = value.trim();
        return trimmed.isEmpty() ? null : trimmed;
    }
}
