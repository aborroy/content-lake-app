package org.hyland.nuxeo.contentlake.live.client;

import org.hyland.nuxeo.contentlake.auth.BasicNuxeoAuthentication;
import org.hyland.nuxeo.contentlake.auth.NuxeoAuthentication;
import org.hyland.nuxeo.contentlake.config.NuxeoProperties;
import org.hyland.nuxeo.contentlake.live.model.AuditCursor;
import org.hyland.nuxeo.contentlake.live.model.NuxeoAuditPage;
import org.springframework.http.MediaType;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientResponseException;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Reads repository-wide audit entries through Nuxeo Automation.
 *
 * <p>Validated against the local {@code nuxeo-deployment} stack on 2026-03-27:
 * {@code GET /nuxeo/api/v1/audit} returns 404 ("Type not found: audit"), while
 * document-scoped {@code GET /nuxeo/api/v1/id/{uid}/@audit} does not provide a
 * repository-wide cursor. {@code Audit.QueryWithPageProvider} is therefore the
 * required surface for the live ingester because it supports a stable
 * repository-wide window ordered by {@code logDate,id} before checkpointing.</p>
 */
public class NuxeoAuditClient {

    static final DateTimeFormatter NXQL_TIMESTAMP =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
    static final String AUDIT_QUERY_OPERATION = "/automation/Audit.QueryWithPageProvider";

    private static final String SOURCE_EVENT_QUERY = """
            SELECT * FROM LogEntry
            WHERE eventId IN ("documentCreated","documentModified","documentSecurityUpdated","documentTrashed","documentRemoved")
              AND (
                    logDate > TIMESTAMP "%s"
                 OR (logDate = TIMESTAMP "%s" AND id > %d)
              )
              AND logDate <= TIMESTAMP "%s"
            ORDER BY logDate ASC, id ASC
            """;

    private final RestClient restClient;

    public NuxeoAuditClient(NuxeoProperties properties) {
        this(properties, new BasicNuxeoAuthentication(properties.getUsername(), properties.getPassword()));
    }

    public NuxeoAuditClient(NuxeoProperties properties, NuxeoAuthentication authentication) {
        this(properties.getBaseUrl(), authentication);
    }

    public NuxeoAuditClient(String baseUrl, NuxeoAuthentication authentication) {
        this.restClient = RestClient.builder()
                .baseUrl(buildApiBaseUrl(baseUrl))
                .requestInterceptor(authentication.asInterceptor())
                .build();
    }

    public NuxeoAuditPage fetchPage(AuditCursor cursor, OffsetDateTime windowEnd, int pageSize) {
        try {
            NuxeoAuditPage response = restClient.post()
                    .uri(AUDIT_QUERY_OPERATION)
                    .contentType(MediaType.APPLICATION_JSON)
                    .accept(MediaType.APPLICATION_JSON)
                    .body(requestBody(cursor, windowEnd, pageSize))
                    .retrieve()
                    .body(NuxeoAuditPage.class);
            return response != null ? response : new NuxeoAuditPage();
        } catch (RestClientResponseException e) {
            throw new IllegalStateException(
                    "Failed to query Nuxeo audit log via Automation operation "
                            + AUDIT_QUERY_OPERATION
                            + "; the live ingester requires repository-wide audit access through "
                            + "Audit.QueryWithPageProvider because the local stack does not expose "
                            + "GET /api/v1/audit and document-scoped @audit is insufficient",
                    e
            );
        }
    }

    static String buildQuery(AuditCursor cursor, OffsetDateTime windowEnd) {
        String startTimestamp = formatTimestamp(cursor.lastLogDate());
        String endTimestamp = formatTimestamp(windowEnd);
        return SOURCE_EVENT_QUERY.formatted(
                startTimestamp,
                startTimestamp,
                cursor.lastEntryId(),
                endTimestamp
        );
    }

    private static Map<String, Object> requestBody(AuditCursor cursor, OffsetDateTime windowEnd, int pageSize) {
        Map<String, Object> params = new LinkedHashMap<>();
        params.put("pageSize", pageSize);
        params.put("currentPageIndex", 0);
        params.put("query", buildQuery(cursor, windowEnd));
        return Map.of("params", params);
    }

    private static String formatTimestamp(OffsetDateTime value) {
        return value.withOffsetSameInstant(ZoneOffset.UTC).format(NXQL_TIMESTAMP);
    }

    private static String buildApiBaseUrl(String baseUrl) {
        String trimmed = baseUrl.endsWith("/") ? baseUrl.substring(0, baseUrl.length() - 1) : baseUrl;
        return trimmed.endsWith("/api/v1") ? trimmed : trimmed + "/api/v1";
    }
}
