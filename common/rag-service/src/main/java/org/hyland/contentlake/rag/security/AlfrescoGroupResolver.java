package org.hyland.contentlake.rag.security;

import lombok.extern.slf4j.Slf4j;
import org.hyland.contentlake.security.SourceGroupResolver;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestClient;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

/**
 * Group membership from an Alfresco repository, over {@code /people/{username}/groups} on the public ReST
 * API.
 *
 * <p>Called with the repository's service account rather than the caller's own credentials, because a caller
 * is not necessarily allowed to read their own group list and the answer is authorization input rather than
 * something the caller asked for.</p>
 *
 * <p>Group ids come back already in the {@code GROUP_} form Alfresco ACLs store, so they need no
 * normalisation. A 404 on the person is the repository saying it holds no such identity, which is not a
 * failure; see {@link SourceGroupResolver}.</p>
 */
@Slf4j
@Component
public class AlfrescoGroupResolver implements SourceGroupResolver {

    private static final String GROUPS_PATH =
            "/alfresco/api/-default-/public/alfresco/versions/1/people/{username}/groups";

    /** One page. The repository's own {@code maxItems} ceiling is configurable and often 100. */
    private static final int PAGE_SIZE = 100;

    /**
     * Page bound. A caller in more groups than this loses the tail, which is fail-closed, and it is logged.
     * The un-paged single call this replaced asked for 1000 and dropped everything past it silently.
     */
    private static final int MAX_PAGES = 100;

    private final RestClient restClient;

    public AlfrescoGroupResolver(
            @Value("${content.service.url}") String baseUrl,
            @Value("${content.service.security.basicAuth.username}") String serviceAccountUsername,
            @Value("${content.service.security.basicAuth.password}") String serviceAccountPassword) {
        this.restClient = RestClient.builder()
                .baseUrl(baseUrl.endsWith("/") ? baseUrl.substring(0, baseUrl.length() - 1) : baseUrl)
                .defaultHeaders(headers -> {
                    headers.setAccept(List.of(MediaType.APPLICATION_JSON));
                    headers.setBasicAuth(serviceAccountUsername, serviceAccountPassword);
                })
                .build();
    }

    @Override
    public String sourceType() {
        return "alfresco";
    }

    @Override
    public List<String> resolveGroups(String username) {
        LinkedHashSet<String> groups = new LinkedHashSet<>();

        for (int page = 0; page < MAX_PAGES; page++) {
            Map<String, Object> body = fetchPage(username, page * PAGE_SIZE);
            if (body == null) {
                return page == 0 ? null : List.copyOf(groups);
            }
            Map<String, Object> list = asMap(body.get("list"));
            if (list == null) {
                break;
            }
            addEntries(groups, list.get("entries"));
            if (!hasMoreItems(list)) {
                return List.copyOf(groups);
            }
            if (page == MAX_PAGES - 1) {
                log.warn("User {} is in more than {} Alfresco groups; the rest are not resolved, so a "
                                + "document granted only to one of them is not retrievable",
                        username, MAX_PAGES * PAGE_SIZE);
            }
        }

        return List.copyOf(groups);
    }

    /** One page, or {@code null} when the repository holds no such person. */
    private Map<String, Object> fetchPage(String username, int skipCount) {
        try {
            return restClient.get()
                    // Template-expanded rather than concatenated: a username can carry a slash, a space or
                    // a percent, and an unencoded one reaches a different URL or none at all.
                    .uri(uriBuilder -> uriBuilder.path(GROUPS_PATH)
                            .queryParam("skipCount", skipCount)
                            .queryParam("maxItems", PAGE_SIZE)
                            .build(username))
                    .retrieve()
                    .body(new ParameterizedTypeReference<Map<String, Object>>() {});
        } catch (HttpClientErrorException.NotFound e) {
            return null;
        }
    }

    private static void addEntries(LinkedHashSet<String> groups, Object entries) {
        if (!(entries instanceof List<?> values)) {
            return;
        }
        for (Object value : values) {
            Map<String, Object> entry = asMap(value);
            Map<String, Object> data = entry == null ? null : asMap(entry.get("entry"));
            if (data != null && data.get("id") instanceof String id && !id.isBlank()) {
                groups.add(id);
            }
        }
    }

    private static boolean hasMoreItems(Map<String, Object> list) {
        Map<String, Object> pagination = asMap(list.get("pagination"));
        return pagination != null && Boolean.TRUE.equals(pagination.get("hasMoreItems"));
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> asMap(Object candidate) {
        return candidate instanceof Map<?, ?> map ? (Map<String, Object>) map : null;
    }
}
