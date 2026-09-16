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
import java.util.Set;

/**
 * Group membership from a Nuxeo instance, over {@code /api/v1/user/{username}}.
 *
 * <p>Three places in that payload can carry groups and all three are read: {@code groups} (the direct
 * memberships), {@code extendedGroups} (transitive memberships, as objects), and
 * {@code properties.groups}, which is where the user schema puts them. Which of the three is populated
 * depends on the instance's user directory, so reading only one loses memberships on some deployments.</p>
 *
 * <p>Nuxeo names groups without a prefix while the ingested ACLs store them as {@code GROUP_<name>}, so a
 * name that does not already carry the prefix gets it. A 404 is the directory saying it holds no such
 * identity, which is not a failure; see {@link SourceGroupResolver}.</p>
 */
@Slf4j
@Component
public class NuxeoGroupResolver implements SourceGroupResolver {

    private static final String GROUP_PREFIX = "GROUP_";

    private final RestClient restClient;

    public NuxeoGroupResolver(
            @Value("${nuxeo.base-url:http://localhost:8081/nuxeo}") String baseUrl,
            @Value("${nuxeo.username:Administrator}") String username,
            @Value("${nuxeo.password:Administrator}") String password) {
        this.restClient = RestClient.builder()
                .baseUrl(apiUrl(baseUrl))
                .defaultHeaders(headers -> {
                    headers.setAccept(List.of(MediaType.APPLICATION_JSON));
                    headers.setBasicAuth(username, password);
                })
                .build();
    }

    @Override
    public String sourceType() {
        return "nuxeo";
    }

    @Override
    public List<String> resolveGroups(String username) {
        Map<String, Object> body;
        try {
            body = restClient.get()
                    .uri(uriBuilder -> uriBuilder.path("/user/{username}").build(username))
                    .retrieve()
                    .body(new ParameterizedTypeReference<Map<String, Object>>() {});
        } catch (HttpClientErrorException.NotFound e) {
            return null;
        }
        if (body == null) {
            return null;
        }

        LinkedHashSet<String> groups = new LinkedHashSet<>();
        addNames(groups, body.get("groups"));

        if (body.get("extendedGroups") instanceof List<?> values) {
            for (Object value : values) {
                if (value instanceof Map<?, ?> group) {
                    add(groups, firstString(group.get("name"), group.get("groupname"), group.get("id")));
                }
            }
        }

        if (body.get("properties") instanceof Map<?, ?> properties) {
            addNames(groups, properties.get("groups"));
        }

        return List.copyOf(groups);
    }

    private static void addNames(Set<String> groups, Object candidate) {
        if (candidate instanceof List<?> values) {
            values.forEach(value -> add(groups, value));
        }
    }

    private static void add(Set<String> groups, Object candidate) {
        if (candidate == null) {
            return;
        }
        String group = candidate.toString().trim();
        if (group.isBlank()) {
            return;
        }
        groups.add(group.startsWith(GROUP_PREFIX) ? group : GROUP_PREFIX + group);
    }

    private static String firstString(Object... candidates) {
        for (Object candidate : candidates) {
            if (candidate instanceof String value && !value.isBlank()) {
                return value;
            }
        }
        return null;
    }

    /** The API root, tolerating a configured base URL that already names it or ends in a slash. */
    private static String apiUrl(String baseUrl) {
        String trimmed = baseUrl.endsWith("/") ? baseUrl.substring(0, baseUrl.length() - 1) : baseUrl;
        return trimmed.endsWith("/api/v1") ? trimmed : trimmed + "/api/v1";
    }
}
