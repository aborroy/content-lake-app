package org.hyland.contentlake.rag.security;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

/**
 * Validates a caller's credentials against a CMIS repository, by asking it for its service document.
 *
 * <p>A plain authenticated GET rather than an OpenCMIS session, for three reasons in descending order of what
 * they would cost to discover later. {@code chemistry-opencmis-client-bindings:1.1.0} drags compile-scoped
 * CXF 3.0.12 and {@code woodstox-core-asl:4.4.1} onto a Spring Boot 4 and Java 25 classpath, which is a
 * javax-era SOAP stack inside the service that enforces security trimming, for one HTTP GET. The CMIS
 * connector's shaded copy cannot be borrowed, because it is shaded precisely so a plugin resolves its own
 * dependencies and {@code rag-service} does not load plugins. And a session is the wrong shape anyway: it is
 * built to be created once and reused, where caller authentication needs a cheap per-request check with a
 * hard timeout.</p>
 *
 * <h3>Checking the response shape, not just the status</h3>
 *
 * <p>A 2xx alone would authenticate a caller against whatever happens to be at the configured URL. Point the
 * setting at any authenticated HTTP service and every one of its users becomes a caller here. So the body has
 * to look like a CMIS service document, and when a repository id is configured it has to be the one named.</p>
 *
 * <h3>What this can and cannot tell the caller</h3>
 *
 * <p>Unlike Alfresco's tickets API and Nuxeo's {@code /me}, <strong>a CMIS service document confirms the
 * credentials are valid but reports no canonical username</strong>: the specification has no "who am I"
 * operation. The login string is therefore used as presented, and that is a real limitation rather than an
 * implementation shortcut. A repository that authenticates {@code Admin} case-insensitively but stores its
 * ACEs as {@code admin} yields a principal matching nothing, and one fronted by a directory may accept
 * {@code alice@corp.example} for an ACE stored as {@code alice}. Both fail closed and silently.</p>
 *
 * <p>Do not lower-case it and do not strip a domain. CMIS principal ids are repository-defined and some
 * repositories are case-sensitive, so normalising here would break the repositories that are.</p>
 */
@Slf4j
@Component
@ConditionalOnProperty(name = "rag.security.cmis.enabled", havingValue = "true")
public class CmisRepositoryDirectory {

    private static final int CONNECT_TIMEOUT_MS = 3_000;
    private static final int READ_TIMEOUT_MS = 5_000;

    /** The key every CMIS repository entry carries, in both bindings. */
    private static final String REPOSITORY_ID = "repositoryId";

    private final RestClient restClient;
    private final String repositoryId;
    private final ObjectMapper objectMapper = new ObjectMapper();

    // @Autowired is required rather than decoration: there is a second, package-private constructor for
    // tests, and Spring infers a constructor only when there is exactly one.
    @Autowired
    public CmisRepositoryDirectory(@Value("${rag.security.cmis.url:}") String url,
                                  @Value("${rag.security.cmis.repository-id:}") String repositoryId) {
        if (url == null || url.isBlank()) {
            // Enabled and unconfigured is a misconfiguration, not a deployment with CMIS off: with the bean
            // absent nothing declines, whereas a silent decline here would reject every CMIS caller and look
            // like bad credentials. Follows EntraGroupResolver: a misconfigured authority must not start.
            throw new IllegalStateException("rag.security.cmis.enabled is true, so rag.security.cmis.url "
                    + "is required (the CMIS browser or AtomPub binding URL, the same value the connector "
                    + "takes as cmis.url)");
        }
        this.restClient = RestClient.builder()
                .baseUrl(trimTrailingSlash(url))
                .requestFactory(requestFactory())
                .build();
        this.repositoryId = repositoryId == null ? "" : repositoryId.trim();
    }

    /** For tests, which supply their own client against a local server. */
    CmisRepositoryDirectory(RestClient restClient, String repositoryId) {
        this.restClient = restClient;
        this.repositoryId = repositoryId == null ? "" : repositoryId.trim();
    }

    /**
     * Whether the repository accepts these credentials.
     *
     * <p>Never throws. A 401 or 403 is a decline, and so is anything else, including an I/O failure: an
     * unreachable repository says nothing about the other authorities, so it must not deny a caller one of
     * them would have accepted.</p>
     */
    boolean authenticate(String username, String password) {
        try {
            ResponseEntity<String> response = restClient.get()
                    .accept(MediaType.APPLICATION_JSON, MediaType.APPLICATION_ATOM_XML, MediaType.ALL)
                    .headers(headers -> headers.setBasicAuth(username, password))
                    .retrieve()
                    // The status is the answer, so nothing here is an exception.
                    .onStatus(status -> true, (request, clientResponse) -> {
                    })
                    .toEntity(String.class);

            if (!response.getStatusCode().is2xxSuccessful()) {
                log.debug("CMIS declined '{}': HTTP {}", username, response.getStatusCode().value());
                return false;
            }
            if (!isServiceDocument(response.getBody())) {
                log.debug("CMIS declined '{}': a 2xx whose body is not a service document naming {}{}",
                        username, REPOSITORY_ID,
                        repositoryId.isEmpty() ? "" : " and repository '" + repositoryId + "'");
                return false;
            }
            log.debug("Authenticated '{}' via CMIS", username);
            return true;
        } catch (Exception e) {
            log.debug("CMIS repository unavailable for '{}': {}", username, e.getMessage());
            return false;
        }
    }

    /**
     * Whether the body is a CMIS service document, and names the configured repository when one is set.
     *
     * <p>Two checks, because the two bindings answer in different formats. The browser binding answers a JSON
     * object keyed by repository id whose entries each carry {@code repositoryId}, which can be checked
     * properly. AtomPub answers XML, and this only looks for {@code repositoryId} anywhere in it: that is a
     * crude check, and it is called crude rather than dressed up, but it still distinguishes a CMIS
     * repository from an arbitrary authenticated service.</p>
     */
    private boolean isServiceDocument(String body) {
        if (body == null || body.isBlank()) {
            return false;
        }
        JsonNode root = parseJsonObject(body);
        if (root != null) {
            return isBrowserServiceDocument(root);
        }
        // AtomPub, or anything else that is not a JSON object.
        if (!body.contains(REPOSITORY_ID)) {
            return false;
        }
        return repositoryId.isEmpty() || body.contains(repositoryId);
    }

    private boolean isBrowserServiceDocument(JsonNode root) {
        if (!repositoryId.isEmpty()) {
            JsonNode named = root.get(repositoryId);
            return named != null && named.hasNonNull(REPOSITORY_ID);
        }
        // No repository pinned, so any entry that declares a repositoryId is enough.
        for (JsonNode entry : root) {
            if (entry.hasNonNull(REPOSITORY_ID)) {
                return true;
            }
        }
        return false;
    }

    /** The body as a JSON object, or {@code null} when it is not one. */
    private JsonNode parseJsonObject(String body) {
        try {
            JsonNode root = objectMapper.readTree(body);
            return root != null && root.isObject() ? root : null;
        } catch (Exception e) {
            return null;
        }
    }

    private static SimpleClientHttpRequestFactory requestFactory() {
        SimpleClientHttpRequestFactory factory = new SimpleClientHttpRequestFactory();
        // An authentication hang is a request hang, so these match the Alfresco and Nuxeo directories.
        factory.setConnectTimeout(CONNECT_TIMEOUT_MS);
        factory.setReadTimeout(READ_TIMEOUT_MS);
        return factory;
    }

    private static String trimTrailingSlash(String url) {
        String trimmed = url.trim();
        return trimmed.endsWith("/") ? trimmed.substring(0, trimmed.length() - 1) : trimmed;
    }
}
