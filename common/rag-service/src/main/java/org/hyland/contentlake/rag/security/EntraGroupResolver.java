package org.hyland.contentlake.rag.security;

import com.microsoft.aad.msal4j.ClientCredentialFactory;
import com.microsoft.aad.msal4j.ClientCredentialParameters;
import com.microsoft.aad.msal4j.ConfidentialClientApplication;
import com.microsoft.aad.msal4j.IClientCredential;
import lombok.extern.slf4j.Slf4j;
import org.hyland.contentlake.security.SourceGroupResolver;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestClient;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

/**
 * Group membership from Entra ID, so a document a Graph source granted to a group is retrievable by its
 * members.
 *
 * <p>Without this, a SharePoint document granted only to a group is ingested with correct read principals
 * and retrievable by nobody: the ACL names {@code GROUP_<objectId>} and nothing at query time can say which
 * groups a caller is in. Since group grants are the normal way SharePoint is administered, that is most of a
 * real corpus, so the connector looks broken rather than restricted.</p>
 *
 * <p>Registered only when a tenant is configured. That is deliberate rather than tidiness: a resolver that
 * exists and cannot work is worse than none at all, because {@link GroupResolutionFailurePolicy} may then
 * cost a caller the whole source on every query, where no resolver merely leaves group-granted documents
 * invisible.</p>
 *
 * <h3>Transitive, not direct</h3>
 * <p>{@code /transitiveMemberOf} rather than {@code /memberOf}, because a SharePoint grant to a group whose
 * members are other groups is ordinary, and a direct read would silently under-resolve it: the caller would
 * be told they are in fewer groups than they are, and documents would quietly not appear.</p>
 *
 * <h3>The caller's username is not necessarily their Entra identity</h3>
 * <p>A caller authenticates against Alfresco or Nuxeo, so the username reaching this resolver is whatever
 * that repository calls them. Entra wants an object id or a userPrincipalName. Where the two differ,
 * {@code rag.security.entra.username-suffix} appends a domain to a bare username, which covers the common
 * case of one tenant whose UPNs are {@code <samAccountName>@<domain>}. Anything more elaborate is a real
 * identity-mapping problem and out of scope here; the safe outcome is the one Graph already gives, a 404,
 * which this reports as "no such identity" so the caller keeps the source with their default authorities.</p>
 */
@Slf4j
@Component
@ConditionalOnProperty(name = "rag.security.entra.enabled", havingValue = "true")
public class EntraGroupResolver implements SourceGroupResolver {

    /** Matches what a Graph connector writes into the ACL. Core reads the prefix as "this is a group". */
    private static final String GROUP_PREFIX = "GROUP_";

    private static final String DEFAULT_GRAPH_BASE_URL = "https://graph.microsoft.com/v1.0";
    private static final String GRAPH_DEFAULT_SCOPE = "https://graph.microsoft.com/.default";

    /** Stops a runaway pager; a user in more groups than this has other problems. */
    private static final int MAX_PAGES = 20;

    private final RestClient restClient;
    private final Supplier<String> accessToken;
    private final String sourceType;
    private final String usernameSuffix;

    // @Autowired is required, not decoration: this class has a second, package-private constructor for
    // tests, and Spring only infers a constructor when there is exactly one. Without the annotation it looks
    // for a default constructor, fails to build the bean, and takes the whole resolver registry and the
    // service down with it at startup.
    @Autowired
    public EntraGroupResolver(
            @Value("${rag.security.entra.graph-base-url:" + DEFAULT_GRAPH_BASE_URL + "}") String graphBaseUrl,
            @Value("${rag.security.entra.tenant-id:}") String tenantId,
            @Value("${rag.security.entra.client-id:}") String clientId,
            @Value("${rag.security.entra.client-secret:}") String clientSecret,
            @Value("${rag.security.entra.certificate-path:}") String certificatePath,
            @Value("${rag.security.entra.certificate-password:}") String certificatePassword,
            @Value("${rag.security.entra.auth-mode:client-credentials}") String authMode,
            @Value("${rag.security.entra.access-token:}") String staticToken,
            @Value("${rag.security.entra.source-type:sharepoint}") String sourceType,
            @Value("${rag.security.entra.username-suffix:}") String usernameSuffix) {
        this(RestClient.builder().baseUrl(trimTrailingSlash(graphBaseUrl))
                        .defaultHeaders(headers -> headers.setAccept(List.of(MediaType.APPLICATION_JSON)))
                        .build(),
                tokenSupplier(authMode, tenantId, clientId, clientSecret, certificatePath,
                        certificatePassword, staticToken),
                sourceType, usernameSuffix);
        log.info("Entra group resolver active for source type '{}' against {}", this.sourceType,
                trimTrailingSlash(graphBaseUrl));
    }

    /** Test seam: a fixed client and token, so the tests exercise the paging and the three answers. */
    EntraGroupResolver(RestClient restClient, Supplier<String> accessToken, String sourceType,
                       String usernameSuffix) {
        this.restClient = restClient;
        this.accessToken = accessToken;
        this.sourceType = sourceType == null || sourceType.isBlank() ? "sharepoint" : sourceType.trim();
        this.usernameSuffix = usernameSuffix == null ? "" : usernameSuffix.trim();
    }

    @Override
    public String sourceType() {
        return sourceType;
    }

    /**
     * The caller's Entra groups as {@code GROUP_<objectId>}, or {@code null} when Entra has no such identity.
     *
     * <p>Ids only. An Entra display name is not unique, so a group principal built from one would match
     * documents granted to a different group of the same name, which is a leak rather than a convenience.
     * The connector writes ids for the same reason.</p>
     */
    @Override
    public List<String> resolveGroups(String username) {
        if (username == null || username.isBlank()) {
            return null;
        }
        String identity = entraIdentity(username);
        Set<String> groups = new LinkedHashSet<>();
        String uri = "/users/" + identity + "/transitiveMemberOf/microsoft.graph.group?$select=id";

        for (int page = 0; uri != null && page < MAX_PAGES; page++) {
            Map<String, Object> body;
            try {
                final String requestUri = uri;
                body = restClient.get()
                        .uri(requestUri)
                        .header("Authorization", "Bearer " + accessToken.get())
                        .retrieve()
                        .body(new ParameterizedTypeReference<Map<String, Object>>() {});
            } catch (HttpClientErrorException.NotFound e) {
                // Entra holds no such identity. Not a failure: the caller keeps this source with their
                // default authorities, which is the ordinary case for a user of another repository.
                log.debug("Entra has no identity '{}'; resolving no groups for this source", identity);
                return null;
            }
            if (body == null) {
                break;
            }
            groups.addAll(groupIdsFrom(body));
            uri = nextLink(body);
        }

        log.debug("Entra resolved {} group(s) for '{}'", groups.size(), identity);
        return List.copyOf(groups);
    }

    @SuppressWarnings("unchecked")
    private static List<String> groupIdsFrom(Map<String, Object> body) {
        Object value = body.get("value");
        if (!(value instanceof List<?> entries)) {
            return List.of();
        }
        List<String> ids = new ArrayList<>();
        for (Object entry : entries) {
            if (entry instanceof Map<?, ?> group) {
                Object id = ((Map<String, Object>) group).get("id");
                if (id != null && !id.toString().isBlank()) {
                    ids.add(GROUP_PREFIX + id.toString().trim());
                }
            }
        }
        return ids;
    }

    /**
     * The next page, as a path this client can request.
     *
     * <p>Graph returns an absolute URL, and this {@link RestClient} has a base URL, so the host part is
     * stripped rather than the link being followed verbatim. Keeping the query string matters: it carries the
     * opaque skip token.</p>
     */
    private String nextLink(Map<String, Object> body) {
        Object next = body.get("@odata.nextLink");
        if (next == null) {
            return null;
        }
        String link = next.toString();
        int marker = link.indexOf("/users/");
        return marker >= 0 ? link.substring(marker) : null;
    }

    /** Appends the configured domain to a bare username, and leaves an object id or a UPN alone. */
    private String entraIdentity(String username) {
        String trimmed = username.trim();
        if (usernameSuffix.isEmpty() || trimmed.contains("@")) {
            return trimmed;
        }
        return trimmed + (usernameSuffix.startsWith("@") ? usernameSuffix : "@" + usernameSuffix);
    }

    /**
     * Tokens for the query path, which is a much smaller job than the connector's.
     *
     * <p>msal4j is used directly here rather than sharing the connector's client, because that client lives
     * inside a shaded plugin jar this service never loads. Duplicating its resource-unit meter and its
     * tenant-wide throttle pause would be the wrong trade anyway: this makes one cheap call per user per
     * cache window, not a crawl, so what it needs from msal4j is the token cache and nothing else.</p>
     *
     * <p>A deployment therefore configures Graph credentials twice, once for the ingester and once here.
     * That is a real cost and it is the reason the alternative was considered; it is accepted because the
     * two have genuinely different rate and failure profiles.</p>
     */
    private static Supplier<String> tokenSupplier(String authMode, String tenantId, String clientId,
                                                  String clientSecret, String certificatePath,
                                                  String certificatePassword, String staticToken) {
        if ("static-token".equalsIgnoreCase(String.valueOf(authMode).trim())) {
            if (staticToken == null || staticToken.isBlank()) {
                throw new IllegalStateException("rag.security.entra.auth-mode is static-token but "
                        + "rag.security.entra.access-token is empty");
            }
            log.warn("Entra group resolver is using a static bearer token. Development only: it cannot be "
                    + "refreshed, so group resolution stops working when it expires.");
            String token = staticToken.trim();
            return () -> token;
        }

        if (tenantId == null || tenantId.isBlank() || clientId == null || clientId.isBlank()) {
            throw new IllegalStateException("rag.security.entra.enabled is true, so "
                    + "rag.security.entra.tenant-id and rag.security.entra.client-id are required");
        }
        boolean hasSecret = clientSecret != null && !clientSecret.isBlank();
        boolean hasCertificate = certificatePath != null && !certificatePath.isBlank();
        if (hasSecret == hasCertificate) {
            throw new IllegalStateException("Supply exactly one of rag.security.entra.client-secret and "
                    + "rag.security.entra.certificate-path");
        }

        IClientCredential credential = hasSecret
                ? ClientCredentialFactory.createFromSecret(clientSecret)
                : certificateCredential(Path.of(certificatePath.trim()), certificatePassword);
        ConfidentialClientApplication application;
        try {
            application = ConfidentialClientApplication.builder(clientId.trim(), credential)
                    .authority("https://login.microsoftonline.com/" + tenantId.trim())
                    .build();
        } catch (Exception e) {
            throw new IllegalStateException("Could not build the Entra ID client for tenant '" + tenantId
                    + "': " + e.getMessage(), e);
        }
        ClientCredentialParameters parameters =
                ClientCredentialParameters.builder(Set.of(GRAPH_DEFAULT_SCOPE)).build();

        // msal4j holds an application token cache and applies its own expiry skew, so this is called per
        // request without re-authenticating.
        return () -> {
            try {
                return application.acquireToken(parameters).get().accessToken();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Interrupted acquiring an Entra token", e);
            } catch (Exception e) {
                // A thrown exception is the contract's "the directory could not be asked", so this must not
                // be softened into a null: that would downgrade a fail-closed deployment silently.
                throw new IllegalStateException("Could not acquire an Entra token for group resolution: "
                        + e.getMessage(), e);
            }
        };
    }

    private static IClientCredential certificateCredential(Path certificate, String password) {
        if (!Files.isReadable(certificate)) {
            throw new IllegalStateException("rag.security.entra.certificate-path '" + certificate
                    + "' does not exist or cannot be read");
        }
        try (InputStream pkcs12 = Files.newInputStream(certificate)) {
            return ClientCredentialFactory.createFromCertificate(pkcs12,
                    password == null ? "" : password);
        } catch (Exception e) {
            // A wrong password arrives as an IOException from the keystore reader, not a distinct type, so
            // both possibilities are named.
            throw new IllegalStateException("Could not load the certificate at " + certificate
                    + "; check that it is a PKCS#12 file and that the password matches", e);
        }
    }

    private static String trimTrailingSlash(String url) {
        String value = url == null || url.isBlank() ? DEFAULT_GRAPH_BASE_URL : url.trim();
        return value.endsWith("/") ? value.substring(0, value.length() - 1) : value;
    }

    /** For a startup log line that names the mode without naming the credential. */
    @Override
    public String toString() {
        return "EntraGroupResolver(sourceType=" + sourceType
                + (usernameSuffix.isEmpty() ? "" : ", usernameSuffix=" + usernameSuffix) + ")";
    }
}
