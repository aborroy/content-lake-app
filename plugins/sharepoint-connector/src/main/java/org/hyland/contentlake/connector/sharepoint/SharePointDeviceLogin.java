package org.hyland.contentlake.connector.sharepoint;

import com.microsoft.aad.msal4j.DeviceCodeFlowParameters;
import com.microsoft.aad.msal4j.IAuthenticationResult;
import com.microsoft.aad.msal4j.PublicClientApplication;

import java.nio.file.Path;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Signs a named user in once, on a host, and leaves a refresh token the connector can use unattended.
 *
 * <p>Run this by hand before starting a deployment configured with {@code sharepoint.auth-mode=device-code}.
 * It prints a short code and a URL, waits for the sign-in to complete in a browser, writes the token cache,
 * and confirms the result against Graph so a bad configuration is caught here rather than on the first
 * crawl.</p>
 *
 * <h3>Why this is a separate entry point rather than something the container does</h3>
 * <p>See {@link DeviceCodeTokenProvider} for the full argument. In short: the flow blocks for up to fifteen
 * minutes, the container has no way to put a human in front of a browser, and exposing the pending code over
 * HTTP would mean an unauthenticated endpoint next to one that triggers a full re-ingest.</p>
 *
 * <h3>Running it needs a classpath, not just this jar</h3>
 * <p>{@code slf4j-api} is deliberately {@code provided} for this plugin, because the host is a Spring Boot
 * application that already has it with a binding, and shading a second copy in would give the host two.
 * msal4j logs through slf4j, so this class cannot run from the jar alone: it fails during class
 * initialisation with {@code NoClassDefFoundError: org/slf4j/LoggerFactory}, before printing anything.</p>
 *
 * <p>The deployment repository ships a wrapper that assembles the classpath. That indirection is the price
 * of not shading slf4j, and it is the right side of that trade.</p>
 */
public final class SharePointDeviceLogin {

    /** Named in every "sign in again" message, so the message is actionable without reading a document. */
    static final String COMMAND_HINT = "scripts/sharepoint-device-login.sh";

    /**
     * Delegated scopes requested when none are configured.
     *
     * <p>{@code Sites.Read.All} is the least-privileged permission measured to serve every call this
     * connector makes, including the item-permission reads the ACL mapping depends on. {@code offline_access}
     * is what yields the refresh token, which is the entire reason this mode exists.</p>
     */
    private static final List<String> DEFAULT_SCOPES =
            List.of("https://graph.microsoft.com/Sites.Read.All", "offline_access");

    private SharePointDeviceLogin() {
    }

    /**
     * The application both this tool and the silent provider build, so they cannot disagree about which
     * cache or which authority they are using. A mismatch there presents as "signed in but still refused".
     */
    static PublicClientApplication application(String authority,
                                               String clientId,
                                               Path cachePath,
                                               com.microsoft.aad.msal4j.IHttpClient transport) {
        if (clientId == null || clientId.isBlank()) {
            throw new GraphException(SharePointConnectorPlugin.CLIENT_ID_SETTING + " is required for "
                    + "device-code authentication");
        }
        try {
            PublicClientApplication.Builder builder = PublicClientApplication.builder(clientId)
                    .setTokenCacheAccessAspect(new FileTokenCache(cachePath));
            if (authority != null && !authority.isBlank()) {
                builder.authority(authority);
            }
            if (transport != null) {
                builder.httpClient(transport);
            }
            return builder.build();
        } catch (GraphException e) {
            throw e;
        } catch (Exception e) {
            throw new GraphException("Could not build the Entra ID public client for authority '" + authority
                    + "' (" + SharePointConnectorPlugin.TENANT_ID_SETTING + ", or "
                    + SharePointConnectorPlugin.AUTHORITY_SETTING + " if set): " + e.getMessage(), e);
        }
    }

    /** Configured scopes, or the default set. Blank entries are dropped rather than sent to Entra. */
    static Set<String> effectiveScopes(List<String> configured) {
        List<String> source = configured == null || configured.isEmpty() ? DEFAULT_SCOPES : configured;
        Set<String> scopes = new LinkedHashSet<>();
        for (String scope : source) {
            if (scope != null && !scope.isBlank()) {
                scopes.add(scope.trim());
            }
        }
        if (scopes.isEmpty()) {
            throw new GraphException(SharePointConnectorPlugin.SCOPES_SETTING
                    + " is set but contains no usable scope");
        }
        return scopes;
    }

    public static void main(String[] args) {
        String tenantId = env("SHAREPOINT_TENANT_ID");
        String authority = env("SHAREPOINT_AUTHORITY");
        String clientId = env("SHAREPOINT_CLIENT_ID");
        String cache = env("SHAREPOINT_TOKEN_CACHE_PATH");
        List<String> scopes = split(env("SHAREPOINT_SCOPES"));

        if (clientId == null) {
            fail("SHAREPOINT_CLIENT_ID is required.");
        }
        if (cache == null) {
            fail("SHAREPOINT_TOKEN_CACHE_PATH is required: it is where the refresh token will be written.");
        }
        if (authority == null) {
            if (tenantId == null) {
                fail("Set SHAREPOINT_TENANT_ID, or SHAREPOINT_AUTHORITY for a sovereign cloud.");
            }
            authority = SharePointConnectorPlugin.DEFAULT_AUTHORITY_HOST + tenantId.trim();
        }

        Path cachePath = Path.of(cache);
        Set<String> requested = effectiveScopes(scopes);
        PublicClientApplication application = application(authority, clientId, cachePath, null);

        System.out.println("Signing in to " + authority);
        System.out.println("Scopes: " + requested);
        System.out.println();

        try {
            IAuthenticationResult result = application.acquireToken(
                            DeviceCodeFlowParameters.builder(requested, code -> {
                                // The library's own message already names the URL and the code, and it is
                                // localised. Printing our own wording would drift from what the browser says.
                                System.out.println(code.message());
                                System.out.println();
                                System.out.flush();
                            }).build())
                    .get();

            if (result == null || result.accessToken() == null || result.accessToken().isBlank()) {
                fail("Entra ID completed the sign-in but returned no access token.");
            }

            String signedInAs = result.account() == null ? "an unknown account" : result.account().username();
            System.out.println("Signed in as " + signedInAs);
            System.out.println("Token cache written to " + cachePath.toAbsolutePath());
            System.out.println();
            System.out.println("Mount that file read-only into the ingester and set "
                    + SharePointConnectorPlugin.TOKEN_CACHE_PATH_SETTING + " to its container path.");
            System.out.println("It holds a refresh token. Treat it as a credential: do not commit it, and "
                    + "keep it owner-readable only.");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            fail("Interrupted before the sign-in completed.");
        } catch (Exception e) {
            Throwable cause = e.getCause() == null ? e : e.getCause();
            fail("Sign-in failed: " + cause.getMessage());
        }
    }

    private static String env(String name) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? null : value.trim();
    }

    private static List<String> split(String value) {
        if (value == null) {
            return List.of();
        }
        return java.util.Arrays.stream(value.split("[,\\s]+"))
                .map(scope -> scope.trim())
                .filter(scope -> !scope.isEmpty())
                .toList();
    }

    /**
     * Exits non-zero with a message on stderr.
     *
     * <p>A wrapper script has to be able to tell a failed sign-in from a successful one, and this tool is
     * the kind of thing that gets put in a shell pipeline the first time it is used twice.</p>
     */
    private static void fail(String message) {
        System.err.println(message);
        System.exit(1);
    }
}
