package org.hyland.contentlake.connector.sharepoint;

import com.microsoft.aad.msal4j.IAccount;
import com.microsoft.aad.msal4j.IAuthenticationResult;
import com.microsoft.aad.msal4j.MsalInteractionRequiredException;
import com.microsoft.aad.msal4j.PublicClientApplication;
import com.microsoft.aad.msal4j.SilentParameters;

import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.logging.Logger;

/**
 * Delegated Graph tokens for a named user, refreshed silently from a cache a human populated once.
 *
 * <p>This is the mode for a tenant that will not issue an application registration with application
 * permissions. Every read is then made as a signed-in person and is bounded by that person's own rights,
 * which is both the point and the limitation: what the crawling identity cannot see is absent from the
 * index, so this indexes one identity's view of a site rather than the site.</p>
 *
 * <h3>It never prompts</h3>
 * <p>{@link #token()} only ever calls {@code acquireTokenSilently}. The interactive device-code flow lives
 * in {@link SharePointDeviceLogin}, which runs on a host rather than in the container, for three reasons
 * that are each sufficient on their own:</p>
 * <ul>
 *   <li>msal4j's device-code call blocks until the human finishes or the code expires, about fifteen
 *       minutes. Inside the ingestion path that hangs the first Graph call of a job, and the host counts a
 *       failed container listing as an incomplete pass rather than an error, so the result would be a
 *       quietly partial crawl rather than a visible failure.</li>
 *   <li>Surfacing a pending user code over HTTP would need an unauthenticated endpoint on a service whose
 *       neighbour triggers a full re-ingest. The host's security configuration is default-deny by explicit
 *       invariant, and this is not the reason to put the first hole in it.</li>
 *   <li>Somebody has to open a browser either way, so the in-container flow buys nothing.</li>
 * </ul>
 *
 * <p>So an empty or expired cache is a configuration problem, reported as one, naming the command that
 * fixes it. That matches this interface's contract that a token failure "is a configuration or tenant
 * problem and not something a retry will fix".</p>
 *
 * <h3>Not a production mode</h3>
 * <p>{@link #supportedInProduction()} is {@code false}. A refresh token can be revoked by a Conditional
 * Access change, a password reset or a tenant policy, and recovery needs a human. That is acceptable for a
 * demonstration and is not acceptable for an unattended service, where app-only credentials remain the
 * right answer.</p>
 */
final class DeviceCodeTokenProvider implements GraphTokenProvider {

    private static final Logger log = Logger.getLogger(DeviceCodeTokenProvider.class.getName());

    private final PublicClientApplication application;
    private final Set<String> scopes;
    private final Path cachePath;
    private final String description;

    DeviceCodeTokenProvider(String authority, String clientId, List<String> scopes, Path cachePath) {
        this(authority, clientId, scopes, cachePath, null);
    }

    /**
     * @param transport test-only seam, matching the one on the client-credentials provider. msal4j refuses
     *                  an authority that is not {@code https}, so driving the real library against a
     *                  loopback endpoint is the only way to assert the wire form of these requests.
     */
    DeviceCodeTokenProvider(String authority,
                            String clientId,
                            List<String> scopes,
                            Path cachePath,
                            com.microsoft.aad.msal4j.IHttpClient transport) {
        if (cachePath == null) {
            throw new GraphException("sharepoint.auth-mode is device-code but "
                    + SharePointConnectorPlugin.TOKEN_CACHE_PATH_SETTING + " is not set, so there is nowhere "
                    + "to read the refresh token a sign-in produced");
        }
        this.scopes = SharePointDeviceLogin.effectiveScopes(scopes);
        this.cachePath = cachePath;
        this.application = SharePointDeviceLogin.application(authority, clientId, cachePath, transport);
        this.description = "delegated device-code as the cached account at " + cachePath
                + ", silent refresh only, scopes " + this.scopes;
        log.info("SharePoint connector authenticating with " + description);
    }

    @Override
    public String token() {
        IAccount account = cachedAccount();
        try {
            IAuthenticationResult result = application
                    .acquireTokenSilently(SilentParameters.builder(scopes, account).build())
                    .get();
            if (result == null || result.accessToken() == null || result.accessToken().isBlank()) {
                throw new GraphException("Entra ID returned no access token for " + description);
            }
            return result.accessToken();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new GraphException("Interrupted while refreshing a Graph token", e);
        } catch (ExecutionException | CompletionException e) {
            Throwable cause = e.getCause() == null ? e : e.getCause();
            if (cause instanceof MsalInteractionRequiredException) {
                throw signInRequired("the stored refresh token is no longer accepted (" + cause.getMessage()
                        + ")");
            }
            throw new GraphException("Could not refresh the Graph token for " + account.username() + ": "
                    + cause.getMessage(), cause);
        } catch (java.net.MalformedURLException e) {
            throw new GraphException("The configured authority is not a usable URL: " + e.getMessage(), e);
        }
    }

    /**
     * The one account the cache holds.
     *
     * <p>Zero accounts is the ordinary first-run state and the one an operator most needs a clear message
     * for, because the cache file may well exist and simply be empty.</p>
     */
    private IAccount cachedAccount() {
        Set<IAccount> accounts;
        try {
            accounts = application.getAccounts().join();
        } catch (CompletionException e) {
            Throwable cause = e.getCause() == null ? e : e.getCause();
            if (cause instanceof GraphException graphException) {
                throw graphException;
            }
            throw new GraphException("Could not read the SharePoint token cache at " + cachePath + ": "
                    + cause.getMessage(), cause);
        }
        if (accounts.isEmpty()) {
            throw signInRequired("it holds no signed-in account");
        }
        // More than one is not an error worth refusing over: the connector runs as one identity, and taking
        // the first is deterministic because msal4j orders the set consistently for a given cache.
        return accounts.iterator().next();
    }

    private GraphException signInRequired(String why) {
        return new GraphException("No usable delegated token: the SharePoint token cache at " + cachePath
                + " cannot be used because " + why + ". Sign in again on the host with "
                + SharePointDeviceLogin.COMMAND_HINT
                + " and restart this service. Nothing in the container can complete an interactive sign-in.");
    }

    @Override
    public String describe() {
        return description;
    }

    @Override
    public String mode() {
        return "device-code";
    }

    /**
     * The account the cache holds, read locally, or {@code null} when it holds none.
     *
     * <p>msal4j serves {@code getAccounts()} from the cache it already loaded, so this costs a file read at
     * worst and never a network call -- which is required, because a status endpoint may poll it.</p>
     *
     * <p>Swallows its own failure rather than propagating. An unreadable cache is exactly the state this is
     * meant to describe, and a status call that threw while reporting on a broken credential would replace a
     * legible screen with a stack trace.</p>
     */
    @Override
    public String identity() {
        try {
            return cachedAccount().username();
        } catch (RuntimeException e) {
            return null;
        }
    }

    /**
     * Whether a silent refresh could happen now, judged by whether the cache names an account.
     *
     * <p>Deliberately not judged by attempting one. A silent acquisition is a round trip to Entra ID, and
     * spending that on every status poll would make asking about the credential as costly as using it -- and
     * would make the status endpoint fail whenever the directory was briefly unreachable, which is a different
     * problem wearing this one's clothes.</p>
     *
     * <p>So this is the cheap half of the question. A cache holding an account whose refresh token has been
     * revoked still reads as usable here, and the sync is what discovers otherwise. That is the honest limit of
     * what can be known without paying, and the alternative is worse.</p>
     */
    @Override
    public boolean usable() {
        try {
            cachedAccount();
            return true;
        } catch (RuntimeException e) {
            return false;
        }
    }

    /**
     * The command that fixes it, and nothing else.
     *
     * <p>Notably <em>not</em> the message {@link #signInRequired} builds: that one names the cache path, which
     * is the most useful thing in a log line and must not reach a browser. It names a file worth attacking, and
     * the screen showing this is reachable from one.</p>
     */
    @Override
    public String remedy() {
        return usable() ? null
                : "Sign in again on the host with " + SharePointDeviceLogin.COMMAND_HINT
                        + ", then restart this service.";
    }

    @Override
    public boolean supportedInProduction() {
        return false;
    }
}
