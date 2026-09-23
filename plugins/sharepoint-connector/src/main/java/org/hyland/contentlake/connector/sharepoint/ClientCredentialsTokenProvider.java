package org.hyland.contentlake.connector.sharepoint;

import com.microsoft.aad.msal4j.ClientCredentialFactory;
import com.microsoft.aad.msal4j.ClientCredentialParameters;
import com.microsoft.aad.msal4j.ConfidentialClientApplication;
import com.microsoft.aad.msal4j.IAuthenticationResult;
import com.microsoft.aad.msal4j.IClientCredential;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.logging.Logger;

/**
 * App-only tokens from Entra ID, which is how this connector authenticates in a deployment.
 *
 * <p>App-only rather than delegated because a batch pass has no user in front of it: there is nobody to
 * complete an interactive prompt when a container restarts at 03:00, and the ROPC alternative of putting
 * a username and password in configuration is documented by Microsoft as incompatible with MFA and
 * unsupported for federated accounts, which closes it in any corporate tenant.</p>
 *
 * <p>The consequence for ACLs is worth stating where it will be read: an app-only identity is not a user,
 * so there is no account whose access could stand in for a document's. That is why the connector's ACL
 * fallback has no {@code sync-account} option the way the CMIS one does.</p>
 *
 * <h3>Certificate over secret</h3>
 * <p>Both are supported and the certificate path exists because a client secret expires on a date nobody
 * diarises and takes the crawl down when it does. Supply either {@code client-secret} or
 * {@code certificate-path} (a PKCS#12 file) with {@code certificate-password}; supplying both is a
 * configuration error rather than a precedence question, because silently preferring one would leave an
 * operator convinced they had rotated a credential that was never used.</p>
 */
public final class ClientCredentialsTokenProvider implements GraphTokenProvider {

    private static final Logger log = Logger.getLogger(ClientCredentialsTokenProvider.class.getName());

    /**
     * The app-only scope. {@code .default} means "every application permission already consented for this
     * app", which is the only scope shape client credentials accepts: consent is granted by an
     * administrator ahead of time rather than requested per call.
     */
    private static final String GRAPH_DEFAULT_SCOPE = "https://graph.microsoft.com/.default";

    private final ConfidentialClientApplication application;
    private final ClientCredentialParameters parameters;
    private final String description;

    /**
     * @param authority     Entra ID authority, normally {@code https://login.microsoftonline.com/<tenant>}
     * @param clientId      the application (client) id of the registered app
     * @param clientSecret  client secret, or {@code null} when using a certificate
     * @param certificate   PKCS#12 file holding the client certificate, or {@code null} when using a secret
     * @param certificatePassword password for that file, or {@code null} for an unprotected one
     * @param scope         scope to request, defaulting to Graph's {@code .default} when blank. Configurable
     *                      only so a sovereign cloud can be pointed at its own Graph
     */
    public ClientCredentialsTokenProvider(String authority,
                                          String clientId,
                                          String clientSecret,
                                          Path certificate,
                                          String certificatePassword,
                                          String scope) {
        this(authority, clientId, clientSecret, certificate, certificatePassword, scope, null);
    }

    /**
     * Test seam: replaces msal4j's transport, not this connector's behaviour.
     *
     * <p>It exists because msal4j rejects an authority that is not {@code https} -- measured, it throws
     * {@code authority should use the 'https' scheme} at {@code build()}, and {@code validateAuthority(false)}
     * skips instance discovery rather than that check. So a token endpoint cannot be mocked by pointing the
     * authority at localhost. Supplying the transport instead means a test drives the real msal4j: the real
     * grant, the real response parsing and the real token cache, answered from a local HTTP server.</p>
     *
     * <p>Production passes {@code null} and msal4j uses its own client. Nothing reads this from
     * configuration, so there is no way for a deployment to end up on a stubbed transport.</p>
     */
    ClientCredentialsTokenProvider(String authority,
                                   String clientId,
                                   String clientSecret,
                                   Path certificate,
                                   String certificatePassword,
                                   String scope,
                                   com.microsoft.aad.msal4j.IHttpClient transport) {
        boolean hasSecret = clientSecret != null && !clientSecret.isBlank();
        boolean hasCertificate = certificate != null;
        if (hasSecret && hasCertificate) {
            throw new GraphException("Both sharepoint.client-secret and sharepoint.certificate-path are "
                    + "set; supply exactly one, so that rotating a credential cannot leave the other in "
                    + "silent use");
        }
        if (!hasSecret && !hasCertificate) {
            throw new GraphException("sharepoint.auth-mode is client-credentials but neither "
                    + "sharepoint.client-secret nor sharepoint.certificate-path is set");
        }

        IClientCredential credential = hasSecret
                ? ClientCredentialFactory.createFromSecret(clientSecret)
                : certificateCredential(certificate, certificatePassword);
        this.description = hasSecret
                ? "client credentials with a secret, as " + clientId + " at " + authority
                : "client credentials with the certificate at " + certificate + ", as " + clientId
                        + " at " + authority;

        try {
            ConfidentialClientApplication.Builder builder =
                    ConfidentialClientApplication.builder(clientId, credential).authority(authority);
            if (transport != null) {
                builder.httpClient(transport);
            }
            this.application = builder.build();
        } catch (Exception e) {
            // A malformed authority is the common case and its message names only the URL, so it is
            // repeated here with the setting that produced it.
            throw new GraphException("Could not build the Entra ID client for authority '" + authority
                    + "' (sharepoint.tenant-id, or sharepoint.authority if set): " + e.getMessage(), e);
        }

        String effectiveScope = scope == null || scope.isBlank() ? GRAPH_DEFAULT_SCOPE : scope.trim();
        this.parameters = ClientCredentialParameters.builder(Set.of(effectiveScope)).build();
        log.info("SharePoint connector authenticating with " + description + " for scope " + effectiveScope);
    }

    private static IClientCredential certificateCredential(Path certificate, String password) {
        if (!Files.isReadable(certificate)) {
            throw new GraphException("sharepoint.certificate-path '" + certificate
                    + "' does not exist or cannot be read by this process");
        }
        try (InputStream pkcs12 = Files.newInputStream(certificate)) {
            return ClientCredentialFactory.createFromCertificate(pkcs12,
                    password == null ? "" : password);
        } catch (Exception e) {
            // One catch on purpose. A wrong password surfaces as an IOException from the keystore reader,
            // not as a distinct exception type, so splitting on the type produced "could not read the
            // file" for a file that was read perfectly well. Both possibilities are named instead.
            throw new GraphException("Could not load the client certificate at " + certificate
                    + "; check that it is a PKCS#12 file and that sharepoint.certificate-password matches",
                    e);
        }
    }

    /**
     * {@inheritDoc}
     *
     * <p>msal4j holds an application token cache and returns a cached token while one is valid, including
     * its own expiry skew, so this is called per request without spending the tenant's budget on
     * authentication.</p>
     */
    @Override
    public String token() {
        try {
            IAuthenticationResult result = application.acquireToken(parameters).get();
            if (result == null || result.accessToken() == null || result.accessToken().isBlank()) {
                throw new GraphException("Entra ID returned no access token for " + description);
            }
            return result.accessToken();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new GraphException("Interrupted while acquiring a Graph token", e);
        } catch (ExecutionException | CompletionException e) {
            Throwable cause = e.getCause() == null ? e : e.getCause();
            // Almost always one of: the app has no application permission consented, the secret has
            // expired, or the tenant id is wrong. All three arrive as the same exception type, so the
            // provider's own description is the only context the log will carry.
            throw new GraphException("Could not acquire a Graph token using " + description + ": "
                    + cause.getMessage(), cause);
        }
    }

    @Override
    public String describe() {
        return description;
    }

    @Override
    public String mode() {
        return "client-credentials";
    }

    /**
     * {@code null}, because an application identity has no user, rather than because one could not be found.
     *
     * <p>The distinction matters to a screen: "acting as the application" and "signed in as nobody we could
     * determine" look identical if both are blank, and only the second is a problem.</p>
     */
    @Override
    public String identity() {
        return null;
    }
}
