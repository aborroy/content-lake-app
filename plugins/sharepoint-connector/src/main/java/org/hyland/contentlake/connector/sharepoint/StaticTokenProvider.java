package org.hyland.contentlake.connector.sharepoint;

import java.util.logging.Logger;

/**
 * Hands out a token the deployment supplied, acquiring nothing.
 *
 * <p>Two uses, both development ones. Against the local mock Graph service there is no Entra ID to
 * authenticate to, and standing one up would mean TLS and an OIDC discovery document in the mock for no
 * gain, since what a mock run tests is this connector's Graph protocol handling and not Microsoft's
 * token endpoint. Against a real tenant it is the way a developer validates ACL mapping in their own
 * OneDrive, where a delegated token from the device code flow is the only way to see a complete
 * permission set and no app registration may be available at all.</p>
 *
 * <p>Not a deployment mode. A token acquired this way expires in about an hour with nothing here able to
 * refresh it, so a container that restarts comes back unable to authenticate. That is why
 * {@link #supportedInProduction()} is {@code false} and why this logs a warning on construction rather
 * than only on failure: the failure arrives an hour after the run that looked fine.</p>
 */
public final class StaticTokenProvider implements GraphTokenProvider {

    private static final Logger log = Logger.getLogger(StaticTokenProvider.class.getName());

    private final String token;

    public StaticTokenProvider(String token) {
        if (token == null || token.isBlank()) {
            throw new GraphException("sharepoint.auth-mode is static-token but sharepoint.access-token is "
                    + "empty; supply a bearer token or use client-credentials");
        }
        this.token = token.trim();
        log.warning("SharePoint connector is using a static bearer token. This is a development mode: the "
                + "token cannot be refreshed, so this run stops working when it expires. Use "
                + "sharepoint.auth-mode=client-credentials for any deployment.");
    }

    @Override
    public String token() {
        return token;
    }

    @Override
    public String describe() {
        return "static bearer token (development only, not refreshable)";
    }

    @Override
    public String mode() {
        return "static-token";
    }

    /**
     * Reported usable, which is a statement about configuration rather than about the token.
     *
     * <p>A pasted token either works or it has expired, and there is no way to tell which without spending a
     * Graph call -- which this must not do. Claiming it is unusable would be wrong on a working run; the honest
     * signal about this mode is {@link #supportedInProduction()}, which is already {@code false}.</p>
     */
    @Override
    public boolean usable() {
        return true;
    }

    @Override
    public boolean supportedInProduction() {
        return false;
    }
}
