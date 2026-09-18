package org.hyland.contentlake.connector.sharepoint;

/**
 * Supplies the bearer token every Graph call carries.
 *
 * <p>This is one of exactly two seams that let the connector run against a local mock and against
 * SharePoint Online with no code difference; the other is the Graph base URL. Everything between them is
 * real Graph protocol handling, which is what makes a mock run evidence about the cloud rather than
 * evidence about the mock.</p>
 *
 * <p>Implementations must be safe for concurrent use and must cache: {@link #token()} is called once per
 * request, and a provider that acquired a new token each time would spend the tenant's throttling budget
 * on authentication.</p>
 */
public interface GraphTokenProvider {

    /**
     * A bearer token valid now, acquiring or refreshing one if needed.
     *
     * @throws GraphException when a token cannot be acquired, which is a configuration or tenant problem
     *                        and not something a retry will fix
     */
    String token();

    /**
     * How this provider authenticates, for a startup log line and for the connector's status.
     *
     * <p>Must never include the token, a client secret, or a certificate's private material. It is
     * written to logs that are routinely pasted into issues.</p>
     */
    String describe();

    /**
     * Whether this provider is a supported deployment mode.
     *
     * <p>{@code false} exists so the host can say so loudly at startup rather than a developer's laptop
     * shortcut reaching production unremarked.</p>
     */
    default boolean supportedInProduction() {
        return true;
    }
}
