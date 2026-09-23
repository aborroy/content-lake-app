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

    /**
     * The mode's name as a setting value, for a screen rather than for a log line.
     *
     * <p>Distinct from {@link #describe()} on purpose: that is prose for an engineer reading a log, this is the
     * token an operator would put in {@code sharepoint.auth-mode}. A screen that showed the sentence could not
     * tell the reader which setting produced it.</p>
     */
    String mode();

    /**
     * Who this provider is acting as, or {@code null} for a mode with no user.
     *
     * <p>An application credential genuinely has no account, which is not the same as an unknown one, so
     * {@code null} here means "there is no user" rather than "could not tell".</p>
     *
     * <p><strong>Must not acquire or refresh a token.</strong> It is read by a status endpoint that may be
     * polled, and a call that reached the network would make asking about a credential as expensive as using
     * one.</p>
     */
    default String identity() {
        return null;
    }

    /**
     * Whether {@link #token()} would succeed right now with no human involved.
     *
     * <p>Only a mode holding a credential that can lapse has anything to report; anything configured up front
     * either worked at startup or the container failed. Same prohibition as {@link #identity()}: answer from
     * local state, never from a round trip.</p>
     */
    default boolean usable() {
        return true;
    }

    /**
     * What an operator should do when {@link #usable()} is {@code false}, or {@code null} when nothing.
     *
     * <p>An instruction, not a diagnostic. It reaches a browser, so it must name no file holding a credential,
     * which rules out the message the provider's own exception carries.</p>
     */
    default String remedy() {
        return null;
    }
}
