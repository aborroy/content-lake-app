package org.hyland.contentlake.spi;

import java.time.OffsetDateTime;

/**
 * How a source is authenticating, in terms an operator screen can render.
 *
 * <p>This exists because a delegated credential expires. Under an interactive sign-in the ingest identity is a
 * refresh token a human minted, and when it lapses the only symptom is a failed job whose log explains itself
 * in terms of a token cache the operator has never heard of. The point of this record is that the state is
 * visible <em>before</em> a sync is attempted rather than diagnosed afterwards.</p>
 *
 * <p><strong>Everything here is safe to put on a screen, and that is a constraint rather than an
 * observation.</strong> No token, no refresh token, no client secret, and no path to the file holding any of
 * them. A cache path is deliberately excluded even though it is the most useful thing in the connector's own
 * log line: it names a file worth attacking, and the screen that would show it is reachable from a browser.
 * {@link #remedy} carries the action instead, which is what the operator actually needs.</p>
 *
 * <p><strong>Answering must be cheap.</strong> A caller is a status endpoint that may be polled, so an
 * implementation must not acquire a token, call the source, or block on a network round trip to fill this in.
 * Anything only a remote call could establish belongs in a {@code null} field, not in a slow one: a status
 * response that hangs because the directory is unreachable is worse than one that admits it does not know.</p>
 *
 * @param mode                  the authentication mode in the connector's own vocabulary, for example
 *                              {@code client-credentials} or {@code device-code}. Never {@code null}: a
 *                              connector reporting auth state at all knows how it authenticates
 * @param supportedInProduction {@code false} for a mode that is a development shortcut, so a screen can say so
 *                              rather than leaving a laptop convenience to reach production unremarked
 * @param identity              the account the source is acting as, or {@code null} for a mode that has none.
 *                              An application identity genuinely has no user, which is not the same as
 *                              "unknown", and a screen should be able to tell those apart
 * @param lastRefreshedAt       when the stored credential was last renewed, or {@code null} when that is not
 *                              knowable without a call. Frequently {@code null}, and that is honest: most
 *                              token libraries do not expose it
 * @param usable                whether a sync could obtain a credential right now with no human involved. The
 *                              one field a screen should lead with, since it is the difference between "this
 *                              will work" and "this will fail in a way that looks like a connector bug"
 * @param remedy                what an operator should do when {@link #usable} is {@code false}, or
 *                              {@code null} when there is nothing to do. An instruction, never a diagnostic:
 *                              it must name no file, no token and no secret
 */
public record SourceAuthState(
        String mode,
        boolean supportedInProduction,
        String identity,
        OffsetDateTime lastRefreshedAt,
        boolean usable,
        String remedy) {

    /**
     * A usable credential with no user behind it, such as an application registration.
     *
     * <p>{@code supportedInProduction} is a parameter rather than assumed. It was briefly hardcoded to
     * {@code true} here on the reasoning that an application credential is the production mode, and that
     * quietly discarded the answer of a development mode which also has no user -- a pasted bearer token has no
     * account either, and is emphatically not a deployment mode. A factory that invents this field hides
     * exactly the fact it exists to surface.</p>
     */
    public static SourceAuthState withoutUser(String mode, boolean supportedInProduction) {
        return new SourceAuthState(mode, supportedInProduction, null, null, true, null);
    }

    /** A working interactive sign-in, acting as the named account. */
    public static SourceAuthState signedInAs(String mode, String identity, OffsetDateTime lastRefreshedAt,
                                             boolean supportedInProduction) {
        return new SourceAuthState(mode, supportedInProduction, identity, lastRefreshedAt, true, null);
    }

    /**
     * A credential that cannot be used without a human, which is the state worth surfacing early.
     *
     * <p>{@code identity} may still be known -- a cache can name the account it holds and be unable to renew
     * its token -- so it is kept rather than blanked, because "signed in as this person, and it has lapsed" is
     * a more actionable thing to render than "not usable".</p>
     */
    public static SourceAuthState needsSignIn(String mode, String identity, String remedy,
                                             boolean supportedInProduction) {
        return new SourceAuthState(mode, supportedInProduction, identity, null, false, remedy);
    }
}
