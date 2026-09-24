package org.hyland.contentlake.security;

import java.util.Locale;

/**
 * One username, and the source type it is a username in.
 *
 * <p>A caller is not one name. A deployment with several content sources has several directories, and the
 * same person is a different principal in each: {@code alice} in Alfresco, {@code alice@example.com} in
 * Entra ID. An ACL stores whichever form its own source uses, so matching one against another is how a
 * caller silently reads nothing, or reads someone else's documents.</p>
 *
 * <h3>An untyped identity is not a missing one</h3>
 *
 * <p>{@code sourceType == null} means "this name is not scoped to one source type", which is exactly what
 * HTTP Basic against a repository establishes: the caller proved one credential, and the same login string
 * is all we know them by anywhere. That is the ordinary single-source case and it must keep behaving as it
 * does today, so an untyped identity answers for every source type rather than for none.</p>
 *
 * @param sourceType the source type as {@code cin_sourceId} stores it ({@code "alfresco"}, {@code "nuxeo"}),
 *                   lower-cased, or {@code null} when the name is not scoped to a type
 * @param username   the caller's name in that source, never null or blank
 */
public record SourceIdentity(String sourceType, String username) {

    public SourceIdentity {
        if (username == null || username.isBlank()) {
            throw new IllegalArgumentException("A source identity needs a username");
        }
        username = username.trim();
        sourceType = sourceType == null || sourceType.isBlank()
                ? null
                : sourceType.trim().toLowerCase(Locale.ROOT);
    }

    /** A name that is not scoped to a source type, and therefore answers for all of them. */
    public static SourceIdentity untyped(String username) {
        return new SourceIdentity(null, username);
    }

    public static SourceIdentity of(String sourceType, String username) {
        return new SourceIdentity(sourceType, username);
    }

    public boolean isUntyped() {
        return sourceType == null;
    }
}
