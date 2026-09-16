package org.hyland.contentlake.security;

import java.util.List;

/**
 * Expands a caller's group membership against the directory of one content source.
 *
 * <p>An ACL stored in {@code sys_racl} names groups, so a caller matches a group-granted document only if
 * something can say which groups they are in. That answer lives in the source system's own directory, and
 * there is one implementation of this interface per directory a deployment can ask. A source with no
 * implementation is not broken: its {@code __Everyone__} documents and the documents granted to the caller
 * by name stay retrievable, and only its group-granted ones are invisible. Filtering less would be
 * over-sharing, so the gap is reported rather than closed by guessing.</p>
 *
 * <p>This is the query side of security and deliberately not part of {@code content-lake-spi}, which is the
 * ingest contract. A connector that ingests ACLs correctly needs no resolver to do so; a deployment that
 * wants those ACLs to be actionable needs one here.</p>
 *
 * <h3>The three answers, which are not interchangeable</h3>
 *
 * <ul>
 *   <li>A list of groups: the caller is known to this directory and these are their groups. Group names must
 *       already be in the form the source's ACLs store, since that is what they are compared against.</li>
 *   <li>{@code null}: this directory has no such identity. That is not a failure. The caller keeps the
 *       source, with their default authorities alone. A user authenticated against one repository and absent
 *       from another is the ordinary case in a multi-source deployment.</li>
 *   <li>A thrown exception: the directory could not be asked. That <em>is</em> a failure, and
 *       {@link GroupResolutionFailurePolicy} decides what it costs the caller.</li>
 * </ul>
 *
 * <p>Confusing the last two is a defect in either direction: throwing for an unknown user costs an
 * authenticated caller a whole source on every query, and returning {@code null} for an unreachable
 * directory silently downgrades a fail-closed deployment to a degraded one.</p>
 *
 * <p>An empty list is a legitimate answer distinct from {@code null}: the caller is known here and is in no
 * groups.</p>
 */
public interface SourceGroupResolver {

    /**
     * The source type this resolver answers for, as {@code cin_sourceId} stores it: {@code "alfresco"},
     * {@code "nuxeo"}, and so on. Matched case-insensitively, and exactly one resolver may claim a type.
     */
    String sourceType();

    /**
     * The caller's groups in this directory, or {@code null} when the directory holds no such identity.
     *
     * @throws RuntimeException when the directory could not be asked
     */
    List<String> resolveGroups(String username);
}
