package org.hyland.contentlake.rag.service;

import lombok.extern.slf4j.Slf4j;
import org.hyland.contentlake.client.HxprService;
import org.hyland.contentlake.rag.security.SourceGroupResolverRegistry;
import org.hyland.contentlake.security.AclFilterBuilder;
import org.hyland.contentlake.security.CallerIdentities;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

/**
 * The ACL-scoped query for one caller, over every source they are authenticated for.
 *
 * <p>One implementation, used by both search services. It replaced a near-verbatim copy of the same eight
 * methods in each: adding a source type meant finding every copy, and fixing one copy was a silent
 * half-fix. The same argument {@code SourceGroupResolverRegistry} makes for resolving authorities in one
 * place applies to building the clause from them.</p>
 *
 * <p>Lives in this package because {@link PermissionSourceCatalog} is package-private here. Do not widen
 * that to move this class somewhere tidier: the catalogue's visibility is what keeps a caller from holding
 * a second copy of the configured source ids that could disagree with the first.</p>
 *
 * <h3>Two skips that are not the same skip</h3>
 *
 * <p>A source is dropped from the filter when the caller has no identity for it, and also when their
 * authorities could not be resolved. Both are fail-closed, and neither may be turned into a default: an
 * unresolved directory yields an empty list, while a source type with no resolver at all yields the
 * caller's default authorities, which is never empty. Substituting one for the other would undo the
 * fail-closed decision {@code SourceGroupResolverRegistry} took.</p>
 */
@Slf4j
@Component
class PermissionFilterBuilder {

    private final PermissionSourceCatalog sourceCatalog;
    private final SourceGroupResolverRegistry groupResolvers;

    @Autowired
    PermissionFilterBuilder(PermissionSourceCatalog sourceCatalog,
                            SourceGroupResolverRegistry groupResolvers) {
        this.sourceCatalog = sourceCatalog;
        this.groupResolvers = groupResolvers;
    }

    /**
     * A builder with no group resolvers, for a service constructed outside an application context.
     *
     * <p>Mirrors {@link SourceGroupResolverRegistry#withoutResolvers()} and exists for the same reason:
     * every source then resolves to the caller's default authorities, which is the answer a deployment
     * with no directory client for that source gets anyway.</p>
     */
    static PermissionFilterBuilder withoutGroupResolvers(HxprService hxprService) {
        return new PermissionFilterBuilder(new PermissionSourceCatalog(hxprService),
                SourceGroupResolverRegistry.withoutResolvers());
    }

    /**
     * What a search service holds as {@code @Value} fields, passed per call.
     *
     * <p>Per call rather than injected here for the reason {@link PermissionSourceCatalog.Configured}
     * exists: a second copy of the configured source ids that could drift from the service's own is a
     * defect waiting to happen.</p>
     */
    record Settings(PermissionSourceCatalog.Configured sources, boolean adminBypassEnabled) {
    }

    /**
     * The complete ACL-scoped HXQL query for this caller.
     *
     * @param identities       the caller's identity per source type
     * @param sourceType       the source type the request asked for, or {@code null} for all
     * @param additionalFilter the caller's own filter, ANDed with the ACL predicate
     */
    String query(CallerIdentities identities, Settings settings, String sourceType,
                 String additionalFilter) {
        List<String> sourceIds = sourceCatalog.resolve(settings.sources(), sourceType, additionalFilter);

        List<String> sourceClauses = new ArrayList<>();
        for (String sourceId : sourceIds) {
            String type = sourceCatalog.sourceType(settings.sources(), sourceId);
            String username = identities.usernameFor(type);
            if (username == null) {
                // No identity for this source, so exclude it from results entirely.
                continue;
            }
            List<String> authorities = authorities(username, sourceId, settings);
            if (authorities.isEmpty()) {
                // Unresolved rather than empty. See the class note on why this is not defaulted.
                log.warn("Excluding source {} from the permission filter for user {}: no authorities resolved",
                        sourceId, username);
                continue;
            }
            sourceClauses.add(AclFilterBuilder.sourcePermissionClause(
                    sourceId,
                    sourceCatalog.qualify(settings.sources(), sourceId),
                    authorities,
                    settings.adminBypassEnabled() && PermissionSourceCatalog.ALFRESCO.equals(type)));
        }

        log.debug("Permission filter for {} over sourceIds={}", identities.describe(), sourceIds);

        if (sourceClauses.isEmpty()) {
            log.warn("No permission clauses resolved (caller={}, sourceType={}, filter={})",
                    identities.describe(), sourceType, additionalFilter);
        }

        return AclFilterBuilder.query(sourceClauses, additionalFilter);
    }

    /**
     * The caller's authorities on one source: themselves, everyone, and whatever that source's directory
     * says about their groups. Empty when the directory could not be asked and the failure policy is
     * fail-closed.
     */
    List<String> authorities(String username, String sourceId, Settings settings) {
        String sourceType = sourceCatalog.sourceType(settings.sources(), sourceId);
        return groupResolvers.authorities(username, sourceId, sourceType);
    }
}
