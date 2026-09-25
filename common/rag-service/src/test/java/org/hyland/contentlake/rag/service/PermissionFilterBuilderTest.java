package org.hyland.contentlake.rag.service;

import org.hyland.contentlake.client.HxprService;
import org.hyland.contentlake.model.HxprTermsAggregationResult;
import org.hyland.contentlake.rag.security.SourceGroupResolverRegistry;
import org.hyland.contentlake.security.CallerIdentities;
import org.hyland.contentlake.security.GroupResolutionFailurePolicy;
import org.hyland.contentlake.security.SourceGroupResolver;
import org.hyland.contentlake.security.SourceIdentity;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.when;

/**
 * The one place the per-source ACL clause is built, so the one place these semantics are asserted.
 *
 * <p>Both search services used to carry a copy of this logic and a copy of these assertions. The interesting
 * cases here are the ones neither copy could express: a caller with more than two identities, and a source
 * the caller has no identity for at all.</p>
 */
@ExtendWith(MockitoExtension.class)
class PermissionFilterBuilderTest {

    private static final PermissionSourceCatalog.Configured SOURCES =
            PermissionSourceCatalog.Configured.of("test-repo", "nuxeo-demo", null);

    @Mock
    private HxprService hxprService;

    private PermissionSourceCatalog catalogue;

    @BeforeEach
    void setUp() {
        catalogue = new PermissionSourceCatalog(hxprService);
    }

    private PermissionFilterBuilder builderWith(SourceGroupResolver... resolvers) {
        return builderWith(GroupResolutionFailurePolicy.FAIL_CLOSED, resolvers);
    }

    /** No cache, so each test's resolver answer is the one that is read. */
    private PermissionFilterBuilder builderWith(GroupResolutionFailurePolicy policy,
                                                SourceGroupResolver... resolvers) {
        return new PermissionFilterBuilder(catalogue,
                new SourceGroupResolverRegistry(List.of(resolvers), policy, 0L, 0L, null));
    }

    private static PermissionFilterBuilder.Settings settings(boolean adminBypass) {
        return new PermissionFilterBuilder.Settings(SOURCES, adminBypass);
    }

    /**
     * Settings naming only an Alfresco source. Needed because a configured source id is resolved whether or
     * not the index holds it, so the default settings always contribute a Nuxeo source and a test about one
     * source in isolation would silently be about two.
     */
    private static PermissionFilterBuilder.Settings alfrescoOnly() {
        return new PermissionFilterBuilder.Settings(
                PermissionSourceCatalog.Configured.of("test-repo", null, null), false);
    }

    private static SourceGroupResolver resolver(String sourceType, Supplier<List<String>> groups) {
        return new SourceGroupResolver() {
            @Override
            public String sourceType() {
                return sourceType;
            }

            @Override
            public List<String> resolveGroups(String username) {
                return groups.get();
            }
        };
    }

    /** Stubs the terms aggregation over {@code cin_sourceId} that source discovery runs. */
    private void stubIndexedSources(String... qualifiedSourceIds) {
        HxprTermsAggregationResult aggregation = new HxprTermsAggregationResult();
        aggregation.setAggregationsBuckets(Arrays.stream(qualifiedSourceIds).map(key -> {
            HxprTermsAggregationResult.Bucket bucket = new HxprTermsAggregationResult.Bucket();
            bucket.setKey(key);
            bucket.setDocCount(1L);
            return bucket;
        }).toList());
        when(hxprService.termsAggregation(isNull(), eq("cin_sourceId"), isNull(), anyInt()))
                .thenReturn(aggregation);
    }

    @Nested
    class OneIdentity {

        @Test
        void scopesEverySourceToThatName() {
            stubIndexedSources("alfresco:test-repo", "nuxeo:nuxeo-demo");
            PermissionFilterBuilder builder = builderWith();

            String filter = builder.query(CallerIdentities.single("alice"), settings(false), null, null);

            assertThat(filter).contains("sys_racl = 'u:alice_#_test-repo'");
            assertThat(filter).contains("sys_racl = 'u:alice_#_nuxeo-demo'");
        }

        @Test
        void carriesTheEveryoneAuthorityUnnamespaced() {
            stubIndexedSources("alfresco:test-repo");

            String filter = builderWith().query(
                    CallerIdentities.single("alice"), settings(false), null, null);

            assertThat(filter).contains("sys_racl = '__Everyone__'");
            assertThat(filter).doesNotContain("GROUP_EVERYONE_#_");
        }
    }

    @Nested
    class TwoIdentities {

        private CallerIdentities twoSessions() {
            return CallerIdentities.builder()
                    .fallback(SourceIdentity.of("alfresco", "alice"))
                    .add(SourceIdentity.of("nuxeo", "bob"))
                    .build();
        }

        @Test
        void scopesEachSourceToItsOwnName() {
            stubIndexedSources("alfresco:test-repo", "nuxeo:nuxeo-demo");

            String filter = builderWith().query(twoSessions(), settings(false), null, null);

            assertThat(filter).contains("sys_racl = 'u:alice_#_test-repo'");
            assertThat(filter).contains("sys_racl = 'u:bob_#_nuxeo-demo'");
            // Neither name may leak into the other's source.
            assertThat(filter).doesNotContain("sys_racl = 'u:bob_#_test-repo'");
            assertThat(filter).doesNotContain("sys_racl = 'u:alice_#_nuxeo-demo'");
        }

        @Test
        void sendsAThirdSourceTypeToTheFallback() {
            stubIndexedSources("alfresco:test-repo", "nuxeo:nuxeo-demo", "sharepoint:site-1");

            String filter = builderWith().query(twoSessions(), settings(false), null, null);

            // Preserved from `isNuxeoSource(id) ? nuxeoUser : alfrescoUser`, which had nowhere else to
            // send a third type.
            assertThat(filter).contains("sys_racl = 'u:alice_#_site-1'");
        }
    }

    @Nested
    class ThreeIdentities {

        @Test
        void scopesEachOfThreeSourcesToItsOwnName() {
            stubIndexedSources("alfresco:test-repo", "nuxeo:nuxeo-demo", "sharepoint:site-1");
            CallerIdentities threeWay = CallerIdentities.builder()
                    .fallback(SourceIdentity.of("alfresco", "alice"))
                    .add(SourceIdentity.of("nuxeo", "bob"))
                    .add(SourceIdentity.of("sharepoint", "alice@example.com"))
                    .build();

            String filter = builderWith().query(threeWay, settings(false), null, null);

            // The case neither of the two copies this replaced could express.
            assertThat(filter).contains("sys_racl = 'u:alice_#_test-repo'");
            assertThat(filter).contains("sys_racl = 'u:bob_#_nuxeo-demo'");
            assertThat(filter).contains("sys_racl = 'u:alice@example.com_#_site-1'");
            assertThat(filter).doesNotContain("sys_racl = 'u:alice_#_site-1'");
        }
    }

    @Nested
    class ASourceTheCallerHasNoIdentityFor {

        @Test
        void isDroppedRatherThanGivenTheOtherName() {
            stubIndexedSources("alfresco:test-repo", "nuxeo:nuxeo-demo");
            // No fallback, so Alfresco is a source this caller has no identity in at all.
            CallerIdentities nuxeoOnly = CallerIdentities.builder()
                    .add(SourceIdentity.of("nuxeo", "bob"))
                    .build();

            String filter = builderWith().query(nuxeoOnly, settings(false), null, null);

            assertThat(filter).contains("sys_racl = 'u:bob_#_nuxeo-demo'");
            assertThat(filter).doesNotContain("test-repo");
        }

        @Test
        void leavesNoClauseAtAllWhenNoSourceMatches() {
            stubIndexedSources("alfresco:test-repo");
            CallerIdentities nuxeoOnly = CallerIdentities.builder()
                    .add(SourceIdentity.of("nuxeo", "bob"))
                    .build();

            String filter = builderWith().query(nuxeoOnly, alfrescoOnly(), null, null);

            // Matches nothing rather than everything, which is the fail-closed direction.
            assertThat(filter).contains("__unresolved_permission_source__");
        }
    }

    @Nested
    class UnresolvedAuthorities {

        @Test
        void dropTheSourceRatherThanFallingBackToDefaults() {
            stubIndexedSources("alfresco:test-repo");
            PermissionFilterBuilder builder = builderWith(resolver("alfresco", () -> {
                throw new IllegalStateException("directory unreachable");
            }));

            String filter = builder.query(CallerIdentities.single("alice"), alfrescoOnly(), null, null);

            // Fail-closed: an unreachable directory costs the source, and must not be quietly replaced by
            // the caller's default authorities.
            assertThat(filter).contains("__unresolved_permission_source__");
            assertThat(filter).doesNotContain("sys_racl = 'u:alice_#_test-repo'");
        }

        @Test
        void keepTheSourceWhenThePolicyIsToDegrade() {
            stubIndexedSources("alfresco:test-repo");
            PermissionFilterBuilder builder = builderWith(GroupResolutionFailurePolicy.DEGRADE,
                    resolver("alfresco", () -> {
                        throw new IllegalStateException("directory unreachable");
                    }));

            String filter = builder.query(CallerIdentities.single("alice"), alfrescoOnly(), null, null);

            assertThat(filter).contains("sys_racl = 'u:alice_#_test-repo'");
        }

        @Test
        void areNotTheSameAsASourceTypeWithNoResolver() {
            stubIndexedSources("sample-directory:sample-directory");

            String filter = builderWith().query(
                    CallerIdentities.single("alice"), settings(false), null, null);

            // No resolver for this type yields default authorities, which are never empty, so the source
            // stays and its public and by-name documents remain retrievable.
            assertThat(filter).contains("sys_racl = 'u:alice_#_sample-directory'");
            assertThat(filter).contains("sys_racl = '__Everyone__'");
        }
    }

    /**
     * The CMIS source, whose group grants do not resolve, asserted as the behaviour it actually is.
     *
     * <p>CMIS has no {@code memberOf} in the specification and {@code CmisAclMapper} reads raw
     * {@code ace.getPrincipalId()} values, so it cannot tell a user from a group. No {@code SourceGroupResolver}
     * claims the type and none is written: a source no resolver claims already falls back to the caller's own
     * name plus {@code GROUP_EVERYONE}, which is never empty. The accepted outcome is therefore that a
     * group-granted CMIS document is <em>unmatched</em>, not that the source is dropped, and these cases are
     * what make the difference between those two verifiable rather than assumed.</p>
     *
     * <p>If a resolver is ever added for this type it must be mutually exclusive with this path rather than
     * additive: {@code SourceGroupResolverRegistry} throws at startup when two beans claim one source type.</p>
     */
    @Nested
    class ACmisSourceWithNoResolver {

        private static PermissionFilterBuilder.Settings cmisOnly(boolean adminBypass) {
            // No configured Alfresco or Nuxeo id, so the only source is the one discovered from the index.
            return new PermissionFilterBuilder.Settings(
                    PermissionSourceCatalog.Configured.of(null, null, null), adminBypass);
        }

        @Test
        void buildsAClauseFromTheCallersOwnNameAndEveryone() {
            stubIndexedSources("cmis:docbase-1");

            String filter = builderWith().query(
                    CallerIdentities.single("alice"), cmisOnly(false), null, null);

            assertThat(filter).contains("sys_racl = 'u:alice_#_docbase-1'");
            assertThat(filter).contains("sys_racl = '__Everyone__'");
        }

        @Test
        void carriesNoGroupTermAtAll() {
            stubIndexedSources("cmis:docbase-1");

            String filter = builderWith().query(
                    CallerIdentities.single("alice"), cmisOnly(false), null, null);

            // The limitation, stated as an assertion: nothing can say which CMIS groups a caller is in, so a
            // document granted only to a group they belong to is not matched by this clause.
            assertThat(filter).doesNotContain("g:GROUP_");
        }

        @Test
        void isNotDroppedFromTheFilter() {
            stubIndexedSources("cmis:docbase-1");

            String filter = builderWith().query(
                    CallerIdentities.single("alice"), cmisOnly(false), null, null);

            // The other half of the same point. Unresolved authorities drop a source and reach the sentinel;
            // no resolver at all keeps it, so its public and by-name documents stay retrievable.
            assertThat(filter).doesNotContain("__unresolved_permission_source__");
        }

        @Test
        void isNeverCoveredByTheAlfrescoAdminBypass() {
            stubIndexedSources("cmis:docbase-1");

            String filter = builderWith().query(
                    CallerIdentities.single("admin"), cmisOnly(true), null, null);

            // The bypass is keyed on PermissionSourceCatalog.ALFRESCO, so admin is ACL-filtered here like
            // anyone else. The E2E depends on this: a CMIS document must be granted to admin by name for an
            // administrator to retrieve it.
            assertThat(filter).contains("sys_racl = 'u:admin_#_docbase-1'");
            assertThat(filter).doesNotContain("cin_sourceId = 'cmis:docbase-1'");
        }
    }

    @Nested
    class AdminBypass {

        @Test
        void grantsFullAccessOnAnAlfrescoSource() {
            stubIndexedSources("alfresco:test-repo");
            PermissionFilterBuilder builder = builderWith(
                    resolver("alfresco", () -> List.of("GROUP_ALFRESCO_ADMINISTRATORS")));

            String filter = builder.query(CallerIdentities.single("admin"), settings(true), null, null);

            assertThat(filter).contains("cin_sourceId = 'alfresco:test-repo'");
        }

        @Test
        void neverReachesAnotherSourceTypeHoweverManyIdentitiesTheCallerHas() {
            stubIndexedSources("sharepoint:site-1");
            PermissionFilterBuilder builder = builderWith(
                    resolver("sharepoint", () -> List.of("GROUP_ALFRESCO_ADMINISTRATORS")));
            CallerIdentities twoWay = CallerIdentities.builder()
                    .fallback(SourceIdentity.of("alfresco", "admin"))
                    .add(SourceIdentity.of("sharepoint", "admin"))
                    .build();

            String filter = builder.query(twoWay, settings(true), null, null);

            // The bypass group is Alfresco's policy and grants nothing in another source.
            assertThat(filter).doesNotContain("cin_sourceId = 'sharepoint:site-1'");
            assertThat(filter).contains("sys_racl = 'u:admin_#_site-1'");
        }
    }

    /** Ported from the two service tests: which sources a filter covers, and how each is namespaced. */
    @Nested
    class SourceSelection {

        @Test
        void combinesTheCallersOwnFilterWithAnd() {
            String filter = builderWith().query(CallerIdentities.single("alice"), alfrescoOnly(),
                    null, "cin_sourceId = 'test-repo'");

            assertThat(filter).contains(" AND ");
            assertThat(filter).contains("cin_sourceId = 'test-repo'");
        }

        @Test
        void narrowsToASourcePinnedInTheCallersOwnFilter() {
            String filter = builderWith().query(CallerIdentities.single("alice"), settings(false),
                    null, "cin_sourceId = 'nuxeo:nuxeo-demo'");

            // An id-level pin in the caller's filter is a first-class way to scope to one source.
            assertThat(filter).contains("sys_racl = 'u:alice_#_nuxeo-demo'");
            assertThat(filter).doesNotContain("u:alice_#_test-repo");
        }

        @Test
        void coversEverySourceAnOperatorPinned() {
            PermissionFilterBuilder builder = builderWith(
                    resolver("alfresco", () -> List.of("GROUP_DEVELOPERS")),
                    resolver("nuxeo", () -> List.of("GROUP_ENGINEERING")));
            PermissionFilterBuilder.Settings pinned = new PermissionFilterBuilder.Settings(
                    PermissionSourceCatalog.Configured.of("test-repo", "nuxeo-demo", "test-repo,nuxeo-demo"),
                    false);

            String filter = builder.query(CallerIdentities.single("alice"), pinned, null, null);

            assertThat(filter).contains("sys_racl = 'g:GROUP_DEVELOPERS_#_test-repo'");
            assertThat(filter).contains("sys_racl = 'u:alice_#_nuxeo-demo'");
            assertThat(filter).contains("sys_racl = 'g:GROUP_ENGINEERING_#_nuxeo-demo'");
            // A group from one source's directory must not be namespaced into another's.
            assertThat(filter).doesNotContain("g:GROUP_ENGINEERING_#_test-repo");
        }

        @Test
        void narrowsToTheRequestedSourceType() {
            String filter = builderWith().query(
                    CallerIdentities.single("alice"), settings(false), "nuxeo", null);

            assertThat(filter).contains("sys_racl = 'u:alice_#_nuxeo-demo'");
            assertThat(filter).doesNotContain("u:alice_#_test-repo");
        }

        @Test
        void discoversASourceIdFromTheIndexWhenNoneIsConfigured() {
            stubIndexedSources("alfresco:discovered-repo");
            PermissionFilterBuilder.Settings nothingConfigured = new PermissionFilterBuilder.Settings(
                    PermissionSourceCatalog.Configured.of(null, null, null), true);
            PermissionFilterBuilder builder = builderWith(
                    resolver("alfresco", () -> List.of("GROUP_ALFRESCO_ADMINISTRATORS")));

            String filter = builder.query(CallerIdentities.single("admin"), nothingConfigured, "alfresco", null);

            assertThat(filter).contains("cin_sourceId = 'alfresco:discovered-repo'");
        }
    }

    @Nested
    class WhenTheAdminBypassIsOff {

        @Test
        void anAlfrescoAdministratorIsAclFilteredLikeAnyoneElse() {
            PermissionFilterBuilder builder = builderWith(
                    resolver("alfresco", () -> List.of("GROUP_ALFRESCO_ADMINISTRATORS")));

            String filter = builder.query(CallerIdentities.single("admin"), alfrescoOnly(), "alfresco", null);

            // No unconditional source clause: the administrator reads through sys_racl like everyone else.
            assertThat(filter).doesNotContain("cin_sourceId = 'alfresco:test-repo'");
            assertThat(filter).contains("sys_racl = 'u:admin_#_test-repo'");
            // The group is namespaced like any other, so it grants only what documents actually name.
            assertThat(filter).contains("sys_racl = 'g:GROUP_ALFRESCO_ADMINISTRATORS_#_test-repo'");
        }

        @Test
        void andWhenOnItReplacesTheAclClauseRatherThanNarrowingIt() {
            PermissionFilterBuilder builder = builderWith(
                    resolver("alfresco", () -> List.of("GROUP_ALFRESCO_ADMINISTRATORS")));
            PermissionFilterBuilder.Settings bypassOn = new PermissionFilterBuilder.Settings(
                    PermissionSourceCatalog.Configured.of("test-repo", null, null), true);

            String filter = builder.query(CallerIdentities.single("admin"), bypassOn, "alfresco", null);

            assertThat(filter).contains("cin_sourceId = 'alfresco:test-repo'");
            assertThat(filter).doesNotContain("sys_racl = 'u:admin_#_test-repo'");
        }
    }

    @Nested
    class WithoutGroupResolvers {

        @Test
        void yieldsDefaultAuthoritiesForEverySource() {
            stubIndexedSources("alfresco:test-repo");

            String filter = PermissionFilterBuilder.withoutGroupResolvers(hxprService)
                    .query(CallerIdentities.single("alice"), settings(false), null, null);

            assertThat(filter).contains("sys_racl = 'u:alice_#_test-repo'");
            assertThat(filter).contains("sys_racl = '__Everyone__'");
        }
    }
}
