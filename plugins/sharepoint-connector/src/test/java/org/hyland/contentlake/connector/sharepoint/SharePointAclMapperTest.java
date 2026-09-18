package org.hyland.contentlake.connector.sharepoint;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.hyland.contentlake.spi.PermissionRule;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * One test per row of the mapping table, plus the guarantees that hold regardless of configuration.
 *
 * <p>Most cases read the fixture files the mock serves, so the mapper is tested against the same payloads
 * the connector will meet end to end, and replacing a fixture with one recorded from a real tenant
 * re-points these tests at reality without editing them.</p>
 */
class SharePointAclMapperTest {

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final Path FIXTURES = Path.of("src/test/resources/fixtures/permissions");

    private static List<JsonNode> fixture(String name) throws IOException {
        JsonNode root = JSON.readTree(Files.readString(FIXTURES.resolve(name + ".json")));
        List<JsonNode> entries = new ArrayList<>();
        root.get("value").forEach(entries::add);
        return entries;
    }

    private static List<JsonNode> entries(String json) throws IOException {
        JsonNode parsed = JSON.readTree(json);
        List<JsonNode> entries = new ArrayList<>();
        if (parsed.isArray()) {
            parsed.forEach(entries::add);
        } else {
            entries.add(parsed);
        }
        return entries;
    }

    @Test
    void mapsANamedUserToBothTheObjectIdAndThePrincipalName() throws Exception {
        SharePointAclMapper.MappedAcl acl = SharePointAclMapper.withDefaults()
                .map(fixture("i-named"), true);

        // Not a widening: they name one identity. The UPN is what rag-service matches on today, the object
        // id is what an Entra resolver will match.
        assertThat(acl.readPrincipals()).containsExactlyInAnyOrder("user-guid-bob", "bob@contoso.com");
        assertThat(acl.ingestable()).isTrue();
    }

    @Test
    void prefersTheDirectoryIdentityWhenAnEntryAlsoCarriesTheSiteLocalOne() throws Exception {
        SharePointAclMapper mapper = SharePointAclMapper.withDefaults();

        SharePointAclMapper.MappedAcl acl = mapper.map(fixture("i-named"), true);

        // The fixture carries user and siteUser for the same person. Emitting both would add a principal
        // that resolves for nobody and inflate the site-local count.
        assertThat(acl.readPrincipals()).noneMatch(principal -> principal.startsWith("SITEUSER_"));
        assertThat(mapper.counters().get("documentsSiteLocalOnly")).isZero();
    }

    @Test
    void mapsAGroupGrantToTheEntraObjectIdAndNeverToItsDisplayName() throws Exception {
        SharePointAclMapper mapper = SharePointAclMapper.withDefaults();

        SharePointAclMapper.MappedAcl acl = mapper.map(fixture("i-group"), true);

        assertThat(acl.readPrincipals()).containsExactly("GROUP_group-guid-finance");
        // Entra display names are not unique, so GROUP_Finance would be a leak vector.
        assertThat(acl.readPrincipals()).noneMatch(principal -> principal.contains("Finance"));
        // Countable, because until an Entra resolver ships this document is retrievable by nobody.
        assertThat(mapper.counters().get("documentsGroupOnly")).isEqualTo(1);
    }

    @Test
    void mapsAnEveryoneExceptExternalUsersClaimToTheEveryoneAuthority() throws Exception {
        SharePointAclMapper.MappedAcl acl = SharePointAclMapper.withDefaults()
                .map(fixture("i-quarterly"), true);

        assertThat(acl.readPrincipals()).containsExactly(SharePointAclMapper.EVERYONE_AUTHORITY);
    }

    @Test
    void doesNotTreatAGroupMerelyNamedLikeEveryoneAsEveryone() throws Exception {
        SharePointAclMapper.MappedAcl acl = SharePointAclMapper.withDefaults().map(entries("""
                {"id":"1","roles":["read"],"grantedToV2":{"group":
                  {"id":"group-guid-finance","displayName":"Everyone in Finance"}}}"""), true);

        // A substring test on display names would hand the whole tenant a Finance document.
        assertThat(acl.readPrincipals()).containsExactly("GROUP_group-guid-finance");
    }

    @Test
    void recognisesTheEveryoneClaimByItsClaimFragmentWhateverItIsCalled() throws Exception {
        SharePointAclMapper.MappedAcl acl = SharePointAclMapper.withDefaults().map(entries("""
                {"id":"1","roles":["read"],"grantedToV2":{"siteGroup":
                  {"id":"4","displayName":"Alle ausser externen Benutzern",
                   "loginName":"c:0-.f|rolemanager|spo-grid-all-users/tenant-guid"}}}"""), true);

        // The display name is localised and unrecognisable; the claim is structured and is not.
        assertThat(acl.readPrincipals()).containsExactly(SharePointAclMapper.EVERYONE_AUTHORITY);
    }

    @Test
    void mapsAnOrganisationLinkToEveryoneAndAnAnonymousLinkToNothing() throws Exception {
        SharePointAclMapper.MappedAcl acl = SharePointAclMapper.withDefaults()
                .map(fixture("i-orgwide"), true);

        // The fixture holds both. Anonymous grants nothing to any authenticated principal, so the result
        // must be exactly the organisation grant.
        assertThat(acl.readPrincipals()).containsExactly(SharePointAclMapper.EVERYONE_AUTHORITY);
    }

    @Test
    void expandsAUsersScopedLinkThroughGrantedToIdentitiesV2() throws Exception {
        SharePointAclMapper.MappedAcl acl = SharePointAclMapper.withDefaults()
                .map(fixture("i-deep"), true);

        assertThat(acl.readPrincipals())
                .containsExactlyInAnyOrder("user-guid-carol", "carol@contoso.com");
    }

    @Test
    void emitsASiteGroupAsAPrincipalNobodyCanMatchAndCountsIt() throws Exception {
        SharePointAclMapper mapper = SharePointAclMapper.withDefaults();

        SharePointAclMapper.MappedAcl acl = mapper.map(entries("""
                {"id":"1","roles":["read"],"grantedToV2":{"siteGroup":
                  {"id":"14","displayName":"Site Members"}}}"""), true);

        // Kept rather than dropped: a principal nobody matches is fail-closed and visible, where a silently
        // dropped grant is neither.
        assertThat(acl.readPrincipals()).containsExactly("GROUP_SITEGROUP_14");
        assertThat(mapper.counters().get("documentsSiteLocalOnly")).isEqualTo(1);
    }

    @Test
    void treatsReadWriteAndOwnerAsGrantingReadAndAnythingElseAsGrantingNothing() throws Exception {
        SharePointAclMapper mapper = SharePointAclMapper.withDefaults();

        for (String role : List.of("read", "write", "owner")) {
            SharePointAclMapper.MappedAcl acl = mapper.map(entries("""
                    {"id":"1","roles":["%s"],"grantedToV2":{"user":
                      {"id":"user-guid-bob","userPrincipalName":"bob@contoso.com"}}}""".formatted(role)),
                    true);
            assertThat(acl.readPrincipals()).as("role %s", role).isNotEmpty();
        }

        SharePointAclMapper.MappedAcl unrecognised = mapper.map(entries("""
                {"id":"1","roles":["sp.limited edit"],"grantedToV2":{"user":
                  {"id":"user-guid-bob","userPrincipalName":"bob@contoso.com"}}}"""), true);

        assertThat(unrecognised.readPrincipals()).isEmpty();
        // Reported by name, because a role that should have granted read means those items under-share and
        // an operator has no other way to find out.
        assertThat(mapper.unrecognisedRolesByCount()).containsEntry("sp.limited edit", 1L);
    }

    @Test
    void refusesAnItemWhoseAclCouldNotBeReadWhenFailingClosed() {
        SharePointAclMapper mapper = SharePointAclMapper.withDefaults();

        SharePointAclMapper.MappedAcl acl = mapper.map(List.of(), false);

        assertThat(acl.ingestable()).isFalse();
        assertThat(acl.readPrincipals()).isEmpty();
        assertThat(mapper.counters().get("documentsWithNoReadableAcl")).isEqualTo(1);
    }

    @Test
    void neverIngestsAnUnreadableAclAsWorldReadableUnlessThatWasChosenExplicitly() {
        SharePointAclMapper failClosed = SharePointAclMapper.withDefaults();
        SharePointAclMapper explicitlyPublic = new SharePointAclMapper(
                SharePointAclMapper.AclFallback.PUBLIC, SharePointAclMapper.GroupGrants.MAP, null);

        assertThat(failClosed.map(List.of(), false).readPrincipals()).isEmpty();
        // The only route to a world-readable document from an unreadable ACL, and it has to be named in
        // configuration.
        assertThat(explicitlyPublic.map(List.of(), false).readPrincipals())
                .containsExactly(SharePointAclMapper.EVERYONE_AUTHORITY);
    }

    @Test
    void anEmptyPermissionsCollectionGrantsNobodyRatherThanEveryone() throws Exception {
        SharePointAclMapper mapper = SharePointAclMapper.withDefaults();

        SharePointAclMapper.MappedAcl acl = mapper.map(fixture("i-empty"), true);

        // Read successfully and grants nothing: distinct from "could not be read", and still ingestable.
        assertThat(acl.ingestable()).isTrue();
        assertThat(acl.readPrincipals()).isEmpty();
        assertThat(mapper.counters().get("documentsGrantedToNobody")).isEqualTo(1);
    }

    @Test
    void omitsGroupPrincipalsEntirelyWhenAskedTo() throws Exception {
        SharePointAclMapper mapper = new SharePointAclMapper(
                SharePointAclMapper.AclFallback.FAIL_CLOSED, SharePointAclMapper.GroupGrants.SKIP, null);

        SharePointAclMapper.MappedAcl acl = mapper.map(fixture("i-group"), true);

        // Fail-closed per item, and deliberately no option that would widen it to the tenant instead.
        assertThat(acl.readPrincipals()).isEmpty();
    }

    @Test
    void reportsWhetherPermissionsAreInheritedAndWhetherTheItemHasItsOwn() throws Exception {
        SharePointAclMapper mapper = SharePointAclMapper.withDefaults();

        SharePointAclMapper.MappedAcl inherited = mapper.map(fixture("i-quarterly"), true);
        SharePointAclMapper.MappedAcl unique = mapper.map(fixture("i-named"), true);

        assertThat(inherited.securityConfig().inheritanceEnabled()).isTrue();
        assertThat(inherited.hasUniquePermissions()).isFalse();
        // What a permission-hierarchy cache keys on: this item's ACL is its own, so an ancestor's cached
        // set must never be served for it.
        assertThat(unique.hasUniquePermissions()).isTrue();
        assertThat(unique.securityConfig().inheritanceEnabled()).isFalse();
    }

    @Test
    void emitsOneStructuredRulePerPrincipalWithItsIdentityType() throws Exception {
        SharePointAclMapper.MappedAcl acl = SharePointAclMapper.withDefaults()
                .map(fixture("root"), true);

        List<PermissionRule> rules = acl.securityConfig().permissions();
        assertThat(rules).extracting(PermissionRule::identity)
                .containsExactlyInAnyOrder("GROUP_SITEGROUP_3", SharePointAclMapper.EVERYONE_AUTHORITY);
        assertThat(rules).allSatisfy(rule -> {
            assertThat(rule.access()).isEqualTo("READ");
            assertThat(rule.identityType()).isEqualTo("group");
        });
    }

    @Test
    void readsEveryEntryOfAPagedCollectionTheCallerAssembled() throws Exception {
        // The caller follows @odata.nextLink and hands over the whole collection. Proving the mapper does
        // not stop early matters because a truncated collection silently drops grants.
        List<JsonNode> assembled = new ArrayList<>();
        for (int i = 0; i < 250; i++) {
            assembled.addAll(entries("""
                    {"id":"p%d","roles":["read"],"grantedToV2":{"user":
                      {"id":"user-%d","userPrincipalName":"user%d@contoso.com"}}}"""
                    .formatted(i, i, i)));
        }

        SharePointAclMapper.MappedAcl acl = SharePointAclMapper.withDefaults().map(assembled, true);

        assertThat(acl.readPrincipals()).hasSize(500).contains("user-249", "user249@contoso.com");
    }

    @Test
    void ignoresAnEntryShapeItDoesNotUnderstandRatherThanGuessing() throws Exception {
        SharePointAclMapper.MappedAcl acl = SharePointAclMapper.withDefaults().map(entries("""
                [{"id":"1","roles":["read"],"grantedToV2":{"application":
                   {"id":"app-guid","displayName":"Some App"}}},
                 {"id":"2","roles":["read"],"link":{"scope":"newScopeMicrosoftAddsLater","type":"view"}},
                 {"id":"3","roles":["read"],"grantedToV2":{"group":{"displayName":"No Id Here"}}}]"""), true);

        // Three shapes with no safe reading. All three grant nothing, and the item under-shares rather
        // than over-shares.
        assertThat(acl.readPrincipals()).isEmpty();
    }

    @Test
    void handlesTheLegacyGrantedToFieldAsWellAsGrantedToV2() throws Exception {
        SharePointAclMapper.MappedAcl acl = SharePointAclMapper.withDefaults().map(entries("""
                {"id":"1","roles":["read"],"grantedTo":{"user":
                  {"id":"user-guid-bob","userPrincipalName":"bob@contoso.com"}}}"""), true);

        // Graph still returns the older field in places, and a connector that read only the V2 form would
        // silently drop those grants.
        assertThat(acl.readPrincipals()).containsExactlyInAnyOrder("user-guid-bob", "bob@contoso.com");
    }

    @Test
    void countsNothingAsGroupOnlyWhenAUserGrantIsAlsoPresent() throws Exception {
        SharePointAclMapper mapper = SharePointAclMapper.withDefaults();

        mapper.map(entries("""
                [{"id":"1","roles":["read"],"grantedToV2":{"group":{"id":"group-guid-finance"}}},
                 {"id":"2","roles":["read"],"grantedToV2":{"user":
                   {"id":"user-guid-bob","userPrincipalName":"bob@contoso.com"}}}]"""), true);

        // Retrievable by Bob today, so it is not one of the documents waiting on a group resolver.
        assertThat(mapper.counters().get("documentsGroupOnly")).isZero();
    }

    @Test
    void everyoneClaimStringsAreConfigurableBecauseTheirExactWordingIsUnconfirmed() throws Exception {
        SharePointAclMapper mapper = new SharePointAclMapper(
                SharePointAclMapper.AclFallback.FAIL_CLOSED,
                SharePointAclMapper.GroupGrants.MAP,
                Set.of("tous sauf les utilisateurs externes"));

        SharePointAclMapper.MappedAcl acl = mapper.map(entries("""
                {"id":"1","roles":["read"],"grantedToV2":{"group":
                  {"id":"g","displayName":"Tous sauf les utilisateurs externes"}}}"""), true);

        // A tenant that words these differently is a configuration change, not a release.
        assertThat(acl.readPrincipals()).containsExactly(SharePointAclMapper.EVERYONE_AUTHORITY);
    }
}
