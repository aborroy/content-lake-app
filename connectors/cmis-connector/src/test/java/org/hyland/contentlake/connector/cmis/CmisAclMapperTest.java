package org.hyland.contentlake.connector.cmis;

import org.apache.chemistry.opencmis.commons.data.Ace;
import org.apache.chemistry.opencmis.commons.data.Acl;
import org.apache.chemistry.opencmis.commons.data.AclCapabilities;
import org.apache.chemistry.opencmis.commons.data.CmisExtensionElement;
import org.apache.chemistry.opencmis.commons.data.PermissionMapping;
import org.apache.chemistry.opencmis.commons.definitions.PermissionDefinition;
import org.apache.chemistry.opencmis.commons.enums.AclPropagation;
import org.apache.chemistry.opencmis.commons.enums.CapabilityAcl;
import org.apache.chemistry.opencmis.commons.enums.SupportedPermissions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Mapping a CMIS ACL onto read principals, and what happens when there is no ACL to map.
 *
 * <p>The bias throughout is that an unrecognised permission grants nothing. A restricted document that
 * becomes searchable by someone who cannot open it in the source is the failure worth designing against;
 * a document nobody retrieves is visible and fixable.</p>
 */
class CmisAclMapperTest {

    private static Ace ace(String principalId, String... permissions) {
        Ace ace = mock(Ace.class);
        when(ace.getPrincipalId()).thenReturn(principalId);
        when(ace.getPermissions()).thenReturn(List.of(permissions));
        return ace;
    }

    private static Acl acl(Ace... aces) {
        Acl acl = mock(Acl.class);
        when(acl.getAces()).thenReturn(List.of(aces));
        return acl;
    }

    /**
     * A repository whose permission mapping names its own permissions for reading.
     *
     * <p>Hand-written stubs rather than mocks: Mockito's inline mock maker cannot instrument
     * {@code PermissionMapping} or {@code AclCapabilities} on JDK 25, both being {@code Serializable}
     * interfaces, and it fails with "Could not modify all classes". Two methods each is cheaper than
     * fighting that.</p>
     */
    private static AclCapabilities capabilitiesWithVendorPermissions(String... readPermissions) {
        PermissionMapping mapping = new PermissionMapping() {
            @Override
            public String getKey() {
                return PermissionMapping.CAN_GET_PROPERTIES_OBJECT;
            }

            @Override
            public List<String> getPermissions() {
                return List.of(readPermissions);
            }

            @Override
            public List<CmisExtensionElement> getExtensions() {
                return List.of();
            }

            @Override
            public void setExtensions(List<CmisExtensionElement> extensions) {
            }
        };

        return new AclCapabilities() {
            @Override
            public SupportedPermissions getSupportedPermissions() {
                return SupportedPermissions.BOTH;
            }

            @Override
            public AclPropagation getAclPropagation() {
                return AclPropagation.PROPAGATE;
            }

            @Override
            public List<PermissionDefinition> getPermissions() {
                return List.of();
            }

            @Override
            public Map<String, PermissionMapping> getPermissionMapping() {
                return Map.of(PermissionMapping.CAN_GET_PROPERTIES_OBJECT, mapping);
            }

            @Override
            public List<CmisExtensionElement> getExtensions() {
                return List.of();
            }

            @Override
            public void setExtensions(List<CmisExtensionElement> extensions) {
            }
        };
    }

    private static CmisAclMapper mapper(CapabilityAcl capability, CmisAclMapper.AclFallback fallback) {
        return new CmisAclMapper(capability, null, fallback, "cmis-sync", "cmis:anyone");
    }

    @Test
    void mapsTheCmisBasicPermissions() {
        CmisAclMapper mapper = mapper(CapabilityAcl.DISCOVER, CmisAclMapper.AclFallback.FAIL_CLOSED);

        assertThat(mapper.readPrincipals(acl(
                ace("alice", "cmis:read"),
                ace("GROUP_SALES", "cmis:write"),
                ace("bob", "cmis:all"))))
                .containsExactly("alice", "GROUP_SALES", "bob");
    }

    @Test
    void aPermissionThatGrantsNoReadIsIgnored() {
        CmisAclMapper mapper = mapper(CapabilityAcl.MANAGE, CmisAclMapper.AclFallback.FAIL_CLOSED);

        assertThat(mapper.readPrincipals(acl(ace("carol", "cmis:unknown-write-only")))).isEmpty();
    }

    @Test
    void resolvesAVendorPermissionThroughTheRepositoryOwnPermissionMapping() {
        // Alfresco says 'Consumer', another repository says something else. Neither is hardcoded: the
        // repository's own mapping is asked which permissions allow reading an object's properties.
        CmisAclMapper mapper = new CmisAclMapper(CapabilityAcl.DISCOVER,
                capabilitiesWithVendorPermissions("Consumer", "Collaborator"),
                CmisAclMapper.AclFallback.FAIL_CLOSED, "cmis-sync", "cmis:anyone");

        assertThat(mapper.readPrincipals(acl(ace("dave", "Consumer")))).containsExactly("dave");
        assertThat(mapper.readPrincipals(acl(ace("erin", "Coordinator")))).isEmpty();
    }

    @Test
    void theRepositoryAnyonePrincipalBecomesTheEveryoneAuthority() {
        // The pipeline turns GROUP_EVERYONE into the un-namespaced __Everyone__ principal. Passing
        // 'cmis:anyone' through unchanged would namespace it per source and match nothing.
        CmisAclMapper mapper = mapper(CapabilityAcl.DISCOVER, CmisAclMapper.AclFallback.FAIL_CLOSED);

        assertThat(mapper.readPrincipals(acl(ace("cmis:anyone", "cmis:read"))))
                .containsExactly("GROUP_EVERYONE");
    }

    @Test
    void theAnonymousPrincipalIsNotTreatedAsEveryone() {
        CmisAclMapper mapper = new CmisAclMapper(CapabilityAcl.DISCOVER, null,
                CmisAclMapper.AclFallback.FAIL_CLOSED, "cmis-sync", "cmis:anyone");

        assertThat(mapper.readPrincipals(acl(ace("cmis:anonymous", "cmis:read"))))
                .containsExactly("cmis:anonymous");
    }

    @Test
    void aRepositoryWithNoAclCapabilityCannotReadAcls() {
        assertThat(mapper(CapabilityAcl.NONE, CmisAclMapper.AclFallback.FAIL_CLOSED).canReadAcls()).isFalse();
        assertThat(mapper(null, CmisAclMapper.AclFallback.FAIL_CLOSED).canReadAcls()).isFalse();
        assertThat(mapper(CapabilityAcl.DISCOVER, CmisAclMapper.AclFallback.FAIL_CLOSED).canReadAcls()).isTrue();
        assertThat(mapper(CapabilityAcl.MANAGE, CmisAclMapper.AclFallback.FAIL_CLOSED).canReadAcls()).isTrue();
    }

    @Test
    void failClosedIsTheDefaultAndYieldsNoPrincipalsAtAll() {
        // Null is what the client turns into a refusal to ingest. It is not an empty set: an empty set
        // would be "nobody may read this document", which is a decision, and this is the absence of one.
        assertThat(mapper(CapabilityAcl.NONE, CmisAclMapper.AclFallback.FAIL_CLOSED).fallbackPrincipals())
                .isNull();
        assertThat(CmisAclMapper.AclFallback.of(null)).isEqualTo(CmisAclMapper.AclFallback.FAIL_CLOSED);
        assertThat(CmisAclMapper.AclFallback.of("nonsense")).isEqualTo(CmisAclMapper.AclFallback.FAIL_CLOSED);
    }

    @Test
    void theSyncAccountFallbackRestrictsToTheConfiguredAccount() {
        assertThat(mapper(CapabilityAcl.NONE, CmisAclMapper.AclFallback.SYNC_ACCOUNT).readPrincipals(null))
                .containsExactly("cmis-sync");
        assertThat(CmisAclMapper.AclFallback.of("sync-account"))
                .isEqualTo(CmisAclMapper.AclFallback.SYNC_ACCOUNT);
    }

    @Test
    void thePublicFallbackHasToBeAskedForExplicitly() {
        assertThat(mapper(CapabilityAcl.NONE, CmisAclMapper.AclFallback.PUBLIC).readPrincipals(null))
                .containsExactly(CmisAclMapper.EVERYONE_AUTHORITY);
        assertThat(CmisAclMapper.AclFallback.of("public")).isEqualTo(CmisAclMapper.AclFallback.PUBLIC);
    }

    @Test
    void anAbsentAclOnAnAclCapableRepositoryStillFallsBackRatherThanGrantingEveryone() {
        // A repository that reports the capability and then returns no ACL for a document: the answer is
        // unknown, so it takes the fallback path rather than being read as "no restrictions".
        CmisAclMapper mapper = mapper(CapabilityAcl.DISCOVER, CmisAclMapper.AclFallback.FAIL_CLOSED);

        assertThat(mapper.readPrincipals(null)).isNull();
    }

    @Test
    void anAclThatGrantsReadToNobodyIsEmptyRatherThanFallenBackOn() {
        // Distinguished from the case above on purpose: here the repository did answer, and the answer is
        // that nobody may read it. Applying a fallback would widen a document the source restricts.
        CmisAclMapper mapper = mapper(CapabilityAcl.DISCOVER, CmisAclMapper.AclFallback.PUBLIC);

        assertThat(mapper.readPrincipals(acl(ace("frank", "cmis:write-only-vendor-permission"))))
                .isEmpty();
    }
}
