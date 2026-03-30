package org.hyland.alfresco.contentlake.client;

import org.alfresco.core.model.Node;
import org.alfresco.core.model.PermissionElement;
import org.alfresco.core.model.PermissionsInfo;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class AlfrescoClientAclExtractionTest {

    private AlfrescoClient client;

    @BeforeEach
    void setUp() {
        client = new AlfrescoClient(null, null);
    }

    @Test
    void extractReadAuthorities_withExplicitUserGrantAndInheritanceDisabled_returnsGrantedUser() {
        Node node = new Node().permissions(new PermissionsInfo()
                .isInheritanceEnabled(false)
                .addLocallySetItem(allowed("user-a", "Consumer")));

        assertThat(client.extractReadAuthorities(node)).containsExactly("user-a");
    }

    @Test
    void extractReadAuthorities_withExplicitGroupGrantAndInheritanceDisabled_returnsGrantedGroup() {
        Node node = new Node().permissions(new PermissionsInfo()
                .isInheritanceEnabled(false)
                .addLocallySetItem(allowed("GROUP_engineering", "SiteConsumer")));

        assertThat(client.extractReadAuthorities(node)).containsExactly("GROUP_engineering");
    }

    @Test
    void extractReadAuthorities_withEmptyLocalPermissionsAndInheritanceDisabled_returnsEmptySet() {
        Node node = new Node().permissions(new PermissionsInfo()
                .isInheritanceEnabled(false)
                .addInheritedItem(allowed("GROUP_parent", "Consumer")));

        assertThat(client.extractReadAuthorities(node)).isEmpty();
    }

    @Test
    void extractReadAuthorities_withDisabledInheritance_ignoresInheritedPermissions() {
        Node node = new Node().permissions(new PermissionsInfo()
                .isInheritanceEnabled(false)
                .addInheritedItem(allowed("GROUP_parent", "SiteConsumer"))
                .addLocallySetItem(allowed("user-a", "Consumer")));

        assertThat(client.extractReadAuthorities(node)).containsExactly("user-a");
    }

    private static PermissionElement allowed(String authorityId, String role) {
        return new PermissionElement()
                .authorityId(authorityId)
                .name(role)
                .accessStatus(PermissionElement.AccessStatusEnum.ALLOWED);
    }
}
