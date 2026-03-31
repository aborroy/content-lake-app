package org.hyland.alfresco.contentlake.repo.acl;

import org.alfresco.model.ContentModel;
import org.alfresco.service.cmr.dictionary.DictionaryService;
import org.alfresco.service.cmr.repository.NodeRef;
import org.alfresco.service.cmr.repository.NodeService;
import org.alfresco.service.namespace.QName;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.lang.reflect.Modifier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class AclChangeCollectorTest {

    @Mock
    private NodeService nodeService;

    @Mock
    private DictionaryService dictionaryService;

    @Mock
    private AclChangeTxnBuffer txnBuffer;

    private AclChangeCollector collector;

    @BeforeEach
    void setUp() {
        collector = new AclChangeCollector(true, null, nodeService, dictionaryService, txnBuffer);
    }

    @Test
    void collectorClass_isPublicForAlfrescoPolicyReflection() {
        assertThat(Modifier.isPublic(AclChangeCollector.class.getModifiers())).isTrue();
    }

    @Test
    void filePermissionChange_enqueuesNonRecursiveRequest() {
        NodeRef nodeRef = new NodeRef("workspace://SpacesStore/file-1");
        when(nodeService.exists(nodeRef)).thenReturn(true);
        when(nodeService.getType(nodeRef)).thenReturn(ContentModel.TYPE_CONTENT);
        when(dictionaryService.isSubClass(ContentModel.TYPE_CONTENT, ContentModel.TYPE_FOLDER)).thenReturn(false);

        collector.onGrantLocalPermission(nodeRef, "user-a", "Consumer");

        ArgumentCaptor<AclChangeRequest> request = ArgumentCaptor.forClass(AclChangeRequest.class);
        verify(txnBuffer).enqueueAfterCommit(request.capture());
        assertThat(request.getValue()).isEqualTo(new AclChangeRequest("file-1", false, "grant-local-permission"));
    }

    @Test
    void folderInheritanceChange_enqueuesRecursiveRequest() {
        NodeRef nodeRef = new NodeRef("workspace://SpacesStore/folder-1");
        QName folderType = QName.createQName("{custom.model}folderType");
        when(nodeService.exists(nodeRef)).thenReturn(true);
        when(nodeService.getType(nodeRef)).thenReturn(folderType);
        when(dictionaryService.isSubClass(folderType, ContentModel.TYPE_FOLDER)).thenReturn(true);

        collector.onInheritPermissionsDisabled(nodeRef, true);

        ArgumentCaptor<AclChangeRequest> request = ArgumentCaptor.forClass(AclChangeRequest.class);
        verify(txnBuffer).enqueueAfterCommit(request.capture());
        assertThat(request.getValue()).isEqualTo(new AclChangeRequest("folder-1", true, "inherit-permissions-disabled"));
    }
}
