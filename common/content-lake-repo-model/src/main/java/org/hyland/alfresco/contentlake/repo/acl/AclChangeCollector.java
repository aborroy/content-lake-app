package org.hyland.alfresco.contentlake.repo.acl;

import lombok.extern.slf4j.Slf4j;
import org.alfresco.model.ContentModel;
import org.alfresco.repo.policy.JavaBehaviour;
import org.alfresco.repo.policy.PolicyComponent;
import org.alfresco.repo.security.permissions.PermissionServicePolicies;
import org.alfresco.service.cmr.dictionary.DictionaryService;
import org.alfresco.service.cmr.repository.NodeRef;
import org.alfresco.service.cmr.repository.NodeService;
import org.alfresco.service.namespace.QName;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class AclChangeCollector implements InitializingBean,
        PermissionServicePolicies.OnGrantLocalPermission,
        PermissionServicePolicies.OnRevokeLocalPermission,
        PermissionServicePolicies.OnInheritPermissionsEnabled,
        PermissionServicePolicies.OnInheritPermissionsDisabled {

    private final boolean enabled;
    private final PolicyComponent policyComponent;
    private final NodeService nodeService;
    private final DictionaryService dictionaryService;
    private final AclChangeTxnBuffer txnBuffer;

    AclChangeCollector(
            @Value("${content.lake.permission.sync.enabled:false}") boolean enabled,
            PolicyComponent policyComponent,
            NodeService nodeService,
            DictionaryService dictionaryService,
            AclChangeTxnBuffer txnBuffer
    ) {
        this.enabled = enabled;
        this.policyComponent = policyComponent;
        this.nodeService = nodeService;
        this.dictionaryService = dictionaryService;
        this.txnBuffer = txnBuffer;
    }

    @Override
    public void afterPropertiesSet() {
        if (!enabled) {
            log.info("Content Lake ACL change collector is disabled");
            return;
        }

        bind(PermissionServicePolicies.OnGrantLocalPermission.QNAME, "onGrantLocalPermission");
        bind(PermissionServicePolicies.OnRevokeLocalPermission.QNAME, "onRevokeLocalPermission");
        bind(PermissionServicePolicies.OnInheritPermissionsEnabled.QNAME, "onInheritPermissionsEnabled");
        bind(PermissionServicePolicies.OnInheritPermissionsDisabled.QNAME, "onInheritPermissionsDisabled");
        log.info("Content Lake ACL change collector bound to Alfresco permission policies");
    }

    @Override
    public void onGrantLocalPermission(NodeRef nodeRef, String authority, String permission) {
        collect(nodeRef, "grant-local-permission");
    }

    @Override
    public void onRevokeLocalPermission(NodeRef nodeRef, String authority, String permission) {
        collect(nodeRef, "revoke-local-permission");
    }

    @Override
    public void onInheritPermissionsEnabled(NodeRef nodeRef) {
        collect(nodeRef, "inherit-permissions-enabled");
    }

    @Override
    public void onInheritPermissionsDisabled(NodeRef nodeRef, boolean copyToInherit) {
        collect(nodeRef, "inherit-permissions-disabled");
    }

    private void collect(NodeRef nodeRef, String reason) {
        if (!enabled || nodeRef == null || !nodeService.exists(nodeRef)) {
            return;
        }
        boolean recursive = isFolder(nodeRef);
        txnBuffer.enqueueAfterCommit(AclChangeRequest.of(nodeRef, recursive, reason));
        log.debug("Queued ACL reconciliation for node {} recursive={} reason={}", nodeRef, recursive, reason);
    }

    private boolean isFolder(NodeRef nodeRef) {
        QName type = nodeService.getType(nodeRef);
        return type != null && dictionaryService.isSubClass(type, ContentModel.TYPE_FOLDER);
    }

    private void bind(QName policyQName, String methodName) {
        policyComponent.bindClassBehaviour(
                policyQName,
                ContentModel.TYPE_BASE,
                new JavaBehaviour(this, methodName)
        );
    }
}
