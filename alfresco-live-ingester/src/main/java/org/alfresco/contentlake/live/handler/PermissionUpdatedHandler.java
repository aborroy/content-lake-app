package org.alfresco.contentlake.live.handler;

import lombok.RequiredArgsConstructor;
import org.alfresco.contentlake.live.service.LiveEventProcessor;
import org.alfresco.event.sdk.handling.handler.OnPermissionUpdatedEventHandler;
import org.alfresco.repo.event.v1.model.DataAttributes;
import org.alfresco.repo.event.v1.model.RepoEvent;
import org.alfresco.repo.event.v1.model.Resource;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class PermissionUpdatedHandler implements OnPermissionUpdatedEventHandler {

    private final LiveEventProcessor processor;

    @Override
    public void handleEvent(RepoEvent<DataAttributes<Resource>> repoEvent) {
        processor.processPermissionUpdate(repoEvent);
    }
}
