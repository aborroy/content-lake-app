package org.hyland.alfresco.contentlake.live.handler;

import lombok.RequiredArgsConstructor;
import org.hyland.alfresco.contentlake.live.service.LiveEventProcessor;
import org.alfresco.event.sdk.handling.handler.OnNodeDeletedEventHandler;
import org.alfresco.repo.event.v1.model.DataAttributes;
import org.alfresco.repo.event.v1.model.RepoEvent;
import org.alfresco.repo.event.v1.model.Resource;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class NodeDeletedHandler implements OnNodeDeletedEventHandler {

    private final LiveEventProcessor processor;

    @Override
    public void handleEvent(RepoEvent<DataAttributes<Resource>> repoEvent) {
        processor.processDeletion(repoEvent);
    }
}
