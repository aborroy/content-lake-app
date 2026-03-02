package org.alfresco.contentlake.live.handler;

import lombok.RequiredArgsConstructor;
import org.alfresco.contentlake.live.service.LiveEventProcessor;
import org.alfresco.event.sdk.handling.filter.EventFilter;
import org.alfresco.event.sdk.handling.filter.IsFileFilter;
import org.alfresco.event.sdk.handling.handler.OnNodeCreatedEventHandler;
import org.alfresco.repo.event.v1.model.DataAttributes;
import org.alfresco.repo.event.v1.model.RepoEvent;
import org.alfresco.repo.event.v1.model.Resource;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class NodeCreatedHandler implements OnNodeCreatedEventHandler {

    private final LiveEventProcessor processor;

    @Override
    public void handleEvent(RepoEvent<DataAttributes<Resource>> repoEvent) {
        processor.processNodeStateChange(repoEvent);
    }

    @Override
    public EventFilter getEventFilter() {
        return IsFileFilter.get();
    }
}
