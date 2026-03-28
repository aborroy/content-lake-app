package org.hyland.alfresco.contentlake.live.handler;

import lombok.RequiredArgsConstructor;
import org.hyland.alfresco.contentlake.live.service.LiveEventProcessor;
import org.hyland.alfresco.contentlake.service.ContentLakeScopeResolver;
import org.alfresco.event.sdk.handling.filter.AspectAddedFilter;
import org.alfresco.event.sdk.handling.filter.AspectRemovedFilter;
import org.alfresco.event.sdk.handling.filter.EventFilter;
import org.alfresco.event.sdk.handling.filter.IsFolderFilter;
import org.alfresco.event.sdk.handling.filter.PropertyAddedFilter;
import org.alfresco.event.sdk.handling.filter.PropertyChangedFilter;
import org.alfresco.event.sdk.handling.filter.PropertyRemovedFilter;
import org.alfresco.event.sdk.handling.handler.OnNodeUpdatedEventHandler;
import org.alfresco.repo.event.v1.model.DataAttributes;
import org.alfresco.repo.event.v1.model.RepoEvent;
import org.alfresco.repo.event.v1.model.Resource;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class FolderIndexedScopeChangedHandler implements OnNodeUpdatedEventHandler {

    private final LiveEventProcessor processor;

    @Override
    public void handleEvent(RepoEvent<DataAttributes<Resource>> repoEvent) {
        processor.processFolderScopeChange(repoEvent);
    }

    @Override
    public EventFilter getEventFilter() {
        return IsFolderFilter.get().and(
                AspectAddedFilter.of(ContentLakeScopeResolver.INDEXED_ASPECT)
                        .or(AspectRemovedFilter.of(ContentLakeScopeResolver.INDEXED_ASPECT))
                        .or(PropertyAddedFilter.of(ContentLakeScopeResolver.EXCLUDE_FROM_LAKE_PROPERTY))
                        .or(PropertyChangedFilter.of(ContentLakeScopeResolver.EXCLUDE_FROM_LAKE_PROPERTY))
                        .or(PropertyRemovedFilter.of(ContentLakeScopeResolver.EXCLUDE_FROM_LAKE_PROPERTY))
        );
    }
}
