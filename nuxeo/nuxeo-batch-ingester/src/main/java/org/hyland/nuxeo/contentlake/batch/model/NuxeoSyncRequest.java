package org.hyland.nuxeo.contentlake.batch.model;

import lombok.Data;
import org.hyland.nuxeo.contentlake.config.NuxeoProperties;

import java.util.List;

@Data
public class NuxeoSyncRequest {

    private List<String> includedRoots;
    private List<String> includedDocumentTypes;
    private List<String> excludedLifecycleStates;
    private Integer pageSize;
    private NuxeoProperties.Mode discoveryMode;
}
