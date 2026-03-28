package org.hyland.alfresco.contentlake.batch.model;

import lombok.Data;

import java.util.List;

@Data
public class BatchSyncRequest {

    private List<String> folders;
    private boolean recursive = true;
    private List<String> types;
}
