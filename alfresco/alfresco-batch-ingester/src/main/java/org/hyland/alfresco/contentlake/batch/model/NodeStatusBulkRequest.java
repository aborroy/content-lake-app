package org.hyland.alfresco.contentlake.batch.model;

import java.util.List;

public record NodeStatusBulkRequest(List<String> nodeIds, Boolean includeFolderAggregate) {}
