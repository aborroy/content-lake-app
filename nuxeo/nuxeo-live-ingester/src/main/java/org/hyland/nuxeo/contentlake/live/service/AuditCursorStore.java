package org.hyland.nuxeo.contentlake.live.service;

import org.hyland.nuxeo.contentlake.live.model.AuditCursor;

import java.util.Optional;

public interface AuditCursorStore {

    Optional<AuditCursor> load(String repositoryKey);

    void save(String repositoryKey, AuditCursor cursor);
}
