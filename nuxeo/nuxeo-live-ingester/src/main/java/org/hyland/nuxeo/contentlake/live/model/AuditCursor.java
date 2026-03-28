package org.hyland.nuxeo.contentlake.live.model;

import java.time.OffsetDateTime;
import java.util.Objects;

public record AuditCursor(OffsetDateTime lastLogDate, long lastEntryId) {

    public AuditCursor {
        Objects.requireNonNull(lastLogDate, "lastLogDate must not be null");
    }

    public static AuditCursor initial(OffsetDateTime lastLogDate) {
        return new AuditCursor(lastLogDate, 0L);
    }

    public static AuditCursor from(NuxeoAuditEntry entry) {
        return new AuditCursor(entry.logDate(), entry.id());
    }
}
