# Sync Pipeline

## Overview

Documents flow from a content source (Alfresco or Nuxeo) through `ContentSyncService` in
`content-lake-core`, which calls SPI interfaces to stay source-agnostic. Two ingestion paths exist:
full batch sync and live/event-driven sync.

---

## Full Sync Flow (`ContentSyncService.syncNode`)

```
SourceNode (from ContentSourceClient)
  │
  ├─ findByNodeId(nodeId, sourceId) ──► staleness check (modifiedAt comparison)
  │                                     skip if already current
  │
  ├─ createDocument() or updateDocument()
  │    builds HxprDocument with metadata + ACL + ingestProperties
  │    hxprService.createDocument(parentPath, doc)
  │
  └─ processContent()
       if textExtractor.supports(mimeType): sourceClient.getContent(nodeId)
       │ otherwise: sourceClient.downloadContent() + textExtractor.extractText()
       │
       chunkingService.chunk(text)
       embeddingService.embedChunks(chunks)
       hxprService.updateEmbeddings(hxprDocId, embeddings)
       documentApi.updateById(hxprDocId, fulltext + INDEXED status)
```

---

## Metadata-Only Flow (`ContentSyncService.ingestMetadata`)

Used by the batch ingester's two-phase pipeline:

1. `ingestMetadata()` -- writes hxpr document with metadata, returns a `SyncResult`
2. `TransformationQueue` enqueues the `SyncResult`
3. `TransformationWorker` picks it up and calls `processContent()`

This decouples metadata indexing (fast) from text extraction and embedding (slow), allowing
incremental progress even if the transformation pipeline is slow or interrupted.

---

## Path Structure in hxpr

Documents land at: `/{hxprTargetPath}/{sourceId}/{sourcePath}/{nodeName}`

- `hxprTargetPath` -- Spring config value (e.g. `/alfresco` or `/nuxeo`)
- `sourceId` -- identifies the source system instance
- `HxprService.ensureFolder()` creates the parent path on demand

---

## Idempotency

Every write is guarded by a `modifiedAt` staleness check. If the hxpr version is already at or
newer than the incoming node, the write is skipped. This makes it safe to run batch and live
ingesters concurrently against the same node without producing duplicate writes.

---

## Scope Resolution

Before a node is synced, `ScopeResolver.isInScope(node)` determines whether it belongs in the lake:

- **Alfresco** -- `ContentLakeScopeResolver`: file is in scope when it (or an ancestor folder) has
  the `cl:indexed` aspect AND neither the file nor any ancestor has `cl:excludeFromLake = true`.
  `shouldTraverse(node)` checks for excluded aspects and path patterns.
  Ancestor lookups hit `AlfrescoClient.getNode()` with an in-memory cache (max 2 000 entries).
  Call `invalidateFolderScope(folderId)` after `cl:indexed` changes.

- **Nuxeo** -- `NuxeoScopeResolver`: config-only scope for MVP. Driven by
  `nuxeo.scope.includedRoots` and `nuxeo.scope.includedTypes` in `application.yml`.

---

## `cin_sourceId` Format

`cin_sourceId` stores `"<sourceType>:<sourceId>"` -- e.g. `"alfresco:abc-uuid"`, `"nuxeo:prod"`.
`NodeSyncService.formatSourceId(SourceNode)` produces this value on every write, so all documents
ingested by current code carry the namespaced format.

### Legacy compatibility (built in)

Documents written by older Alfresco-only code stored the raw repository UUID with no
`"<sourceType>:"` prefix. Lookups handle both transparently: `HxprService.findByNodeId(nodeId,
sourceId)` builds its predicate through `buildSourceIdPredicate` / `sourceIdVariants`, which
OR-queries the namespaced form and the legacy raw id. No migration pause or dual-write window is
required -- a rolling deploy finds old and new documents alike.

### `findByNodeId` overloads

`findByNodeId(String nodeId, String sourceId)` is the preferred entry point; all source-aware
callers pass the formatted `sourceId`. The single-arg `findByNodeId(String nodeId)` overload remains
as a convenience for callers without source context and delegates to the two-arg form with a `null`
`sourceId` (matching on `cin_id` alone).

---

## Live Ingestion -- Alfresco

`alfresco-live-ingester` connects to Alfresco ActiveMQ and consumes `alfresco.repo.event2` topics.
Each `RepoEvent` is dispatched to a typed handler:

| Handler | Trigger |
|---|---|
| `NodeCreatedHandler` | new file or folder |
| `NodeUpdatedHandler` | content or metadata change |
| `NodeDeletedHandler` | deletion |
| `FolderMovedHandler` | folder move (triggers subtree reconciliation) |
| `ChildAssociationCreatedHandler` / `DeletedHandler` | child assoc changes |
| `PeerAssociationCreatedHandler` / `DeletedHandler` | peer assoc changes |
| `PermissionUpdatedHandler` | ACL change when emitted by the repository |
| `FolderIndexedScopeChangedHandler` | `cl:indexed` aspect toggled on a folder |

`RecentEventDeduplicator` prevents redundant processing when multiple events arrive for the same
node within a short window.

Alfresco Repository does not reliably emit permission update events for the ACL changes needed by
Content Lake. Because of that, Alfresco ACL reconciliation should not rely on the live ingester as
the primary mechanism. The primary production path is the repository-side `content-lake-repo-model`
addon, which detects ACL changes after commit and publishes a persistent ActiveMQ queue message.
`alfresco-batch-ingester` consumes that queue in a transacted listener and executes ACL
reconciliation, so failed reconciliation attempts are redelivered by the broker.

---

## Live Ingestion -- Nuxeo

`nuxeo-live-ingester` uses audit polling via `NuxeoAuditClient`. It queries the Nuxeo audit log
periodically, tracking the last-seen cursor in `AuditCursorStore` (default implementation:
`FileAuditCursorStore`). `NuxeoAuditMetrics` exposes polling and processing counters via Micrometer.

---

## Batch Ingestion -- Alfresco

`alfresco-batch-ingester` triggers a full sync:

1. `NodeDiscoveryService` walks the Alfresco folder tree, filtered by `ContentLakeScopeResolver`
2. Each in-scope node is passed to `BatchIngestionService`
3. `MetadataIngester` handles two-phase: metadata first, then transformation queue
4. `TransformationWorker` picks up tasks from `TransformationQueue` and calls `processContent()`

`HxprModelBootstrapRunner` runs on startup to ensure the hxpr content model is provisioned.

The same service also exposes `POST /api/sync/permissions` for Alfresco ACL reconciliation:

- File requests update only the stored hxpr ACL for that file.
- Folder requests traverse descendants recursively and update or delete affected file documents
  without re-running text extraction or embeddings.
- In production, the Alfresco repository addon should publish a queue message after ACL changes
  commit, and `alfresco-batch-ingester` should consume that message and run this reconciliation.
- It also remains available as an explicit fallback when you need to reconcile a node manually.

---

## Batch Ingestion -- Nuxeo

`nuxeo-batch-ingester` uses NXQL discovery:

```sql
SELECT * FROM Document
WHERE ecm:path STARTSWITH '/default-domain/workspaces'
  AND ecm:primaryType IN ('File','Note')
  AND ecm:currentLifeCycleState != 'deleted'
  AND ecm:isProxy = 0
  AND ecm:isCheckedInVersion = 0
```

`NuxeoDiscoveryService` pages through results using `currentPageIndex` and `pageSize`.
`NuxeoBatchIngestionService` calls `ContentSyncService` for each discovered document.
