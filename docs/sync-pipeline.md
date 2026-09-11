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
       if isTextMimeType(mimeType): sourceClient.getContent(nodeId)   # no extractor involved
       │ else if textExtractor.supportsSourceReference(): textExtractor.extract(nodeId, mimeType)
       │ else: sourceClient.downloadContent() + textExtractor.extract(resource, mimeType)
       │       ──► ExtractedText(text, PLAIN | MARKDOWN)
       │
       ContentFingerprint.of(...) ──► matches the stored one? write INDEXED and stop
       │
       chunkingService.chunk(extracted.text())          # markdown kept as-is
       embeddingService.embedChunks(chunks)
       hxprService.updateEmbeddings(hxprDocId, embeddings)
       documentApi.updateById(hxprDocId, plainText(extracted) + INDEXED status)
```

Chunking is structure-aware: detected tables are kept as atomic `ChunkType.TABLE` chunks (never
hard-split mid-row or dropped as noise), and every chunk records a `sectionIndex`. That section id is
what lets retrieval later expand a matched chunk back to its parent section (small-to-big retrieval,
`rag.retrieval.small-to-big.enabled`). When keyword-context enrichment is enabled
(`content-lake.ingest.keyword-context-enrichment-enabled`), document-level context is prepended to
each chunk's keyword-search text so short chunks stay findable by the keyword leg of hybrid search.


## Extraction Representation, and What Each Field Holds

`TextExtractor` returns an `ExtractedText(text, format)` where format is `PLAIN` or `MARKDOWN`. The
distinction exists because table detection needs structure: a table is recognised by a markdown
separator row or by rows carrying at least two `|` characters, and flattening a document to plaintext
removes exactly that signal. A PDF table extracted as plaintext arrives as an undelimited run of
words and is classified `PROSE`.

`extraction.format` (`plaintext` by default, or `auto` / `markdown`) decides whether a transform
engine is asked for markdown, and markdown is requested only where the engine advertises that
transform. Every source can use it: `TransformEngineTextExtractor` speaks the
`alfresco-transform-core` `/transform` protocol directly rather than going through a repository, so
the same engine serves the Alfresco, Nuxeo and filesystem connectors. Extractors are composed by
`ChainingTextExtractor`, which falls through on a `null` return or an exception, so extraction
degrades to in-process Tika and never fails an ingest.

`text/markdown` is in `TEXT_MIME_TYPES`, so a markdown source document is read straight through with
no extractor involved and reaches chunking with its structure intact. No transform engine is needed
for markdown content.

Chunks and the keyword index hold deliberately different representations:

| Destination | Holds | Why |
|---|---|---|
| Chunks and their embeddings | the extracted representation as-is, markdown included | chunking segments on heading and table boundaries, and classifies tables from their pipe rows |
| `cin_ingestProperties.contentLake_extractedText` | markup-stripped plain text | hxpr folds this into the analysed `sys_fulltext` index, which the lexical leg of hybrid search queries and re-scores by term frequency |
| `sys_fulltextBinary` | markup-stripped plain text | the same content, for consistency |

The reason both mirrors get stripped text rather than markdown: markdown punctuation is not a term
anyone searches for, and leaving pipes, dash rules, backticks and link syntax in an analysed index
dilutes the term frequencies of the words that are. `MarkdownToPlainText` renders a table row as its
cells separated by two spaces, which is what the flattened extraction paths already produce for a
table, so keyword behaviour is the same whichever path fed the document. Underscore emphasis is only
stripped at word boundaries, so a `snake_case` identifier survives as one token, which the lexical leg
depends on to retrieve rare identifiers at all.

Note that `sys_fulltextBinary` is **not** exposed to HXQL, so it cannot be queried however it is
spelled; `contentLake_extractedText` is the field that actually feeds keyword matching. Both are
written from the same stripped text so the two can never disagree.

When `extraction.format` is `plaintext`, the strip step is a no-op and every value written is
byte-identical to a deployment without any of this.

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

## Content Reuse

The staleness check stops a stale write from overwriting a newer one, but it cannot tell that content
is byte-identical. A permission change, a property edit or a folder move all leave a newer
`source_modifiedAt`, so all of them used to re-run extraction, chunking and embedding over content
that had not changed.

`processContent` therefore recomputes a content fingerprint after extraction and compares it against
`cin_ingestProperties.contentLake_contentFingerprint`. On a match it writes the sync state and stops:
no chunking, no embedding calls, and the existing embedding child stays as the current one.
Extraction is still paid for, because the fingerprint is taken over the extracted text rather than the
binary.

The fingerprint covers the extracted text, the embedding type and the chunking parameters, plus the
keyword-enrichment flag. Anything that would change the stored chunks or vectors therefore forces a
reprocess: switching `EMBEDDING_MODEL` or `EMBEDDING_CHUNK_SIZE` re-embeds the corpus rather than
short-circuiting it.

Three conditions must all hold before the short circuit is taken:

- `content-lake.ingest.content-reuse-enabled` is true, the default. The switch exists because the
  failure mode is silent in the dangerous direction, so an operator can rule the fingerprint out
  without a rebuild.
- the stored fingerprint equals the recomputed one;
- the extracted-text mirror is present.

The last condition matters more than it looks. `processContent` writes the mirror and the fingerprint
together in its final step, after the embeddings are stored, so the pair is evidence that a content
pass ran to completion. A document that is `INDEXED` with no embeddings at all is invisible to search
while looking finished to monitoring, and trusting a bare fingerprint would make that state permanent.

Permissions are never skipped alongside content: `cin_read`, `cin_deny` and `sys_acl` are written on
every metadata write, whatever the fingerprint says.

`NodeSyncService.getContentReuseStats()` reports short circuits against full reprocesses, and every
short circuit logs the running totals, so the saving is measurable rather than assumed.

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

The `CinRemote`/`CinContext` content model is defined natively by the ai-ready-index engine, so no
client-side model provisioning runs at startup.

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

---

## Deletion

Deletion is one operation, `NodeSyncService.delete(SourceTombstone)`, reached from every source. A
`SourceTombstone` carries a node id, an optional event timestamp, and a reason (`DELETED`,
`OUT_OF_SCOPE`, `MISSING_AT_RECONCILE`). It is deliberately not a `SourceNode`: by the time most
tombstones are raised the node is gone from its source, so there is no name, mime type or ACL to carry.

The order matters. Embedding children are cleared before the document itself, because hxpr's cascade on
document delete is not contractual and an orphaned `_e_*` child remains searchable: the read path
substitutes the `*` embedding-type wildcard, so a child with no parent still answers queries. Clearing
the children is best effort, since a surviving parent is the worse of the two outcomes.

An event timestamp, when present, is compared against the stored `source_modifiedAt` and a delete older
than the indexed version is skipped. That guard orders events; it does not veto a reconciliation sweep,
which observed the source directly and therefore carries no event time.

## Reconciliation Sweep

Deletion otherwise depends entirely on a delete event arriving. With the live ingester down, a dropped
broker message, or a node removed while only the batch ingester runs, the document outlives its source
and search returns it as a phantom result, with nothing to notice.

After a successful batch sync, each batch ingester can compare the index against what its discovery pass
actually saw and delete the difference. It is **off by default** (`*.reconcile.enabled`), because it
deletes documents.

Every precondition fails towards doing nothing:

| Guard | Why |
|---|---|
| Discovery must report itself complete | Several discovery paths return a partial result and log a warning rather than failing, so completeness is asserted by discovery, never inferred by the caller |
| No node-level failures | A node that failed may or may not have been enumerated, so the discovered set is not trustworthy. There is no tolerance knob: a partial-failure threshold is not a number an operator can calibrate |
| An empty discovery deletes nothing | Zero nodes is exactly what a broken discovery returns |
| The discovered-id set must not have overflowed `max-seen-ids` | An incomplete record of what discovery saw would make present documents look missing |
| `max-delete-ratio` and `max-deletes` | The backstop against a partial pass that was never reported as one |
| Scope | Only documents under the roots discovery actually covered are candidates, so a folder-scoped sync cannot delete documents another sync owns |

The scope predicate matches on `cin_paths`, which holds the **hxpr** path
(`<hxprTargetPath>/<repositoryId><sourcePath>`) rather than the raw source path; convert with
`NodeSyncService.contentLakePathPrefix`. A document with no paths never matches and so is never deleted.

The sweep's outcome is attached to the job, so `GET /api/sync/status/{jobId}` reports it:

```json
{
  "reconciliation": {
    "status": "COMPLETED",
    "indexed": 42, "inScope": 12, "candidates": 1,
    "deleted": 1, "failed": 0, "ratio": 0.083,
    "detail": "1 document(s) deleted"
  }
}
```

Any `status` other than `COMPLETED` means nothing was deleted, and says why:
`DISABLED`, `SKIPPED_INCOMPLETE_DISCOVERY`, `SKIPPED_NODE_FAILURES`, `SKIPPED_EMPTY_DISCOVERY`,
`ABORTED_SEEN_SET_OVERFLOW`, `ABORTED_SCAN_FAILED`, `ABORTED_RATIO`, `ABORTED_ABSOLUTE_CAP`.

**Expect the first sweep on an existing corpus to abort on the ratio guard.** A corpus that has
accumulated drift genuinely has many documents to remove, and the guard cannot tell that from a broken
discovery pass. That is correct behaviour: read the logged figures, satisfy yourself the deletions are
genuine, raise the ratio for one run, then lower it again.

The filesystem source has no live ingester, so a batch sync is the only path that writes and this sweep
is the only path that ever deletes: without it, a file removed from the mounted directory stays
searchable indefinitely.

`connector-batch-ingester` has no live path either, but its sweep is off by default and warrants more
caution than the filesystem one. The sweep's scope comes from the source paths discovery resolved, and a
plugin connector's `SourceNode.path()` is whatever the connector decided: one reporting `/` would hand the
sweep everything under that source's target path. Read `resolvedRootPaths` from a completed run before
enabling it. Its walk also reports an incomplete pass whenever a container could not be listed, which is
routine rather than exceptional there, and an incomplete pass never deletes.

## Embedding Storage and the Embedding Type

Embeddings are stored as a Parquet file in a `SysEmbeddings` child document named
`_e_{embeddingType}`, and hxpr indexes them by that child name. The type is **derived from the
configured embedding model**, sanitized for use inside a `sys_name`: `ai/mxbai-embed-large` becomes
`ai-mxbai-embed-large`. `EmbeddingTypeResolver` is the single derivation, called by both the write and
the clear path so the two cannot drift.

Two consequences worth knowing:

- `embedding.model-name` must resolve to the same value in every module. Two writers deriving different
  types would leave one document carrying children under both, and the read path's `*` wildcard would
  then merge vectors from two models in one result set.
- Clearing a document's embeddings is type-agnostic: it removes every `_e_*` child, not only the one
  matching the current configuration. That is what stops a child written under a previously configured
  model from surviving a re-sync and continuing to answer queries.

The derived type is written in two places that must agree: the child's `sys_name` and every Parquet
row's `type` column, which is the `sysembed_type` an embeddings query matches on. Both come from the
same argument in `ParquetEmbeddingWriter`, so they cannot diverge. They previously did, with the row
carrying the raw configured model instead, which is invisible while the read path substitutes the `*`
wildcard and is a total retrieval failure as soon as a query names a type.

### Several types at once

A corpus can hold vectors from more than one model, which is what makes a model upgrade something
other than a full re-ingest with search degraded throughout:

- `EmbeddingTypeCatalog` reads the types actually present from `sysembed_type` on the embedding rows,
  cached for `rag.embedding.type-discovery.ttl-seconds`. Reading them back rather than deriving them is
  what finds a type written by a retired model. It has to be the row's own type and not the child
  document's `sys_name`: the name is always the sanitized derivation, so a name-based scan cannot see a
  row that recorded the raw model name, and would report a type matching none of those rows. Every type
  it reports therefore provably matches rows, because a row carrying it is how it was found.
- There is no aggregation endpoint over the embeddings index, so that scan is a paged wildcard vector
  query and is a sample rather than an enumeration. A type holding a very small share of a corpus larger
  than the scan ceiling can be missed, and a type nobody discovers is a type nobody queries, so the
  coverage achieved is logged. `rag.embedding.additional-models` pins the types when that matters.
- `MultiTypeVectorSearchService` runs one search per active type, each with a query vector from that
  type's own model, then normalises each type's scores by that type's best score before merging on the
  best score per chunk. Both halves matter: a query vector from one model has no meaningful similarity
  to another model's vectors, and two models' score scales differ, so without normalisation whichever
  reports larger numbers would win every comparison.
- With one active type this collapses to exactly the single wildcard call the services made before, so
  a single-model corpus is unaffected.
- A model whose vectors are still present must be listed in `rag.embedding.additional-models` for its
  type to be queried in its own space. A type present but unlisted is still searched, using the
  configured model, which is what the wildcard already did for it.
- `EmbeddingBackfillService` re-embeds the corpus into the configured type at a controlled rate,
  leaving other types in place so they keep answering queries until it finishes. It re-chunks the stored
  extracted-text mirror rather than re-extracting from the source, so it needs no source credentials but
  does not reproduce the original chunk boundaries exactly. It therefore leaves the content fingerprint
  stale on purpose, so the next content sync reprocesses that document properly. Resuming re-scans and
  skips documents that already carry the target type, which is why there is no cursor to persist.
