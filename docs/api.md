# API Usage

### Batch Ingester (port 9090)

#### Start Synchronization

```bash
# Sync configured folders
curl -X POST http://localhost:9090/api/sync/configured -u admin:admin

# Sync specific folder
curl -X POST http://localhost:9090/api/sync/batch \
  -u admin:admin \
  -H "Content-Type: application/json" \
  -d '{"folders": ["node-id"], "recursive": true, "types": ["cm:content"]}'
```

#### Monitor Progress

```bash
# Overall status
curl http://localhost:9090/api/sync/status -u admin:admin

# Job-specific status
curl http://localhost:9090/api/sync/status/{jobId} -u admin:admin
```

#### Reconcile Alfresco Permissions

Use this after an Alfresco permission change when you want to force reconciliation manually. It
updates hxpr ACLs without re-running text extraction or embeddings.

```bash
# Reconcile a single file ACL
curl -X POST http://localhost:9090/api/sync/permissions \
  -u admin:admin \
  -H "Content-Type: application/json" \
  -d '{"nodeIds":["file-node-id"],"recursive":true}'

# Reconcile a folder ACL across its descendant files
curl -X POST http://localhost:9090/api/sync/permissions \
  -u admin:admin \
  -H "Content-Type: application/json" \
  -d '{"nodeIds":["folder-node-id"],"recursive":true}'
```

#### Query Node Status

```bash
# Single node
curl http://localhost:9090/api/content-lake/nodes/{nodeId}/status -u admin:admin

# Bulk node list
curl -X POST http://localhost:9090/api/content-lake/nodes/status \
  -u admin:admin \
  -H "Content-Type: application/json" \
  -d '{"nodeIds":["node-id-1","node-id-2"]}'

# Optional: include aggregated subtree status for folders
curl -X POST http://localhost:9090/api/content-lake/nodes/status \
  -u admin:admin \
  -H "Content-Type: application/json" \
  -d '{"nodeIds":["folder-id"],"includeFolderAggregate":true}'

# Optional: same aggregation for single-folder lookup
curl "http://localhost:9090/api/content-lake/nodes/{folderId}/status?includeFolderAggregate=true" \
  -u admin:admin
```

#### Prove a Node Is Retrievable

`status` reports a claim: it is read from the node's own `cl:syncStatusValue`, and a document can hold
that value while holding no embeddings, in which case it is invisible to semantic and hybrid search but
looks finished to monitoring. `index-proof` measures instead, by counting the document's chunks on the
embeddings index:

```bash
curl "http://localhost:9090/api/content-lake/nodes/{nodeId}/index-proof" -u admin:admin

# Include more sampled chunks (bounded; the response size does not grow with the document)
curl "http://localhost:9090/api/content-lake/nodes/{nodeId}/index-proof?sampleSize=10" -u admin:admin
```

The response separates what was counted from what was claimed, and reduces to one `verdict`:

| Verdict | Meaning |
|---|---|
| `INDEXED_WITH_EMBEDDINGS` | the document exists and the embeddings index holds chunks for it |
| `METADATA_ONLY` | the document exists with zero chunks, so it cannot be retrieved |
| `ABSENT` | no document exists for this node |

`measured.embeddingTypes` lists every type the document has an embedding child for, so a child left
behind by a previously configured embedding model is visible. `claimed.sectionMapChunks` is the chunk
count ingestion believed it produced: a non-zero value against a measured `chunkCount` of zero is the
signature of an embedding phase that never completed. A `null` verdict with a populated `error` means a
measurement could not be taken, and is deliberately not a guess.

### RAG Service (port 9091)

#### RAG Prompt

Ask a question and get an LLM-generated answer grounded in your indexed Alfresco and Nuxeo documents:

```bash
curl -X POST http://localhost:9091/api/rag/prompt -u admin:admin \
  -H "Content-Type: application/json" \
  -d '{ "question": "What are the key findings in the Q4 report?" }'
```

With options:

```bash
curl -X POST http://localhost:9091/api/rag/prompt -u admin:admin \
  -H "Content-Type: application/json" \
  -d '{
    "question": "Summarize the budget proposal",
    "sourceType": "nuxeo",
    "topK": 10,
    "minScore": 0.6,
    "includeContext": true
  }'
```

Multi-turn conversation (same `sessionId`):

```bash
# Turn 1
curl -X POST http://localhost:9091/api/rag/prompt -u admin:admin \
  -H "Content-Type: application/json" \
  -d '{
    "sessionId": "demo-session-1",
    "question": "Summarize the Q4 report highlights"
  }'

# Turn 2 (follow-up resolved with history)
curl -X POST http://localhost:9091/api/rag/prompt -u admin:admin \
  -H "Content-Type: application/json" \
  -d '{
    "sessionId": "demo-session-1",
    "question": "Can you expand on the second point?"
  }'
```

Response:

```json
{
  "answer": "The Q4 report highlights a 12% revenue increase...",
  "question": "What are the key findings in the Q4 report?",
  "sessionId": "demo-session-1",
  "retrievalQuery": "what are the key findings in the q4 report",
  "historyTurnsUsed": 2,
  "model": "ai/gpt-oss",
  "tokenCount": 672,
  "searchTimeMs": 245,
  "generationTimeMs": 1830,
  "totalTimeMs": 2075,
  "sourcesUsed": 3,
  "sources": [
    {
      "documentId": "abc-123",
      "sourceId": "nuxeo:nuxeo-demo",
      "sourceType": "nuxeo",
      "nodeId": "e4f5a6b7-...",
      "name": "Q4-Financial-Report.pdf",
      "path": "/default-domain/workspaces/finance",
      "openInSourceUrl": "http://localhost:8081/nuxeo/ui/#!/browse/default-domain/workspaces/finance/Q4-Financial-Report.pdf",
      "chunkText": "Revenue for Q4 increased by 12%...",
      "score": 0.87
    }
  ]
}
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `question` | String | *required* | Natural-language question |
| `sessionId` | String | user-scoped default | Conversation session id for multi-turn context |
| `resetSession` | boolean | false | Clear conversation history for the target session before this prompt |
| `topK` | int | server default (`rag.default-top-k`, 15) | Number of chunks to retrieve for context |
| `minScore` | double | server default (`rag.default-min-score`, 0.01) | Minimum similarity threshold |
| `filter` | String | -- | Additional HXQL filter |
| `sourceType` | String | -- | Optional source filter: `alfresco` or `nuxeo` |
| `embeddingType` | String | model default | Embedding type to match |
| `systemPrompt` | String | -- | Override the default LLM system prompt |
| `includeContext` | boolean | false | Include retrieved chunks in response |
| `inferFilters` | boolean | false | Let the service infer metadata filters from the question intent |

| Response Field | Type | Description |
|---------------|------|-------------|
| `sessionId` | String | Effective session id used by server |
| `retrievalQuery` | String | Query actually sent to retrieval (may be reformulated) |
| `historyTurnsUsed` | Integer | Number of prior turns included in this generation |
| `tokenCount` | Integer | Total token usage (prompt + completion) when provider reports it |
| `sources[].sourceType` | String | Source type for each cited document |
| `sources[].openInSourceUrl` | String | Native-source deep link (Share for Alfresco, Web UI for Nuxeo) |
| `sources[].chunkType` | String | Chunk classification `PROSE`/`TABLE` (from the section map); omitted when unknown |
| `currentSummary` | String | Persistent running conversation summary when `rag.conversation.summary.enabled=true`; otherwise null |
| `verified` | Boolean | Citation-faithfulness result when `rag.citation.verify.enabled=true`; otherwise null |
| `unsupportedClaims` | String[] | Answer claims not grounded in the cited sources (citation verification only) |

#### Chat Stream (SSE)

Streaming responses are available with Server-Sent Events (SSE).

- Canonical endpoint: `GET /api/rag/chat/stream`
- Backward-compatible endpoint: `POST /api/rag/chat/stream` (same JSON body as `/api/rag/prompt`)
- Content type: `text/event-stream`
- Authentication: same as other `/api/rag/**` endpoints (Basic Auth or Alfresco ticket)

`GET` example:

```bash
curl -N -G http://localhost:9091/api/rag/chat/stream -u admin:admin \
  --data-urlencode "question=What changed in Q4?" \
  --data-urlencode "sessionId=demo-session-1" \
  --data-urlencode "resetSession=false" \
  --data-urlencode "topK=5" \
  --data-urlencode "minScore=0.5"
```

Compatibility `POST` example:

```bash
curl -N -X POST http://localhost:9091/api/rag/chat/stream -u admin:admin \
  -H "Content-Type: application/json" \
  -d '{
    "question": "What changed in Q4?",
    "sessionId": "demo-session-1",
    "topK": 5,
    "minScore": 0.5
  }'
```

Query params for `GET`:

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `question` | String | *required* | Natural-language question |
| `sessionId` | String | user-scoped default | Conversation session id for multi-turn context |
| `resetSession` | boolean | false | Clear conversation history before this prompt |
| `topK` | int | 5 | Number of chunks to retrieve for context |
| `minScore` | double | 0.5 | Minimum similarity threshold |
| `filter` | String | -- | Additional HXQL filter |
| `sourceType` | String | -- | Optional source filter: `alfresco` or `nuxeo` |
| `embeddingType` | String | model default | Embedding type to match |
| `systemPrompt` | String | -- | Override the default LLM system prompt |
| `includeContext` | boolean | false | Include retrieved chunks in final metadata |

SSE events:

- `event: token` incremental token payload (`{"token":"..."}`)
- `event: metadata` final payload with `RagPromptResponse` fields including `sources`, timing fields, `model`, and `tokenCount`
- `event: done` terminal success event
- `event: error` terminal failure event with error message

Example stream:

```text
event: token
data: {"token":"Revenue "}

event: token
data: {"token":"grew 12% in Q4."}

event: metadata
data: {"answer":"Revenue grew 12% in Q4.","question":"What changed in Q4?","model":"ai/gpt-oss","tokenCount":672,"searchTimeMs":245,"generationTimeMs":1830,"totalTimeMs":2075,"sourcesUsed":3,"sources":[{"documentId":"abc-123","sourceId":"nuxeo:nuxeo-demo","sourceType":"nuxeo","nodeId":"e4f5a6b7-...","name":"Q4-Financial-Report.pdf","path":"/default-domain/workspaces/finance","openInSourceUrl":"http://localhost:8081/nuxeo/ui/#!/browse/default-domain/workspaces/finance/Q4-Financial-Report.pdf","chunkText":"Revenue for Q4 increased by 12%...","score":0.87}]}

event: done
data: {"status":"ok"}
```

Error stream example:

```text
event: error
data: {"message":"Failed to prepare RAG stream: ..."}
```

#### Semantic Search

Search directly against the embedded chunks without LLM generation:

```bash
curl -X POST http://localhost:9091/api/rag/search/semantic -u admin:admin \
  -H "Content-Type: application/json" \
  -d '{ "query": "contract renewal terms", "topK": 5, "minScore": 0.6 }'
```

Semantic search applies a minimum similarity score to suppress low-quality vector matches when no strong semantic relation exists.

Results can include both Alfresco and Nuxeo hits in the same response. Each hit now includes `sourceType` and `openInSourceUrl` so clients can label and open the native source system directly.

```json
{
  "query": "contract renewal terms",
  "resultCount": 2,
  "results": [
    {
      "rank": 1,
      "score": 0.91,
      "chunkText": "The renewal clause starts on page 3...",
      "sourceDocument": {
        "documentId": "doc-alf-1",
        "sourceId": "alfresco:repo-main",
        "sourceType": "alfresco",
        "nodeId": "550e8400-e29b-41d4-a716-446655440000",
        "name": "Vendor Contract.pdf",
        "path": "/Company Home/Sites/legal/documentLibrary",
        "mimeType": "application/pdf",
        "openInSourceUrl": "http://localhost:80/share/page/document-details?nodeRef=workspace://SpacesStore/550e8400-e29b-41d4-a716-446655440000"
      }
    },
    {
      "rank": 2,
      "score": 0.88,
      "chunkText": "Renewal requires 30 days notice...",
      "sourceDocument": {
        "documentId": "doc-nux-1",
        "sourceId": "nuxeo:nuxeo-demo",
        "sourceType": "nuxeo",
        "nodeId": "660e8400-e29b-41d4-a716-446655440000",
        "name": "Supplier Agreement.docx",
        "path": "/default-domain/workspaces/legal",
        "mimeType": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        "openInSourceUrl": "http://localhost:8081/nuxeo/ui/#!/browse/default-domain/workspaces/legal/Supplier%20Agreement.docx"
      }
    }
  ]
}
```

* Default value: `0.2` (`semantic-search.default-min-score`)
* Applied server-side after vector retrieval
* Can be overridden per request

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `query` | String | *required* | Query text; embedded and matched against the chunk vectors |
| `topK` | int | `5` | Chunks to return, maximum 50. A long document may contribute several |
| `topDocuments` | Integer | -- | Distinct documents to return chunks from, maximum 50. Owns the budget when present |
| `chunksPerDocument` | Integer | -- | Chunks any one document may contribute, maximum 10 |
| `minScore` | Double | `0.2` | Minimum cosine score a chunk must reach; an explicit `0.0` disables the threshold |
| `filter` | String | -- | Additional raw HXQL filter |
| `namedQuery` | String | -- | Server-side named query resolved to an HXQL fragment |
| `sourceType` | String | -- | Restricts the request to one source system, for example `alfresco` or `nuxeo` |
| `embeddingType` | String | -- | Restricts retrieval to one embedding type; all types when absent |

`topK` is a budget of chunks and `topDocuments` a budget of documents. When `topDocuments` is present it owns
the result budget and `topK` is ignored; when it is absent nothing about the request changes. A value above a
maximum is clamped rather than rejected, and the clamped value is reported back in `appliedTopDocuments` /
`appliedChunksPerDocument`; zero or negative is rejected with 400, having no such reading.

The chunks a document contributes are bounded either way: by `chunksPerDocument` when the request sets it,
otherwise by `rag.retrieval.document-diversity.max-chunks-per-document`. So a `topDocuments` answer can be
shorter than `topDocuments * chunksPerDocument`, because a document outside the budget is never admitted to
fill the remainder. `documentCount` is what says how many documents actually answered.

```bash
curl -X POST http://localhost:9091/api/rag/search/semantic -u admin:admin \
  -H "Content-Type: application/json" \
  -d '{ "query": "contract renewal terms", "topDocuments": 10, "chunksPerDocument": 2 }'
```

| Response Field | Type | Description |
|---------------|------|-------------|
| `query` | String | Original query |
| `resultCount` | int | Chunks returned |
| `documentCount` | Integer | Distinct documents those chunks belong to; always reported |
| `appliedTopDocuments` | Integer | The document budget as applied after clamping; absent when none was asked for |
| `appliedChunksPerDocument` | Integer | The per-document bound as applied; absent when no document budget was asked for |
| `totalCount` | Integer | Matching chunks in the index, where hxpr reports it |
| `searchTimeMs` | long | Total search execution time |
| `results[]` | array | Chunks, best first, with `rank`, `score`, `chunkText`, `sourceDocument` and `chunkMetadata` |

#### Hybrid Search

Run vector + keyword retrieval and fuse results with `rrf` (default) or `weighted` scoring:

```bash
curl -X POST http://localhost:9091/api/rag/search/hybrid -u admin:admin \
  -H "Content-Type: application/json" \
  -d '{
    "query": "budget approval process",
    "strategy": "rrf",
    "candidateCount": 20,
    "maxResults": 5,
    "metadata": {
      "mimeType": "application/pdf",
      "pathPrefix": "/Company Home/Sites/finance/documentLibrary",
      "modifiedAfter": "2026-01-01T00:00:00Z",
      "modifiedBefore": "2026-12-31T23:59:59Z",
      "properties": {
        "cm:title": "Budget"
      }
    }
  }'
```

Structured metadata filters are optional. You can still pass a raw HXQL `filter` for advanced cases.
Use `sourceType` when you want to restrict the request to a single source system without writing raw HXQL.

`topDocuments` and `chunksPerDocument` behave exactly as on semantic search, taking the place of `maxResults`
when present. Asking for one raises the candidates each leg retrieves, since a document budget cannot be met
out of a pool the size of the answer; `candidateCount` and `maxResults` keep their own maximum of 100.

Response example:

```json
{
  "query": "budget approval process",
  "strategy": "weighted",
  "normalization": "max",
  "model": "ai/mxbai-embed-large",
  "resultCount": 2,
  "vectorCandidates": 20,
  "keywordCandidates": 18,
  "searchTimeMs": 143,
  "results": [
    {
      "rank": 1,
      "score": 0.0325,
      "chunkText": "The budget approval workflow starts with...",
      "sourceDocument": {
        "documentId": "doc-nux-1",
        "sourceId": "nuxeo:nuxeo-demo",
        "sourceType": "nuxeo",
        "nodeId": "660e8400-e29b-41d4-a716-446655440000",
        "name": "Budget Policy.pdf",
        "path": "/default-domain/workspaces/finance",
        "mimeType": "application/pdf",
        "openInSourceUrl": "http://localhost:8081/nuxeo/ui/#!/browse/default-domain/workspaces/finance/Budget%20Policy.pdf"
      },
      "vectorScore": 0.87,
      "keywordScore": 1.0,
      "vectorRank": 2,
      "keywordRank": 1
    }
  ]
}
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `query` | String | *required* | Query for both vector and keyword legs |
| `strategy` | String | `rrf` | Fusion strategy: `rrf` or `weighted` |
| `normalization` | String | `max` | Weighted score normalization: `max` or `minmax` |
| `candidateCount` | int | `20` | Candidates retrieved from each leg before fusion |
| `maxResults` | int | `5` | Final fused result limit |
| `topDocuments` | Integer | -- | Distinct documents to return chunks from, maximum 50. Owns the budget when present, and `maxResults` is ignored |
| `chunksPerDocument` | Integer | -- | Chunks any one document may contribute, maximum 10 |
| `vectorWeight` | double | `0.7` | Weight when `strategy=weighted` |
| `textWeight` | double | `0.3` | Weight when `strategy=weighted` |
| `filter` | String | -- | Additional raw HXQL filter |
| `sourceType` | String | -- | Optional source filter: `alfresco` or `nuxeo` |
| `metadata.mimeType` | String | -- | MIME type filter (for example `application/pdf`) |
| `metadata.pathPrefix` | String | -- | Path prefix filter (starts-with match) |
| `metadata.modifiedAfter` | String | -- | Inclusive lower bound for `source_modifiedAt`; see the note below |
| `metadata.modifiedBefore` | String | -- | Inclusive upper bound for `source_modifiedAt`; see the note below |
| `metadata.properties` | Map<String,String> | -- | Exact-match filters on `cin_ingestProperties.<key>` |

#### The date bounds are string comparisons, so they depend on how a document was stored

`modifiedAfter` and `modifiedBefore` become HXQL range predicates over the stored text of
`source_modifiedAt`, not over a parsed instant. Ingestion writes that value in one fixed-width UTC form,
`uuuu-MM-dd'T'HH:mm:ss.SSSSSSSSS'Z'`, precisely so the comparison orders correctly, and a source cannot
override the format.

**A document ingested before that was enforced may still hold a variable-width value, and is filtered
imprecisely.** Until this was fixed (#149) the Alfresco and Nuxeo adapters wrote
`OffsetDateTime.toString()`, which elides zero seconds: a document modified at exactly `10:00:00` was stored
as `2026-09-17T10:00Z`, whose `Z` sorts after the `:` of any bound carrying seconds, so it was excluded from
ranges it plainly fell inside.

**Re-running a sync does not repair it**, and this is the part worth knowing before planning around it. The
staleness check parses the stored value rather than comparing it, so an untouched document is correctly
judged current and its metadata is never rewritten. A document's stored form is therefore normalised only
when the source genuinely modifies it, or when the index is rebuilt from empty. Bound a query you need to
be exact to whole minutes, or re-ingest.

Pass bounds in the same shape you would store: a full `uuuu-MM-ddTHH:mm:ssZ` at minimum. A bound with no
seconds has the mirror-image problem against correctly stored values.

| Response Field | Type | Description |
|---------------|------|-------------|
| `query` | String | Original query |
| `strategy` | String | Effective fusion strategy used |
| `normalization` | String | Normalization mode used when `strategy=weighted` |
| `model` | String | Embedding model used for vector search |
| `resultCount` | int | Number of fused results returned |
| `vectorCandidates` | int | Number of vector candidates retrieved |
| `keywordCandidates` | int | Number of keyword candidates retrieved |
| `documentCount` | Integer | Distinct documents the fused results belong to; always reported |
| `appliedTopDocuments` | Integer | The document budget as applied after clamping; absent when none was asked for |
| `appliedChunksPerDocument` | Integer | The per-document bound as applied; absent when no document budget was asked for |
| `searchTimeMs` | long | Total hybrid search execution time |
| `results[].score` | double | Fused score (RRF or weighted) |
| `results[].vectorScore` | Double | Raw vector score, if available |
| `results[].keywordScore` | Double | Raw keyword score, if available |
| `results[].sourceDocument` | object | Source document metadata |
| `results[].chunkMetadata` | object | Chunk position/type metadata |

##### Integration Smoke Test (local hxpr)

Use this checklist to validate issue #14 end-to-end:

1. Ensure at least one folder is ingested into hxpr via batch/live ingesters.
2. Call hybrid search without metadata constraints and verify `resultCount > 0`.
3. Call hybrid search with a restrictive metadata filter (for example `mimeType: application/pdf`) and confirm results narrow.
4. Switch strategy to `weighted` and confirm response field `strategy` is `weighted`.
5. Confirm Nuxeo hits expose `openInSourceUrl` values that open in Nuxeo Web UI.

Example smoke-test requests:

```bash
# Baseline
curl -X POST http://localhost:9091/api/rag/search/hybrid -u admin:admin \
  -H "Content-Type: application/json" \
  -d '{"query":"budget approval process","strategy":"rrf","candidateCount":20,"maxResults":5}'

# Restrictive metadata
curl -X POST http://localhost:9091/api/rag/search/hybrid -u admin:admin \
  -H "Content-Type: application/json" \
  -d '{"query":"budget approval process","strategy":"rrf","sourceType":"nuxeo","metadata":{"mimeType":"application/pdf"}}'

# Weighted strategy
curl -X POST http://localhost:9091/api/rag/search/hybrid -u admin:admin \
  -H "Content-Type: application/json" \
  -d '{"query":"budget approval process","strategy":"weighted","normalization":"minmax","vectorWeight":0.7,"textWeight":0.3}'
```

#### Faceted Search

Discover the indexed values of a property (with counts) so clients can build informed filters.
Buckets are scoped to the caller's document permissions.

```bash
curl -X POST http://localhost:9091/api/rag/search/facets -u admin:admin \
  -H "Content-Type: application/json" \
  -d '{ "property": "source_mimeType", "topN": 10, "sourceType": "alfresco" }'
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `property` | String | *required* | Property to aggregate on (400 when blank) |
| `filter` | String | -- | Additional raw HXQL filter |
| `sourceType` | String | -- | Optional source filter: `alfresco` or `nuxeo` |
| `searchTerm` | String | -- | Restrict buckets to values matching a term |
| `topN` | int | service default | Maximum number of buckets to return |

The response is `{ "property": "...", "buckets": [ { "value": "...", "count": N }, ... ] }`.

#### Named Queries

List the hxpr named-query definitions registered server-side, to offer them as saved-search filters.
Apply one by passing `namedQuery` on a search request (an alternative to an inline `filter`).

```bash
curl http://localhost:9091/api/rag/named-queries -u admin:admin
# -> ["recent-contracts","hr-policies"]
```

#### Session Summary

Return the persistent running conversation summary for a session (also returned inline on
`/api/rag/prompt` as `currentSummary`). Returns 404 when `rag.conversation.summary.enabled=false`.

```bash
curl http://localhost:9091/api/rag/sessions/user:alice/summary -u admin:admin
# -> {"sessionId":"user:alice","summary":"..."}   (summary null when none yet)
```

#### In-App Evaluation (smoke)

Runs a small caller-supplied sample set through the live pipeline and returns coarse retrieval-hit
and faithfulness signals - a quick sanity check, not the authoritative quality gate
(`content-lake-eval` remains that). Disabled by default; enable with `rag.evaluation.enabled=true`
(returns 403 when disabled).

```bash
curl -X POST http://localhost:9091/api/rag/evaluate -u admin:admin \
  -H "Content-Type: application/json" \
  -d '[
    {
      "question": "What are the key findings in the Q4 report?",
      "expectedAnswer": "Revenue grew 12%.",
      "expectedSourceIds": ["nuxeo:nuxeo-demo"]
    }
  ]'
```

The response reports `totalSamples`, `retrievalHits`, and `retrievalHitRate` across the sample set.

### Health Checks

Every service publishes `/actuator/health` and `/actuator/info` without credentials, so a container
orchestrator can probe them. Everything else on every service, `/actuator/metrics` included, requires
authentication.

```bash
# Batch ingester (no auth required)
curl http://localhost:9090/actuator/health

# Live ingester (no auth required)
curl http://localhost:9092/actuator/health

# RAG service (no auth required)
curl http://localhost:9091/actuator/health

# RAG service detailed health (auth required)
curl http://localhost:9091/api/rag/health -u admin:admin

# Metrics on any service (auth required)
curl http://localhost:9091/actuator/metrics -u admin:admin
```

#### Ingest Metrics

Every ingester publishes how much re-embedding the content fingerprint avoided, tagged with the source
type. Embedding is the pipeline's bottleneck, so this is the number that says what the fingerprint is
worth, and a value that stops growing is the signal that something started perturbing the fingerprint
inputs.

| Metric | Meaning |
|---|---|
| `contentlake.ingest.content.shortcircuits` | Documents whose chunking and embedding were skipped because the content had not changed |
| `contentlake.ingest.content.reprocesses` | Documents that were chunked and embedded |

```bash
curl http://localhost:9092/actuator/metrics/contentlake.ingest.content.shortcircuits -u admin:admin
```

Both are per-process and start at zero on restart, as any counter does. Expect the live ingesters to
dominate the short-circuit count: a batch sync skips content entirely for an unchanged
`source_modifiedAt` before the fingerprint is reached, so most batch passes never get as far as the short
circuit.

### Live Ingester (port 9092)

The live ingester consumes Alfresco Event2 messages from ActiveMQ using Alfresco Java SDK handler interfaces such as `OnNodeUpdatedEventHandler` and `OnPermissionUpdatedEventHandler`.

It reuses the same shared ingestion pipeline as the batch ingester:

- Fetch the current node snapshot from Alfresco REST API
- Apply scope and exclusion rules
- Sync metadata to hxpr
- Extract text with Transform Service
- Chunk and embed with Spring AI
- Update permissions or delete when nodes move out of scope

Permission reconciliation is separate from content updates:

- Content and scope changes are handled through Event2 live ingestion.
- Alfresco permission changes should be reconciled through `POST /api/sync/permissions` in `alfresco-batch-ingester` because the repository does not reliably emit permission update events.
- In production, the `content-lake-repo-model` addon inside Alfresco Repository should detect ACL changes after commit and publish a persistent ActiveMQ queue message. `alfresco-batch-ingester` consumes that queue and runs the same ACL reconciliation path.
- If a permission event is emitted, the live ingester can still process it, but that path is best-effort rather than the primary contract.

When the live ingester does receive a permission-related event, it distinguishes between file and folder targets:

- **File-level event**: the ACL is updated only for that file (`updatePermissions`) -- no content re-extraction or embedding regeneration.
- **Folder-level event**: the live ingester walks the full descendant subtree and applies an ACL-only update to every indexed file beneath the folder. This covers three event types that can signal a folder ACL change: `PERMISSION_UPDATED`, `PEER_ASSOC_CREATED`, and `PEER_ASSOC_DELETED`. A fourth handler (`FolderPermissionFallbackHandler`) catches `NODE_UPDATED` events on folders where only the ACL changed (no structural diff), providing a safety net for sources that do not emit a dedicated permission event.

Folder-level propagation behaviour:

- Descendant files with `isInheritanceEnabled: false` keep their locally-set ACL unchanged -- the folder's new permissions are not pushed down to them.
- Descendant files with inheritance enabled receive a recomputed ACL derived from the folder's current Alfresco permissions snapshot.
- Files that fall outside scope after the change are deleted from hxpr rather than updated.
- The propagation never re-ingests content; it is strictly an ACL patch.

The live path is guarded by the same `alfresco_modifiedAt` staleness check used by batch ingestion, so batch and live runs can coexist safely.

Status endpoint:

```bash
curl http://localhost:9092/api/live/status
```
