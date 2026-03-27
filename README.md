# Alfresco Content Lake

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Java](https://img.shields.io/badge/Java-21-orange.svg)](https://openjdk.org/projects/jdk/21/)
[![Spring Boot](https://img.shields.io/badge/Spring%20Boot-3.4.3-brightgreen.svg)](https://spring.io/projects/spring-boot)
[![Maven](https://img.shields.io/badge/Maven-3.9+-red.svg)](https://maven.apache.org/)
[![Docker](https://img.shields.io/badge/Docker-Compose-blue.svg)](https://docs.docker.com/compose/)
[![Status](https://img.shields.io/badge/Status-PoC-yellow.svg)]()

**AI-powered semantic search and RAG for Alfresco using hxpr Content Lake**

[Features](#features) • [Quick Start](#quick-start) • [Architecture](#architecture) • [Authentication](#authentication) • [API Usage](#api-usage) • [Configuration](#configuration)

## Related Projects

- [alfresco-content-lake-ui](https://github.com/aborroy/alfresco-content-lake-ui) - ACA-based frontend for semantic search and RAG over Content Lake.
- [alfresco-content-lake-deploy](https://github.com/aborroy/alfresco-content-lake-deploy) - Docker Compose deployment for Alfresco, hxpr, Content Lake services, and the UI.
- [nuxeo-deployment](https://github.com/aborroy/nuxeo-deployment) - Companion project that builds and runs the local Nuxeo server. Required when using `compose.nuxeo.yaml` in this repo.

## Overview

Proof of Concept for AI-powered semantic search and Retrieval-Augmented Generation (RAG) on Alfresco Content Services.

Leverages **hxpr** as a Content Lake to enable high-quality AI search while:

* Keeping Alfresco as the source of truth
* Enforcing server-side permissions via ACLs
* Supporting on-premises AI execution
* Minimizing data duplication

## Features

- Two-Phase Sync Pipeline: Fast metadata ingestion + async content processing
- Near Real-Time Sync: Alfresco Event2 listener over ActiveMQ using the Alfresco Java SDK
- Semantic Search: Vector embeddings with permission-aware kNN search
- RAG: LLM-powered question answering grounded in Alfresco document content
- Permission-Aware: Server-side ACL enforcement via hxpr
- Local AI: On-premises LLM and embedding models using Spring AI
- Repository Scope Model: `cl:indexed` and `cl:excludeFromLake` for Alfresco-native scope control
- REST API: Generic connector using Alfresco REST APIs
- Secured Endpoints: Alfresco authentication (username/password or tickets)
- Shared Ingestion Core: Common metadata, transform, chunking, embedding, ACL, and delete/update logic in `content-lake-core`
- Idempotent Coexistence: `alfresco_modifiedAt` guard prevents stale batch/live writes from overwriting newer content

## Architecture

```text
                 ┌──────────────────────────────────────┐
                 │ Alfresco Repository + Event2         │
                 │ REST API + ActiveMQ topic            │
                 └──────────────────────────────────────┘
                          │                     │
                          │                     │
                          ▼                     ▼
┌──────────────────────────────────────┐   ┌──────────────────────────────────────┐
│ batch-ingester                       │   │ live-ingester                        │
│ Discovery → Metadata → Queue         │   │ SDK Handlers → Filter → Sync         │
└──────────────────────────────────────┘   └──────────────────────────────────────┘
                          │                     │
                          └──────────┬──────────┘
                                     ▼
               ┌──────────────────────────────────────────┐
               │ content-lake-common                      │
               │ Node sync, Transform, Chunk, Embed, ACL  │
               │ `alfresco_modifiedAt` idempotency guard  │
               └──────────────────────────────────────────┘
                                     ▼
               ┌──────────────────────────────────────────┐
               │ hxpr Content Lake                        │
               └──────────────────────────────────────────┘
                                     ▼
               ┌──────────────────────────────────────────┐
               │ rag-service                              │
               │ Query → Embed → Search → Augment → LLM   │
               └──────────────────────────────────────────┘
```

### Modules

| Module | Port | Description |
|--------|------|-------------|
| `content-lake-repo-model` | — | Alfresco repository JAR that bootstraps the `cl:indexed` content model for scope control |
| `content-lake-core` | — | Shared clients and ingestion pipeline: metadata sync, transform, chunking, embedding, ACL updates, idempotency |
| `content-lake-source-nuxeo` | — | Nuxeo REST clients, scope resolver, auth abstraction, and server-side conversion-backed text extraction |
| `alfresco-batch-ingester` | 9090 | Alfresco folder discovery, batch scheduling, metadata enqueueing, and `/api/sync/*` controllers |
| `nuxeo-batch-ingester` | 9093 | Nuxeo full-batch discovery and one-shot sync, using NXQL by default with `@children` fallback |
| `alfresco-live-ingester` | 9092 | Alfresco Event2 listener over ActiveMQ using Alfresco Java SDK handlers and filters |
| `nuxeo-live-ingester` | 9094 | Nuxeo audit-stream listener using a persisted watermark and no full repository scans |
| `rag-service` | 9091 | Semantic search and RAG question answering |

## Quick Start

### Prerequisites

- Java 21+ and Maven 3.9+
- Docker and Docker Compose
- Alfresco Content Services 25.x+
  - Alfresco Transform Service (for text extraction)
- hxpr Content Lake (with OAuth2 IDP)
- Docker Model Runner (for embeddings and LLM)

### Installation

```bash
# Clone repository
git clone https://github.com/aborroy/alfresco-content-lake.git
cd alfresco-content-lake

# Build all modules
mvn clean package

# Deploy the repository content model to ACS before starting the ingesters
# Artifact:
#   content-lake-repo-model/target/content-lake-repo-model-1.0.0-SNAPSHOT.jar
# Deploy it to the Alfresco Repository classpath.

# Configure (see Environment Variables below)
export ALFRESCO_URL=http://localhost:8080
export ALFRESCO_INTERNAL_USERNAME=admin
export ALFRESCO_INTERNAL_PASSWORD=admin
# ... (see full configuration below)

# Run batch ingestion
java -jar batch-ingester/target/batch-ingester-1.0.0-SNAPSHOT.jar

# Run live ingestion
java -jar live-ingester/target/live-ingester-1.0.0-SNAPSHOT.jar

# Run RAG service
java -jar rag-service/target/rag-service-1.0.0-SNAPSHOT.jar

# Or with Docker Compose (both services)
docker-compose up
```

### Alfresco Repo Model

The batch and live ingesters now rely on an Alfresco content model for scope control:

- `cl:indexed` marks a folder subtree as in scope for Content Lake ingestion
- `cl:excludeFromLake` lets a file opt out, or a folder subtree opt out, even when an ancestor folder is indexed

Build artifact:

```bash
content-lake-repo-model/target/content-lake-repo-model-1.0.0-SNAPSHOT.jar
```

Deploy that JAR to the Alfresco Repository classpath before enabling ingestion. Typical options are:

- include it in an ACS SDK `modules/platform` build
- copy or mount it into an Alfresco Repository image under `webapps/alfresco/WEB-INF/lib`

### Starting From A Non-Indexed Repository

If your Alfresco Repository does not yet use `cl:indexed`, the recommended startup sequence is:

1. Build the project and deploy the repository model JAR to Alfresco Repository.
   After deployment, restart the repository so `cl:indexed` and `cl:excludeFromLake` are available.
2. Start `batch-ingester`.
3. Run a batch synchronization against the folder you want to onboard.
   The ingester automatically adds `cl:indexed` to each root folder if it is not already present, then performs the initial backfill into Content Lake.
4. Start `live-ingester`.
   Live ingestion then keeps that indexed subtree up to date.

Example for indexing all sites under `Company Home/Sites`:

1. Resolve the Alfresco node id for `Company Home/Sites`.
   You can obtain it from Alfresco UI tools or the Alfresco REST API.
2. Run the batch sync against that folder:

```bash
curl -X POST http://localhost:9090/api/sync/batch \
  -u admin:admin \
  -H "Content-Type: application/json" \
  -d '{"folders":["SITES_FOLDER_NODE_ID"],"recursive":true,"types":["cm:content"]}'
```

This single call marks `SITES_FOLDER_NODE_ID` with `cl:indexed` (if needed) and ingests all existing content beneath it.

3. After the batch completes, start `live-ingester` so new or changed content under `Company Home/Sites` continues to sync automatically.

Important:

- `cl:indexed` can also be set directly via the Alfresco Repository nodes API or the Content Lake UI extension; the batch ingester sets it automatically only for root folders passed in the request
- `cl:excludeFromLake` on a folder removes that folder's full subtree from Content Lake scope; batch discovery skips it and live reconciliation deletes previously ingested descendants
- if you later want to index only one site, pass that site folder to `/api/sync/batch` instead of `Company Home/Sites`

### Environment Variables

```bash
# Alfresco (Internal Service Account)
export ALFRESCO_URL=http://localhost:8080
export ALFRESCO_INTERNAL_USERNAME=admin
export ALFRESCO_INTERNAL_PASSWORD=admin

# hxpr Content Lake
export HXPR_URL=http://localhost:8080
export HXPR_REPOSITORY_ID=default
export HXPR_IDP_TOKEN_URL=http://localhost:5002/idp/connect/token
export HXPR_IDP_CLIENT_ID=nuxeo-client
export HXPR_IDP_CLIENT_SECRET=secret
export HXPR_IDP_USERNAME=testuser
export HXPR_IDP_PASSWORD=password

# Transform Service (batch-ingester only)
export TRANSFORM_URL=http://localhost:10090
export TRANSFORM_ENABLED=true

# ActiveMQ / Event2 (live-ingester only)
export ACTIVEMQ_URL=tcp://localhost:61616
export ACTIVEMQ_USER=admin
export ACTIVEMQ_PASSWORD=admin
export ALFRESCO_EVENT_TOPIC=alfresco.repo.event2

# Nuxeo (Nuxeo ingesters + rag-service authority lookup)
export NUXEO_URL=http://localhost:8081/nuxeo
export NUXEO_USERNAME=Administrator
export NUXEO_PASSWORD=Administrator
export NUXEO_SOURCE_ID=local

# AI/Embeddings (both services)
# Spring AI appends /v1 itself; use the Docker Model Runner root URL.
export MODEL_RUNNER_URL=http://localhost:12434
export EMBEDDING_MODEL=ai/mxbai-embed-large

# LLM (rag-service only)
export LLM_MODEL=ai/gpt-oss
export LLM_TEMPERATURE=0.3
export LLM_MAX_TOKENS=1024

# RAG defaults (rag-service only)
export RAG_DEFAULT_TOP_K=5
export RAG_DEFAULT_MIN_SCORE=0.5
export RAG_MAX_CONTEXT_LENGTH=12000

# Performance (batch-ingester only)
export TRANSFORM_WORKERS=4
export EMBEDDING_CHUNK_SIZE=900
export EMBEDDING_CHUNK_OVERLAP=120
```

## Nuxeo Backfill And Live Sync

`compose.nuxeo.yaml` starts both the `nuxeo-batch-ingester` and the
`nuxeo-live-ingester`. The Nuxeo server itself is provided by the
[nuxeo-deployment](https://github.com/aborroy/nuxeo-deployment) companion
project, which must be running before you start this stack.

**Step 1 — start Nuxeo** (in a separate terminal, from the sibling
`nuxeo-deployment/` directory):

```bash
git clone https://github.com/aborroy/nuxeo-deployment.git ../nuxeo-deployment
cd ../nuxeo-deployment
docker compose up --build
```

Nuxeo will be available at `http://localhost:8081/nuxeo` once healthy.

**Step 2 — start the Nuxeo ingesters** (from this directory):

```bash
docker compose -f compose.nuxeo.yaml up --build
```

This starts:

- `nuxeo-batch-ingester` on `http://localhost:9093` for one-shot backfills
- `nuxeo-live-ingester` on `http://localhost:9094` for audit-driven incremental sync

Defaults:

- Nuxeo credentials: `Administrator` / `Administrator`
- Discovery mode: `NXQL`
- Included roots: `/default-domain/workspaces`
- Included types: `File`, `Note`

Trigger a full configured backfill:

```bash
curl -X POST http://localhost:9093/api/sync/configured \
  -u Administrator:Administrator
```

Trigger a custom backfill with request overrides:

```bash
curl -X POST http://localhost:9093/api/sync/batch \
  -u Administrator:Administrator \
  -H "Content-Type: application/json" \
  -d '{
    "includedRoots": ["/default-domain/workspaces"],
    "includedDocumentTypes": ["File", "Note"],
    "excludedLifecycleStates": ["deleted"],
    "pageSize": 50,
    "discoveryMode": "NXQL"
  }'
```

Check status:

```bash
curl http://localhost:9093/api/sync/status -u Administrator:Administrator
curl http://localhost:9093/api/sync/status/{jobId} -u Administrator:Administrator
```

The live listener has no manual sync API. Use the actuator endpoints for
health and metrics:

```bash
curl http://localhost:9094/actuator/health
curl http://localhost:9094/actuator/metrics
```

When using the deployment repo's reverse proxy, the public sync API remains `/api/sync/*`.
Route to Nuxeo by adding `?sourceType=nuxeo`; omit it or use `alfresco` for the existing Alfresco ingester.

## Authentication

REST API authentication is source-specific:

- Alfresco ingesters validate incoming credentials or tickets against Alfresco.
- `nuxeo-batch-ingester` uses HTTP Basic auth with the configured Nuxeo service credentials.
- `nuxeo-live-ingester` does not expose sync APIs; health and metrics come from Spring Actuator.

### Supported Methods

| Method | Example |
|--------|---------|
| **Basic Auth** | `curl -u admin:password http://localhost:9090/api/sync/status` |
| **Ticket (query)** | `curl "http://localhost:9090/api/sync/status?alf_ticket=TICKET_xxx"` |
| **Ticket (header)** | `curl -H "Authorization: Basic BASE64(TICKET_xxx)" ...` |

**Note:** Bearer token authentication (OAuth2/OIDC with Keycloak) is not yet supported.

### Source-Native ACL Filtering

Current mixed-source filtering keeps Alfresco and Nuxeo principals source-native:

- Ingested ACLs are written to hxpr with the source instance suffix `_#_<sourceId>`.
- Alfresco and Nuxeo principals are not normalized to a shared identity yet.
- `rag-service` expands Alfresco groups from Alfresco and Nuxeo groups from Nuxeo, then applies them only to matching source IDs.
- This mode assumes the authenticated username is the same login string in each source you want to query.
- Nuxeo group expansion in `rag-service` uses the configured `NUXEO_USERNAME` and `NUXEO_PASSWORD` service credentials to read `/api/v1/user/{username}`.

### Quick Example

```bash
# Authenticate and start sync
curl -X POST http://localhost:9090/api/sync/configured \
  -u admin:admin

# Or use Alfresco ticket
TICKET=$(curl -X POST http://localhost:8080/alfresco/api/-default-/public/authentication/versions/1/tickets \
  -H "Content-Type: application/json" \
  -d '{"userId":"admin","password":"admin"}' | jq -r '.entry.id')

curl -X POST "http://localhost:9090/api/sync/configured?alf_ticket=$TICKET"
```

## API Usage

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
| `topK` | int | 5 | Number of chunks to retrieve for context |
| `minScore` | double | 0.5 | Minimum similarity threshold |
| `filter` | String | — | Additional HXQL filter |
| `sourceType` | String | — | Optional source filter: `alfresco` or `nuxeo` |
| `systemPrompt` | String | — | Override the default LLM system prompt |
| `includeContext` | boolean | false | Include retrieved chunks in response |

| Response Field | Type | Description |
|---------------|------|-------------|
| `sessionId` | String | Effective session id used by server |
| `retrievalQuery` | String | Query actually sent to retrieval (may be reformulated) |
| `historyTurnsUsed` | Integer | Number of prior turns included in this generation |
| `tokenCount` | Integer | Total token usage (prompt + completion) when provider reports it |
| `sources[].sourceType` | String | Source type for each cited document |
| `sources[].openInSourceUrl` | String | Native-source deep link (Share for Alfresco, Web UI for Nuxeo) |

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
| `filter` | String | — | Additional HXQL filter |
| `sourceType` | String | — | Optional source filter: `alfresco` or `nuxeo` |
| `embeddingType` | String | model default | Embedding type to match |
| `systemPrompt` | String | — | Override the default LLM system prompt |
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

* Default value: `0.5`
* Applied server-side after vector retrieval
* Can be overridden per request

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
| `vectorWeight` | double | `0.7` | Weight when `strategy=weighted` |
| `textWeight` | double | `0.3` | Weight when `strategy=weighted` |
| `filter` | String | — | Additional raw HXQL filter |
| `sourceType` | String | — | Optional source filter: `alfresco` or `nuxeo` |
| `metadata.mimeType` | String | — | MIME type filter (for example `application/pdf`) |
| `metadata.pathPrefix` | String | — | Path prefix filter (starts-with match) |
| `metadata.modifiedAfter` | String | — | Inclusive lower bound for `source_modifiedAt` |
| `metadata.modifiedBefore` | String | — | Inclusive upper bound for `source_modifiedAt` |
| `metadata.properties` | Map<String,String> | — | Exact-match filters on `cin_ingestProperties.<key>` |

| Response Field | Type | Description |
|---------------|------|-------------|
| `query` | String | Original query |
| `strategy` | String | Effective fusion strategy used |
| `normalization` | String | Normalization mode used when `strategy=weighted` |
| `model` | String | Embedding model used for vector search |
| `resultCount` | int | Number of fused results returned |
| `vectorCandidates` | int | Number of vector candidates retrieved |
| `keywordCandidates` | int | Number of keyword candidates retrieved |
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

### Health Checks

```bash
# Batch ingester (no auth required)
curl http://localhost:9090/actuator/health

# Live ingester (no auth required)
curl http://localhost:9092/actuator/health

# RAG service (no auth required)
curl http://localhost:9091/actuator/health

# RAG service detailed health (auth required)
curl http://localhost:9091/api/rag/health -u admin:admin
```

### Live Ingester (port 9092)

The live ingester consumes Alfresco Event2 messages from ActiveMQ using Alfresco Java SDK handler interfaces such as `OnNodeUpdatedEventHandler` and `OnPermissionUpdatedEventHandler`.

It reuses the same shared ingestion pipeline as the batch ingester:

- Fetch the current node snapshot from Alfresco REST API
- Apply scope and exclusion rules
- Sync metadata to hxpr
- Extract text with Transform Service
- Chunk and embed with Spring AI
- Update permissions or delete when nodes move out of scope

The live path is guarded by the same `alfresco_modifiedAt` staleness check used by batch ingestion, so batch and live runs can coexist safely.

Status endpoint:

```bash
curl http://localhost:9092/api/live/status
```

## Configuration

### Ingestion

Edit `batch-ingester/src/main/resources/application.yml`:

```yaml
ingestion:
  sources:
    - folder: your-folder-node-id
      recursive: true
      types: [cm:content]
  exclude:
    paths: ["*/surf-config/*", "*/thumbnails/*"]
    aspects: [cm:workingcopy]
```

### Live Ingestion

Edit `live-ingester/src/main/resources/application.yml`:

```yaml
spring:
  activemq:
    broker-url: ${ACTIVEMQ_URL:tcp://localhost:61616}
    user: ${ACTIVEMQ_USER:admin}
    password: ${ACTIVEMQ_PASSWORD:admin}
  jms:
    cache:
      enabled: false

alfresco:
  events:
    topic-name: ${ALFRESCO_EVENT_TOPIC:alfresco.repo.event2}
    enable-handlers: true
    enable-spring-integration: false

live-ingester:
  filter:
    exclude-paths: ["*/surf-config/*", "*/thumbnails/*"]
    exclude-aspects: [cm:workingcopy]
  scope:
    include-paths: []
    required-aspects: []
  dedup:
    window: ${LIVE_INGESTER_DEDUP_WINDOW:PT2M}
    max-entries: ${LIVE_INGESTER_DEDUP_MAX_ENTRIES:10000}
```

Notes:

- `spring.jms.cache.enabled=false` is required so the Alfresco Java SDK can use the native ActiveMQ connection factory.
- By default, the live ingester behaves as an exclude-only listener. Set `include-paths` or `required-aspects` to narrow the scope.
- Transform Service receives the original Alfresco filename when available, improving binary format detection during text extraction.

### RAG

Edit `rag-service/src/main/resources/application.yml`:

```yaml
spring:
  ai:
    openai:
      chat:
        options:
          model: ${LLM_MODEL:ai/gpt-oss}
          temperature: ${LLM_TEMPERATURE:0.3}
          maxTokens: ${LLM_MAX_TOKENS:1024}

rag:
  default-top-k: 5
  default-min-score: 0.5
  max-context-length: 12000
  default-system-prompt: >
    You are a document assistant that answers questions based strictly on
    the provided context.

    RULES:
    1. Use ONLY information from the DOCUMENT CONTEXT below. Do not use prior knowledge.
    2. When referencing information, cite the source using its label (e.g. "According to Source 1...").
    3. If multiple sources contain relevant information, synthesize them and cite each.
    4. If the context does not contain enough information to fully answer the question,
    clearly state what you can answer and what is missing.
    5. Be concise and direct. Do not repeat the question or add unnecessary preamble.
  conversation:
    enabled: true
    max-history-turns: 10
    session-ttl-minutes: 30
    query-reformulation: true

semantic-search:
  default-min-score: 0.5

search:
  hybrid:
    enabled: true
    strategy: rrf       # or weighted
    normalization: max  # max or minmax (weighted strategy)
    vector-weight: 0.7
    text-weight: 0.3
    initial-candidates: 20
    final-results: 5
    rrf-k: 60
    default-min-score: 0.0
```

Conversation memory storage:

- Default implementation is in-memory.
- To use Redis or a database, provide a custom Spring bean implementing `ConversationMemoryStore`; the default in-memory store is only created when no other `ConversationMemoryStore` bean exists.

## Roadmap

### Next (Q2 2026 - Open Source Release)

- [ ] Harden live-ingester with end-to-end Event2 coverage and operational guidance
- [ ] OAuth2/Keycloak integration
- [ ] Comprehensive testing suite
- [ ] Production deployment guide

### Future

- [ ] Conversation history / multi-turn chat sessions
- [ ] Re-ranking with cross-encoder models
- [ ] Multiple embedding models per document
- [ ] Document versioning support
- [ ] DocFilters integration (better text extraction)
- [ ] Multilingual embeddings
- [ ] Performance optimizations for 10K+ documents

## Development

### Build

```bash
mvn clean package
```

### Run Tests

```bash
mvn test
```

### Run Locally

```bash
# Batch Ingester
mvn spring-boot:run -pl batch-ingester
# or
java -jar batch-ingester/target/batch-ingester-1.0.0-SNAPSHOT.jar

# Live Ingester
mvn spring-boot:run -pl live-ingester
# or
java -jar live-ingester/target/live-ingester-1.0.0-SNAPSHOT.jar

# RAG Service
mvn spring-boot:run -pl rag-service
# or
java -jar rag-service/target/rag-service-1.0.0-SNAPSHOT.jar
```

## Contributing

Contributions welcome! Please:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit changes (`git commit -m 'feat: add amazing feature'`)
4. Push to branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## Acknowledgments

- Built with [Spring AI](https://spring.io/projects/spring-ai)
- Uses [Alfresco Java SDK](https://github.com/Alfresco/alfresco-java-sdk)
- Powered by [hxpr Content Lake](https://www.hyland.com/)
- Created for the Alfresco/Hyland community
