# Architecture

## Project Overview

`content-lake-app` is a Java/Spring Boot pipeline that ingests documents from content repositories
(Alfresco, Nuxeo) into **hxpr** -- a Hyland platform that stores documents, embeddings, and ACLs
for hybrid semantic search and RAG.

The pipeline is designed around a **Source Provider Interface (SPI)**: a set of contracts each
content source implements independently, while the shared pipeline (chunking, embedding, hxpr
storage, RAG) remains source-agnostic.

---

## Module Layout

```
content-lake-app/
├── common/
│   ├── content-lake-repo-model/   Alfresco content model XML (cl:indexed, cl:excludeFromLake)
│   │                              Deployed to Alfresco only. Do not modify.
│   ├── content-lake-spi/          Source-agnostic interfaces only (zero external deps)
│   │   └── org.hyland.contentlake.spi
│   │       ├── ContentSourceClient
│   │       ├── ScopeResolver
│   │       ├── SourceNode
│   │       ├── SecurityConfig / PermissionRule   (OIS-aligned structured ACL)
│   │       ├── TextExtractor
│   │       └── ExtractedText / TextFormat        (PLAIN | MARKDOWN)
│   ├── content-lake-core/         Shared pipeline -- no source-specific SDK imports
│   │   └── org.hyland.contentlake
│   │       ├── client/            HxprService, HxprDocumentApi, HxprQueryApi
│   │       ├── config/            HxprProperties
│   │       ├── extractor/         TikaTextExtractor (source-agnostic, Tika-based)
│   │       │                       TransformEngineTextExtractor (/transform protocol, any source)
│   │       │                       ChainingTextExtractor (ordered fallback, degrades to Tika)
│   │       │                       ExtractionChain (multi-service chain from config)
│   │       │                       ExtractionBackend / TransformCoreBackend (pluggable protocols)
│   │       │                       MarkdownToPlainText, ExtractionFormat
│   │       ├── model/             HxprDocument, HxprEmbedding, Chunk, ContentLakeNodeStatus
│   │       └── service/           ContentSyncService, EmbeddingService, Chunker, chunking strategies
│   └── rag-service/               Semantic search + RAG Spring Boot app
│       └── org.hyland.contentlake.rag
│
├── alfresco/
│   ├── content-lake-source-alfresco/  Alfresco adapter
│   │   └── org.hyland.alfresco.contentlake
│   │       ├── client/            AlfrescoClient (impl ContentSourceClient), TransformClient
│   │       ├── config/            TransformProperties
│   │       ├── security/          SecurityConfig, AlfrescoAuthenticationProvider, *TicketAuth*
│   │       ├── adapter/           AlfrescoSourceNodeAdapter
│   │       └── service/           ContentLakeScopeResolver (impl ScopeResolver)
│   ├── alfresco-batch-ingester/   Spring Boot app: full-batch Alfresco sync
│   │   └── org.hyland.alfresco.contentlake.batch
│   └── alfresco-live-ingester/    Spring Boot app: Alfresco ActiveMQ event listener
│       └── org.hyland.alfresco.contentlake.live
│
└── nuxeo/
    ├── content-lake-source-nuxeo/  Nuxeo adapter
    │   └── org.hyland.nuxeo.contentlake
    │       ├── client/            NuxeoClient (impl ContentSourceClient), NuxeoConversionClient
    │       ├── auth/              BasicNuxeoAuthentication, NuxeoAuthentication
    │       ├── config/            NuxeoProperties
    │       ├── adapter/           NuxeoSourceNodeAdapter
    │       ├── model/             NuxeoDocument
    │       └── service/           NuxeoScopeResolver (impl ScopeResolver)
    ├── nuxeo-batch-ingester/       Spring Boot app: full-batch Nuxeo sync via NXQL
    │   └── org.hyland.nuxeo.contentlake.batch
    └── nuxeo-live-ingester/        Spring Boot app: audit-driven Nuxeo sync
        └── org.hyland.nuxeo.contentlake.live

filesystem/
    ├── content-lake-source-filesystem/  Filesystem adapter
    │   └── org.hyland.filesystem.contentlake
    │       ├── client/            FileSystemSourceClient (impl ContentSourceClient)
    │       ├── config/            FileSystemProperties
    │       └── service/           FileSystemScopeResolver (impl ScopeResolver)
    └── filesystem-batch-ingester/  Spring Boot app: directory walk + one-shot sync (uses TikaTextExtractor)
        └── org.hyland.filesystem.contentlake.batch
```

Sibling runtime projects:

```
nuxeo-deployment/       Runnable local Nuxeo + PostgreSQL stack
alfresco-content-lake-ui/
└── ext-rag/            ADF extension source (Angular)
alfresco-content-app/
└── projects/ext-rag/   Real ACA workspace for build/test validation
```

---

## Dependency Graph

```
                 ┌─────────────────────┐
                 │  content-lake-spi   │  interfaces only, zero external deps
                 └──────────┬──────────┘
                            │
            ┌───────────────┼───────────────┐
            │               │               │
 ┌──────────▼────────┐  ┌───▼────┐  ┌───────▼──────────┐
 │ source-alfresco   │  │  core  │  │  source-nuxeo    │
 │ (Alf adapter)     │  │        │  │  (Nuxeo adapter) │
 └────────┬──────────┘  └───┬────┘  └──────┬───────────┘
          │                 │              │
   ┌──────▼──────────┐      │       ┌──────▼──────────┐
   │ alf-batch-ing.  │◄─────┘  ────►│ nuxeo-batch-ing │
   │ alf-live-ing.   │              └─────────────────┘
   └─────────────────┘
                │                    │
                └──────┬─────────────┘
                       │
                ┌──────▼──────┐
                │ rag-service  │  depends on core only
                └─────────────┘
```

---

## SPI Interfaces

Four interfaces in `content-lake-spi` (`org.hyland.contentlake.spi`), carrying zero Alfresco/Nuxeo
imports. Every content source adapter must implement them.

### `SourceNode` -- universal document representation

```java
public record SourceNode(
    String nodeId,              // unique ID within the source system
    String sourceId,            // identifies the source system instance
    String sourceType,          // "alfresco", "nuxeo", ...
    String name,
    String path,
    String mimeType,
    OffsetDateTime modifiedAt,
    boolean folder,
    Set<String> readPrincipals,
    Set<String> denyPrincipals,            // identities explicitly denied read access by the source ACL
    Map<String, Object> sourceProperties  // merged into cin_ingestProperties
) {}
```

### `ContentSourceClient`

```java
public interface ContentSourceClient {
    String getSourceId();
    String getSourceType();   // e.g. "alfresco", "nuxeo"
    SourceNode getNode(String nodeId);
    List<SourceNode> getChildren(String containerId, int skip, int maxItems);
    Resource downloadContent(String nodeId, String fileName);
    byte[] getContent(String nodeId);
    default void writeSyncStatus(String nodeId, String status, String error) {}  // optional sync-status write-back
    default void clearSyncStatus(String nodeId) {}                                // optional sync-status clear
}
```

### `TextExtractor`

```java
public interface TextExtractor {
    boolean supports(String mimeType);
    String extractText(Resource content, String mimeType);
    default boolean supportsSourceReference(String mimeType) { return false; }  // can extract straight from a node ref
    default String extractText(String nodeId, String mimeType) { ... }          // source-reference extraction

    default TextFormat preferredFormat(String mimeType) { return PLAIN; }       // PLAIN | MARKDOWN
    default ExtractedText extract(Resource content, String mimeType) { ... }    // text + its representation
    default ExtractedText extract(String nodeId, String mimeType) { ... }
}
```

`ExtractedText(text, format)` lets an extractor that can produce structure say so. It matters because
table detection recognises a table only from a markdown separator row or rows carrying at least two
`|` characters, so flattening a document to plaintext removes the signal chunking needs. The defaults
wrap `extractText` as `PLAIN`, so an extractor that only produces flattened text implements nothing
extra. A `null` return still means "no text", not an error.

Note for tests: Mockito does not execute interface default methods, so a mocked `TextExtractor` must
stub `extract(...)` rather than `extractText(...)`.

### `ScopeResolver`

```java
public interface ScopeResolver {
    boolean isInScope(SourceNode node);
    boolean shouldTraverse(SourceNode node);
}
```

---

## Core Data Model

### `HxprDocument` -- the unit stored in hxpr

| Java field | JSON key | Purpose |
|---|---|---|
| `sysId` | `sys_id` | hxpr document identifier |
| `sysPrimaryType` | `sys_primaryType` | `"SysFile"` or `"SysFolder"` |
| `sysName` | `sys_name` | display name |
| `sysMixinTypes` | `sys_mixinTypes` | must include `"CinRemote"` for ingested docs |
| `sysFulltextBinary` | `sys_fulltextBinary` | extracted plain text |
| `sysAcl` | `sys_acl` | list of `ACE` grants |
| `cinId` | `cin_id` | source node ID |
| `cinSourceId` | `cin_sourceId` | `"<sourceType>:<sourceId>"` (e.g. `"alfresco:abc-uuid"`) |
| `cinPaths` | `cin_paths` | hxpr path list |
| `cinIngestProperties` | `cin_ingestProperties` | arbitrary metadata map |
| `cinIngestPropertyNames` | `cin_ingestPropertyNames` | must mirror `cinIngestProperties.keySet()` |
| `sysembedEmbeddings` | `sysembed_embeddings` | inline embedding array |

`@JsonIgnore` fields carry runtime state inside the pipeline but are **not** sent to hxpr.

### `cin_ingestProperties` keys

**Generic keys** (in `content-lake-core`, `ContentLakeIngestProperties`):

| Constant | Key string | Meaning |
|---|---|---|
| `SOURCE_NODE_ID` | `source_nodeId` | node ID within the source system |
| `SOURCE_SYSTEM_ID` | `source_systemId` | source system instance identifier |
| `SOURCE_TYPE` | `source_type` | `"alfresco"`, `"nuxeo"`, ... |
| `SOURCE_PATH` | `source_path` | node path |
| `SOURCE_NAME` | `source_name` | node name |
| `SOURCE_MIME_TYPE` | `source_mimeType` | MIME type |
| `SOURCE_MODIFIED_AT` | `source_modifiedAt` | ISO-8601 timestamp |
| `CONTENT_LAKE_SYNC_STATUS` | `contentLake_syncStatus` | `PENDING`, `INDEXED`, `FAILED` |
| `CONTENT_LAKE_SYNC_ERROR` | `contentLake_syncError` | error message when FAILED |

Source adapters add extra properties via `SourceNode.sourceProperties()` using their own namespace
(e.g. `alfresco_repositoryId`, `nuxeo_documentType`).

### `cin_sourceId` format

`"<sourceType>:<sourceId>"` -- e.g. `"alfresco:abc-123-def"`, `"nuxeo:prod-instance"`.

`HxprService.findByNodeId(nodeId, sourceId)` filters by both `cin_id` and `cin_sourceId`.

### ACL model

`sys_acl` is a list of `ACE` objects. Each ACE has `granted`, `permission` (`"Read"`), and either a
`User` or a `Group`. User/group names are namespaced per source instance by appending
`_#_<repositoryId>`. `GROUP_EVERYONE` maps to the special principal `__Everyone__`.

Both sides of that encoding come from `AclFilterBuilder` in
`common/content-lake-core/.../security/`: `NodeSyncService` writes the ACEs through it at ingest, and
the read path builds its `sys_racl` predicate from it at query time. Read access to every document
depends on those two agreeing, so they share one implementation rather than two matching ones.

[security-model.md](security-model.md) covers the trust boundary this sits inside: which component
enforces read access, what the model does not do, and what a deployment has to get right.

| Element | Value | Note |
|---|---|---|
| ACL field queried at read time | `sys_racl` | the engine's expansion of `sys_acl` |
| User principal | `u:<username>_#_<sourceId>` | |
| Group principal | `g:<groupName>_#_<sourceId>` | |
| Everyone | `__Everyone__` | un-namespaced: readable from any source |
| No resolvable source | `cin_sourceId = '__unresolved_permission_source__'` | matches nothing, so the absence of a decision is not the absence of a filter |
| Whole-source read | `cin_sourceId = '<sourceType>:<sourceId>'` | no `sys_racl` condition at all; only for `GROUP_ALFRESCO_ADMINISTRATORS`, only on an Alfresco source, and only when `rag.security.admin-bypass.enabled` is set |

The `_#_<sourceId>` suffix is what keeps `g:sales_#_alfresco` from matching `g:sales_#_nuxeo`. Two
repositories can each have a `sales` group with different members, so stripping the suffix would merge
two unrelated populations and hand each the other's documents.

HXQL literals are escaped with a backslash (`\'`), and a backslash is doubled before quotes are
escaped. SQL-style quote doubling (`''`) is rejected by the engine with HTTP 400.

Two inputs feed that predicate, and each fails closed independently:

| Input | Source | On failure |
|---|---|---|
| Caller identity | `SecurityContextService.getCurrentUsername()` | throws `AuthenticationCredentialsNotFoundException`, which Spring Security translates into a 401 |
| Group membership per source | Alfresco `GET /people/{user}/groups`, Nuxeo `GET /api/v1/user/{username}` | governed by `rag.security.group-resolution-failure` |

`rag.security.group-resolution-failure` takes `fail-closed` (the default) or `degrade`. Under
`fail-closed` a source whose directory cannot be reached is dropped from the predicate entirely, so
the caller sees nothing from it; under `degrade` the caller keeps their own name plus
`GROUP_EVERYONE` and silently loses only group-granted documents. Both modes log at WARN, and an
unrecognised value reads as `fail-closed`. When every source drops out, `AclFilterBuilder.query`
emits the `__unresolved_permission_source__` sentinel rather than no clause at all.

### `Chunk` -- unit of embedding

Text is split into `Chunk` records before embedding. Beyond the text and offsets, each chunk carries
structural metadata used by retrieval:

| Field | Purpose |
|---|---|
| `index` / `startOffset` / `endOffset` | position of the chunk within the extracted text |
| `chunkingStrategy` | name of the strategy that produced it |
| `chunkType` | `PROSE` or `TABLE` (see below) |
| `sectionIndex` | parent-section id, enabling small-to-big (parent-section) retrieval at query time |

`ChunkType.TABLE` marks detected tables (markdown/pipe-delimited) so the noise-reduction and chunking
stages keep them as an atomic unit rather than hard-splitting them mid-row or discarding them as
prose noise.

---

## Design Decisions

- **Separate deployables per source** -- no multi-source monolith JAR; simpler ops + independent scaling
- **Separate hxpr root paths per source** -- `/alfresco/...` vs `/nuxeo/...`; rag-service queries across both
- **`cin_sourceId` format** -- `"<sourceType>:<sourceId>"` enables per-source and per-instance filtering
- **`nuxeo.sourceId` config** -- must be set explicitly; do NOT use Nuxeo's built-in `repository` field (always `"default"`)
- **Nuxeo text extraction** -- use Nuxeo `ConversionService` via REST `@convert` for non-text blobs; do not use embedded Tika or deprecated `TransformService`
- **Nuxeo scope** -- config-only for MVP (`nuxeo.scope.includedRoots` + `includedTypes`); schema-based facet is a follow-up
- **Nuxeo auth** -- Basic auth for MVP, wrapped in an abstraction for future token/OAuth2
- **Nuxeo discovery** -- NXQL query preferred over `@children` tree walk for scalability
- **Every runnable module defines its own filter chain, and every chain denies by default** -- the
  only public paths are `/actuator/health` and `/actuator/info`, so a container orchestrator can probe
  a service without credentials while an endpoint added later is authenticated without anyone
  configuring it. `rag-service` additionally exempts `/api/rag/health`. Each chain carries a negative
  test asserting that an unmapped path and `/actuator/metrics` both return 401: the guarantee is that
  forgetting to secure a route is not a way to publish it
- **`rag-service` is the policy enforcement point for read access, not the index** -- the engine
  applies no ACL filter of its own for our connection, because the service account it authenticates is
  an administrator, so the predicate `AclFilterBuilder` produces is the only thing standing between a
  caller and another caller's documents. It therefore lives in exactly one class, which both search
  services and the ingest write path call: one place to audit, one place to test. A per-request
  identity on the engine side would need a delegation primitive the engine does not have
- **An unresolved authorization input is never encoded as a missing filter** -- there is no anonymous
  or placeholder principal: a request with no authenticated caller is rejected rather than answered
  under a synthetic name, and a source whose group directory is unreachable is excluded from the
  predicate instead of falling back to a permissive default. The failure mode is a caller seeing too
  few documents and a WARN in the log, never too many. `rag.security.group-resolution-failure=degrade`
  exists for deployments that would rather lose group-granted results than lose a whole source, and it
  is opt-in for that reason
- **A group membership that reads a whole source is opt-in** -- `GROUP_ALFRESCO_ADMINISTRATORS`
  replaces the `sys_racl` predicate with a bare `cin_sourceId` match, which is the widest grant the
  filter can express, so `rag.security.admin-bypass.enabled` defaults to `false` and an Alfresco
  administrator reads through document ACLs like anyone else. Deployments that want repository-admin
  discoverability turn it on. The bypass is also scoped at the call site rather than inside the
  predicate builder: `AclFilterBuilder` grants it only when the caller passes both the opt-in and the
  fact that the source is an Alfresco source, so it cannot reach a source that does not recognise the
  group
- **Feedback documents are authorized by the endpoint, not by an ACL** -- a feedback entry records a
  user's question and the answer they were given, and it belongs to no content source, so there is no
  `_#_<sourceId>` suffix that could namespace its principal. It is written with no read ACE, which
  keeps it out of the ACL-filtered search path entirely, and `FeedbackService` puts a predicate on the
  stored submitter into every listing query. The aggregate view the evaluation harness needs is a
  separate method restricted to `rag.feedback.operator-users`, empty by default, because "authenticated"
  and "may read everyone's questions" are different things
- **`filesystem-batch-ingester` authenticates against one configured account** -- the other ingesters
  validate callers against their source repository, but a filesystem has no user directory. Both
  `filesystem.batch.security.username` and `.password` are required and startup fails when either is
  blank, because the alternative is a default credential on an endpoint that triggers a full re-ingest
- **Query expansion in the search services, not the RAG advisor** -- multi-query, HyDE and decomposition
  all run inside `SemanticSearchService`/`HybridSearchService`, which both the search controllers and
  `HxprDocumentRetriever` go through. Placing them in `ContentLakeRetrievalAdvisor` instead would hide
  them from the `/search/*` endpoints the retrieval-quality gate measures
- **Retrieval-quality stages are opt-in and NoOp by default** -- rerank (`RerankService`), diversity
  (`DiversitySelector`), expansion (`QueryExpansionService`) and the pre-generation relevance gate
  (`RetrievalGrader`) each register one bean chosen by a single `@Configuration`, defaulting to the
  behaviour that predates them, so a build with every flag off retrieves exactly as before
- **hxpr AdvancedQuery integration** -- structured metadata filters, named queries, facet aggregation
  (`FacetsService`), vocabulary lookup and chunk full-text search are expressed through hxpr's
  AdvancedQuery API rather than hand-built query strings, which is what powers the metadata filters on
  hybrid search and the `/search/facets` endpoint
