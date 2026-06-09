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
│   │       └── TextExtractor
│   ├── content-lake-core/         Shared pipeline -- no source-specific SDK imports
│   │   └── org.hyland.contentlake
│   │       ├── client/            HxprService, HxprDocumentApi, HxprQueryApi, HxprTokenProvider
│   │       ├── config/            HxprProperties
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
}
```

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
- **`rag-service` security** -- needs its own `SecurityConfig`; options are permit-all behind network policy or OAuth2/OIDC via hxpr IDP
