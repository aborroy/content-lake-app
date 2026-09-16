# Configuration

### Connector Plugins

A source connector can be a jar rather than a module of this build. Every ingester scans
`/opt/content-lake/connectors` at startup, so a connector needs no Maven module, no entry in an intermediate
POM and no COPY line in the deployment repository's service Dockerfiles.

Connectors that ship with the project live under `plugins/`, which the reactor never builds:
`plugins/archetype/` generates a skeleton, `plugins/cmis-connector/` is a shipped connector, and
`plugins/examples/sample-directory-connector/` is a worked example. Do not confuse `plugins/` with the
reactor group `connector/`, which is the host application that loads them.

```bash
# Generate the skeleton (see plugins/archetype/README.md for the properties)
mvn archetype:generate -DarchetypeGroupId=org.hyland \
  -DarchetypeArtifactId=content-lake-connector-archetype -DarchetypeVersion=1.0.0-SNAPSHOT \
  -DgroupId=com.example -DartifactId=my-cmis-connector -Dpackage=com.example.cmis \
  -Dversion=1.0.0 -DsourceType=cmis -DinteractiveMode=false

cd my-cmis-connector && mvn package
cp target/my-cmis-connector-1.0.0.jar ../content-lake-app-deployment/connectors/
```

A connector implements `ConnectorPlugin` from `content-lake-spi` and declares itself in
`META-INF/services/org.hyland.contentlake.spi.ConnectorPlugin`. The host validates its schema, hands it a
`ConnectorContext` to read settings from, and then asks it for a client, a scope resolver and, optionally,
its own text extractor.

```bash
curl http://localhost:9090/api/connectors -u admin:admin
```

```json
{ "connectors": [
    { "sourceType": "alfresco", "origin": "in-tree", "implementation": "...AlfrescoClient", "settings": 6 },
    { "sourceType": "cmis", "origin": "my-cmis-connector-1.0.0.jar",
      "implementation": "com.example.cmis.SourceConnectorClient", "settings": 4 } ],
  "problems": [] }
```

Failure is per connector and never fatal: a jar that cannot be read, a service entry naming a class that is
not there, a plugin whose constructor throws, or one claiming a source type another connector already has is
reported in `problems` and skipped. An ingester with one broken plugin and three working ones has three
working connectors. The exception is configuration: a plugin whose settings do not satisfy its own schema is
refused, and in the default `fail` mode that aborts startup rather than leaving a connector that cannot work
looking like it is running.

Plugin connectors are kept in a registry rather than registered as beans, deliberately: publishing a
plugin's `TextExtractor` or `ScopeResolver` as a bean would make injection by type ambiguous in an ingester
that already has one, so mounting a jar would break the ingester it was mounted next to.

A connector's settings are read from the ingester's environment, so a schema field is settable both as
declared and as an environment variable name: dots and hyphens become underscores and the name is
upper-cased, which makes `cmis.page-size` reachable as `CMIS_PAGE_SIZE`.

### Ingesting Through A Plugin Connector

Every ingester loads a connector; `connector-batch-ingester` is the one that ingests with it. The Alfresco,
Nuxeo and filesystem ingesters each drive a client they were compiled against, so for them a mounted jar is
listed and otherwise inert.

```bash
# From content-lake-app-deployment, on top of any base profile
CONNECTOR_SYNC_USERNAME=admin CONNECTOR_SYNC_PASSWORD=admin \
  docker compose --profile alfresco --profile connector up -d --build connector-batch-ingester

curl -u admin:admin -X POST http://localhost:9096/api/sync/configured
curl -u admin:admin http://localhost:9096/api/status
```

It resolves its client, its scope rules and optionally its extractor from `ConnectorRegistry`, filling in a
permissive default scope and the host extraction chain when the connector supplies neither. Configuration is
under `connector.*`:

| Setting | Meaning |
|---|---|
| `connector.source-type` | Which loaded connector to ingest with. Optional with one jar mounted, required with several |
| `connector.roots` | Containers to walk. Empty asks the connector, through `ContentSourceClient.getRootNodeId()` |
| `connector.page-size` | Children fetched per listing |
| `connector.max-depth` | Depth backstop for a hierarchy that does not bottom out |
| `connector.security.*` | Credentials for this ingester's own sync API. No defaults; startup fails without both |
| `connector.reconcile.*` | Post-discovery deletion sweep. Off by default |
| `connector.change-feed.*` | Read the connector's change feed instead of walking it. Off by default |
| `connector.cursor.*` | Where the change feed's position is kept between passes |

Two things about it are deliberate. With no connector loaded it fails to start, because its only source is
that jar and a sync API reporting zero documents hides the misconfiguration. And the walk visits each node id
once, because a source with multi-filing (CMIS, for one) reaches a document through several parents and would
otherwise ingest it repeatedly.

`plugins/examples/sample-directory-connector` is a working connector that ingests a mounted
directory, and `content-lake-app-deployment/test/test-connector.sh` builds it, mounts it and asserts the
documents come back out of semantic search.

### Incremental Passes Through A Connector's Change Feed

A connector that overrides `supportsChangeFeed()` can report what changed since a token instead of being
walked in full. Both sides have to agree: the connector implements the feed, and the deployment turns it on.

| Setting | Default | Meaning |
|---|---|---|
| `connector.change-feed.enabled` | `false` | Opt-in, for the same reason the sweep is: an incremental pass suspends the sweep, so a feed that under-reports deletions leaves them in the index |
| `connector.change-feed.page-size` | `200` | Changes requested per `changesSince` call |
| `connector.change-feed.max-pages` | `100` | Pages read in one pass. Reaching it is not a failure: the cursor reached is saved and the next pass continues from it |
| `connector.change-feed.full-walk-every` | `0` | `0` never forces a walk; `N` forces a walk plus sweep every Nth pass. `24` suits an hourly incremental schedule |
| `connector.cursor.store` | `hxpr` | `hxpr`, `file` or `memory` |
| `connector.cursor.hxpr-path` | `/content-lake/_state/cursors` | Where the `hxpr` store keeps its state document |
| `connector.cursor.file` | `/data/connector-cursor.json` | Path the `file` store writes, which needs a writable mount |

The default is `hxpr` because this container's mounts are read-only, so a file store would have nowhere to
write. Its state document carries no `cin_sourceId` and no `cin_paths`, which is what keeps the sweep from
proposing the deletion of the cursor that tells it where it is, and it has no embeddings, which is what keeps
it out of search results. `memory` is the honest answer for a deployment with neither a mount nor hxpr write
access: every pass is a full walk, which is the behaviour with the feature off.

How one pass decides which mechanism it runs: the feed, if the feature is on, the connector declares one, a
cursor is stored, and `full-walk-every` has not been reached. Otherwise a full walk. The first pass therefore
always walks, because a feed opened at the source's current position would never mention the content that
already exists; the host reads that position before the walk and saves it after, so a change made while the
walk ran is replayed by the next pass rather than lost. A cursor the source has expired is discarded and the
same job falls back to a walk.

The two mechanisms never both delete in one pass. A walk hands its enumeration to the sweep; an incremental
pass applies only the tombstones the feed reported and its `reconciliation` block says
`SKIPPED_INCREMENTAL_RUN`. Deletions from either path are counted in the job's `deletedCount`.

### The CMIS Connector

`plugins/cmis-connector/` is a shipped connector rather than an example: one jar that ingests any CMIS 1.1
repository, which is how a repository with no adapter of its own becomes a source. It is built standalone,
like any connector, and its OpenCMIS dependency travels inside the jar.

```bash
mvn -pl common/content-lake-spi -am install -DskipTests
mvn -f plugins/cmis-connector/pom.xml package
cp plugins/cmis-connector/target/cmis-connector-1.0.0.jar \
   ../content-lake-app-deployment/connectors/
```

```bash
# Against the Alfresco in this stack, over its own CMIS endpoint
CMIS_URL=http://alfresco:8080/alfresco/api/-default-/public/cmis/versions/1.1/browser \
CMIS_USERNAME=admin CMIS_PASSWORD=admin CMIS_ROOT_PATH=/Sites \
CONNECTOR_SYNC_USERNAME=admin CONNECTOR_SYNC_PASSWORD=admin \
  docker compose --profile alfresco --profile connector up -d connector-batch-ingester
```

| Setting | Meaning |
|---|---|
| `cmis.url` | Service endpoint. Required |
| `cmis.binding` | `browser` (default) or `atompub` |
| `cmis.repository-id` | Optional; resolved automatically when the endpoint exposes exactly one |
| `cmis.username` / `cmis.password` | Account to authenticate as. Required, and never printed |
| `cmis.root-path` | Folder a batch pass starts from. Defaults to the repository root |
| `cmis.include-paths` / `cmis.exclude-paths` | Path scope. Excludes are applied after includes and win |
| `cmis.include-mime-types` / `cmis.exclude-mime-types` | MIME scope, `text/*` wildcards allowed |
| `cmis.acl-fallback` | `fail-closed` (default), `sync-account` or `public`; see below |

Three limits, stated because they are properties of CMIS rather than of this implementation:

- **Batch only.** CMIS exposes no change feed this connector uses, so there is no live counterpart. A
  re-ingest is another batch pass, and unchanged content is skipped by the host's content-reuse check.
- **No aspect-based scope.** There is no CMIS equivalent of `cl:indexed`, so scope is the path and MIME
  patterns above rather than a decision an editor makes in the repository.
- **ACL support is repository-dependent.** CMIS makes ACL access an optional capability. Where the
  repository reports one, permissions are read per document, mapped through the basic CMIS permissions and
  stored as read principals. Where it reports none, the connector refuses to run rather than guessing:
  `cmis.acl-fallback` is what an operator sets to proceed deliberately, either restricting every document to
  the sync account or, for an already-public corpus, to everyone. A repository whose permissions cannot be
  read must not silently produce world-readable documents.

`content-lake-app-deployment/test/test-cmis.sh` is the end-to-end check: it ingests the same Alfresco folder
twice, once through the native adapter and once over CMIS, compares the document sets, and asserts that a
document restricted in Alfresco is not retrievable by a user the ACL excludes.

### Connector Schema And Startup Validation

Each source connector publishes the settings it needs, and every ingester checks its configuration
against that schema before it starts serving. A missing or malformed setting is reported by name instead
of surfacing later as a downstream symptom, such as a filesystem ingester that finds no documents because
its root path was never mounted.

```bash
curl http://localhost:9095/api/connectors/schema -u sync-user:sync-secret
```

```json
[ { "sourceType": "filesystem",
    "fields": [ { "name": "filesystem.root-path", "type": "DIRECTORY",
                  "description": "Absolute directory to ingest from, a local path or a mounted volume",
                  "required": true, "secret": false, "allowedValues": [] } ] } ]
```

The response carries field descriptors and never values, so it cannot disclose a credential; `secret`
tells tooling to mask its own input, and keeps the value out of validation messages and logs. The
endpoint is authenticated like every other API path.

Validation covers the connector's own connection and scope settings. Shared pipeline configuration (hxpr,
embedding model, chunking, extraction engines) and per-ingester scheduling are not part of a connector
schema.

| Setting | Values | Effect |
|---|---|---|
| `content-lake.connector.validation` | `fail` (default) | Startup aborts, listing every setting at fault |
| | `warn` | Problems are logged and the service starts, for a deployment whose mount or endpoint appears late |
| | `off` | No check |

### Ingestion

Edit `alfresco/alfresco-batch-ingester/src/main/resources/application.yml`:

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

Edit `alfresco/alfresco-live-ingester/src/main/resources/application.yml`:

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

Edit `common/rag-service/src/main/resources/application.yml`:

```yaml
spring:
  ai:
    openai:
      chat:
        options:
          model: ${LLM_MODEL:ai/gpt-oss}
          temperature: ${LLM_TEMPERATURE:0.3}
          maxTokens: ${LLM_MAX_TOKENS:2048}

rag:
  default-top-k: ${RAG_DEFAULT_TOP_K:15}
  default-min-score: ${RAG_DEFAULT_MIN_SCORE:0.01}
  max-context-length: ${RAG_MAX_CONTEXT_LENGTH:20000}
  use-hybrid-search: ${RAG_USE_HYBRID_SEARCH:true}
  default-system-prompt: >
    You are a document assistant that answers questions based strictly on
    the provided context. (See application.yml for the full prompt text.)
  conversation:
    enabled: ${RAG_CONVERSATION_ENABLED:true}
    max-history-turns: ${RAG_CONVERSATION_MAX_HISTORY_TURNS:10}
    session-ttl-minutes: ${RAG_CONVERSATION_SESSION_TTL_MINUTES:30}
    query-reformulation: ${RAG_CONVERSATION_QUERY_REFORMULATION:true}
    # Persistent running summary stored in hxpr. Off until the sessions folder is provisioned.
    summary:
      enabled: ${RAG_CONVERSATION_SUMMARY_ENABLED:false}
      base-path: ${RAG_CONVERSATION_SUMMARY_BASE_PATH:/_sessions}

semantic-search:
  default-min-score: ${SEMANTIC_SEARCH_MIN_SCORE:0.2}

search:
  hybrid:
    enabled: ${SEARCH_HYBRID_ENABLED:true}
    strategy: ${SEARCH_HYBRID_STRATEGY:rrf}            # rrf or weighted
    normalization: ${SEARCH_HYBRID_NORMALIZATION:max}  # max or minmax (weighted strategy)
    vector-weight: ${SEARCH_HYBRID_VECTOR_WEIGHT:0.7}
    text-weight: ${SEARCH_HYBRID_TEXT_WEIGHT:0.3}
    initial-candidates: ${SEARCH_HYBRID_INITIAL_CANDIDATES:75}
    final-results: ${SEARCH_HYBRID_FINAL_RESULTS:20}
    rrf-k: ${SEARCH_HYBRID_RRF_K:60}
    default-min-score: ${SEARCH_HYBRID_MIN_SCORE:0.01}
```

#### Result Shaping On The Search Endpoints

How much of a result set one document may occupy, applied to `/api/rag/search/semantic` and
`/api/rag/search/hybrid` and **not** to the RAG generation path, which asks for chunks and measurably loses
answer quality when they are capped:

```yaml
rag:
  retrieval:
    document-diversity:
      enabled: ${RAG_DOCUMENT_DIVERSITY_ENABLED:true}
      max-chunks-per-document: ${RAG_DOCUMENT_DIVERSITY_MAX_CHUNKS_PER_DOCUMENT:2}
      over-fetch-factor: ${RAG_DOCUMENT_DIVERSITY_OVER_FETCH_FACTOR:3}
```

| Setting | Effect |
|---|---|
| `enabled` | Whether the per-document cap applies at all. On by default: without it a single long document can fill a whole `topK`, which a caller cannot distinguish from the other documents not being indexed |
| `max-chunks-per-document` | Chunks one document may contribute before others are preferred. Chunks over the cap are deferred behind other documents' best ones, not dropped, so this changes which chunks arrive and never how many. It is also the fallback for a `topDocuments` request that sends no `chunksPerDocument`, and the request field is its per-request override |
| `over-fetch-factor` | How far past `topK` to retrieve, as a multiple, so the cap has other documents' chunks available to promote. Bounded by the endpoint's own maximum |

#### Query-Side Security

Everything that shapes the per-request permission predicate lives under `rag.security.*`:

```yaml
rag:
  security:
    group-resolution-failure: ${RAG_SECURITY_GROUP_RESOLUTION_FAILURE:fail-closed}  # or degrade
    admin-bypass:
      enabled: ${RAG_SECURITY_ADMIN_BYPASS_ENABLED:false}
    group-cache:
      ttl-seconds: ${RAG_SECURITY_GROUP_CACHE_TTL_SECONDS:300}   # 0 disables the cache
      max-size: ${RAG_SECURITY_GROUP_CACHE_MAX_SIZE:10000}
```

| Setting | Effect |
|---|---|
| `group-resolution-failure` | What a query does when a group directory cannot be reached at all. `fail-closed` drops that source from the predicate; `degrade` keeps the caller's own name plus `GROUP_EVERYONE` and loses only group-granted documents. Both log at WARN; an unrecognised value reads as `fail-closed` |
| `admin-bypass.enabled` | Whether `GROUP_ALFRESCO_ADMINISTRATORS` reads an Alfresco source with no `sys_racl` condition. Never applies to a Nuxeo source |
| `group-cache.ttl-seconds` | How long a resolved membership is reused, and therefore the ceiling on how stale it may be: a caller removed from a group keeps reading that group's documents until the entry expires |
| `group-cache.max-size` | Entry bound on that cache. Entries are keyed by source type and username, so the working set is roughly one per active caller per source type |

Group expansion itself is per source type, not global: `rag-service` holds one `SourceGroupResolver`
bean per type and ships `alfresco` and `nuxeo`. A source of any other type, which today means the
filesystem source or any plugin connector, contributes a clause built from the caller's own authorities
only, so its group-granted documents are retrievable by nobody. That is logged once per source at WARN
and is settings-independent: there is no flag that turns it on, only a resolver bean for that type.
Cache hit-rate is exposed as `cache.gets{cache=rag.security.groups}` under `/actuator/metrics`.

#### Optional Retrieval and Generation Features

These stages are **off by default**: with every flag unset, retrieval and generation behave as the
baseline pipeline. Enable them individually to trade latency or extra LLM calls for quality. They are
configured under `rag.*` in `common/rag-service/src/main/resources/application.yml`.

```yaml
rag:
  # Cross-encoder / LLM re-ranking of retrieved candidates
  reranker:
    enabled: ${RAG_RERANKER_ENABLED:false}
    url: ${RAG_RERANKER_URL:}
    top-n: ${RAG_RERANKER_TOP_N:8}
  # Maximal Marginal Relevance diversification
  mmr:
    enabled: ${RAG_MMR_ENABLED:false}
    lambda: ${RAG_MMR_LAMBDA:0.5}
    pool-size: ${RAG_MMR_POOL_SIZE:30}
  # Query expansion (shared variant budget for multi-query, HyDE and decomposition)
  query-expansion:
    max-variants: ${RAG_QUERY_EXPANSION_MAX_VARIANTS:6}
    rrf-k: ${RAG_QUERY_EXPANSION_RRF_K:60}
  multi-query:
    enabled: ${RAG_MULTI_QUERY_ENABLED:false}
    variants: ${RAG_MULTI_QUERY_VARIANTS:3}
  hyde:
    enabled: ${RAG_HYDE_ENABLED:false}
    max-chars: ${RAG_HYDE_MAX_CHARS:1000}
  query-decomposition:
    enabled: ${RAG_QUERY_DECOMPOSITION_ENABLED:false}
    max-sub-questions: ${RAG_QUERY_DECOMPOSITION_MAX_SUB_QUESTIONS:4}
  # Self-RAG relevance gate applied before generation
  retrieval-grading:
    enabled: ${RAG_RETRIEVAL_GRADING_ENABLED:false}
    min-score: ${RAG_RETRIEVAL_GRADING_MIN_SCORE:0.0}
    min-hits: ${RAG_RETRIEVAL_GRADING_MIN_HITS:1}
    broaden: ${RAG_RETRIEVAL_GRADING_BROADEN:true}
  # Intent-aware filter inference (opt in per request via inferFilters)
  filter-inference:
    category-property: ${RAG_FILTER_INFERENCE_CATEGORY_PROPERTY:}
  # Post-generation citation faithfulness check (adds one LLM call per answer)
  citation:
    verify:
      enabled: ${RAG_CITATION_VERIFY_ENABLED:false}
  # Small-to-big retrieval: expand each hit to its parent section for LLM context
  retrieval:
    small-to-big:
      enabled: ${RAG_RETRIEVAL_SMALL_TO_BIG_ENABLED:false}
      max-section-chars: ${RAG_RETRIEVAL_SMALL_TO_BIG_MAX_SECTION_CHARS:4000}
  # In-app evaluation smoke endpoint (content-lake-eval remains the authoritative gate)
  evaluation:
    enabled: ${RAG_EVALUATION_ENABLED:false}
```

Ingestion has a matching opt-in flag, `content-lake.ingest.keyword-context-enrichment-enabled`
(default `false`), which prepends document-level context to each chunk's keyword-search text.

Conversation memory storage:

- Default implementation is in-memory.
- To use Redis or a database, provide a custom Spring bean implementing `ConversationMemoryStore`; the default in-memory store is only created when no other `ConversationMemoryStore` bean exists.
