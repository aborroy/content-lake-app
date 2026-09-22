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

Every ingester loads a connector; `plugin-batch-ingester` is the one that ingests with it. The Alfresco,
Nuxeo ingesters each drive a client they were compiled against, so they never ingest from a
mounted jar and only list what they found.

Loading is not the same as being inert, though. Validation is fail-closed by default, so a jar whose
required settings are supplied only to `plugin-batch-ingester` would otherwise stop the other five
ingesters at startup over a connector they were never going to use. They therefore default
`content-lake.connector.validation` to `warn`: the jar and the reason it did not load appear under
`problems[]` on `GET /api/connectors`, and ingestion continues. Set `CONNECTOR_VALIDATION=fail` on one of
those services (or `CONNECTOR_VALIDATION_INGESTERS=fail` for all five in the deployment stack) to make it
refuse to start instead. `plugin-batch-ingester` still defaults to `fail`, because for it a connector
that will not load means there is nothing to ingest.

```bash
# From content-lake-app-deployment, on top of any base profile
CONNECTOR_SYNC_USERNAME=admin CONNECTOR_SYNC_PASSWORD=admin \
  docker compose --profile alfresco --profile connector up -d --build plugin-batch-ingester

curl -u admin:admin -X POST http://localhost:9096/api/sync/configured
curl -u admin:admin http://localhost:9096/api/status
```

It resolves its client, its scope rules and optionally its extractor from `ConnectorRegistry`, filling in a
permissive default scope and the host extraction chain when the connector supplies neither. Configuration is
under `connector.*`:

| Setting | Meaning |
|---|---|
| `connector.source-type` | Which loaded connector to ingest with. Optional with one jar mounted, required with several |
| `connector.roots` | Containers to walk, unless an operator selected some. Empty asks the connector, through `ContentSourceClient.getRootNodeIds()` |
| `connector.page-size` | Children fetched per listing |
| `connector.max-depth` | Depth backstop for a hierarchy that does not bottom out |
| `connector.security.*` | Credentials for this ingester's own sync API. No defaults; startup fails without both |
| `connector.reconcile.*` | Post-discovery deletion sweep. Off by default |
| `connector.change-feed.*` | Read the connector's change feed instead of walking it. Off by default |
| `connector.cursor.*` | Where the change feed's position is kept between passes |
| `connector.selection.*` | Where the roots an operator chose are kept, so a scope change needs no restart. Off by default |

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
| `connector.selection.store` | `none` | `none`, `hxpr`, `file` or `memory`. `none` keeps the pre-selection behaviour exactly and leaves `/api/selection` unavailable |
| `connector.selection.hxpr-path` | `/content-lake/_state/roots` | Where the `hxpr` store keeps its state document. Deliberately not the cursor folder |
| `connector.selection.file` | `/var/lib/content-lake/connector/roots.json` | Path the `file` store writes, which needs a writable mount |

The default is `hxpr` because this container's mounts are read-only, so a file store would have nowhere to
write. Its state document carries no `cin_sourceId` and no `cin_paths`, which is what keeps the sweep from
proposing the deletion of the cursor that tells it where it is, and it has no embeddings, which is what keeps
it out of search results. `memory` is the honest answer for a deployment with neither a mount nor hxpr write
access: every pass is a full walk, which is the behaviour with the feature off.

#### Changing which roots a sync walks, without a restart

Roots used to be read once at startup into an immutable list, so changing a scope meant editing configuration
and restarting a container. With `connector.selection.store` set to anything but `none`, a pass asks the
selection store instead, and `GET`, `PUT` and `DELETE /api/selection` read and change it. Precedence per pass
is the stored selection, then `connector.roots`, then whatever the connector names as its own root.

Three properties of that are worth stating, because each is a decision rather than an accident.

**A selection that is present and empty is not a fall-through.** It means somebody cleared the choice. Treating
it as "walk everything" would silently re-ingest a whole source the moment a picker was emptied, so an empty
selection yields an empty scope.

**A pass with an empty scope reports itself incomplete.** The reconciliation sweep deletes what an
authoritative enumeration did not mention, so a pass that walked nothing must never claim to be complete: it
would propose deleting every document of the source. Incomplete means the sweep declines to act, which is the
safe direction.

**With `none`, nothing changes.** Roots come from configuration and then from the connector, a deployment that
supplies neither still fails at startup, and the endpoint answers `501`. That is the default, so upgrading
alters no behaviour.

The store also moves root resolution off the startup path, which matters for any connector that resolves its
roots over the network: doing that during bean construction turns a transient source outage into a container
that will not boot, rather than a job that failed and can be retried. Both the `hxpr` and `file` stores are
wiped by `make clean`, which is correct, because a selection is the scope of an index that no longer exists. A
selection does survive a container restart.

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
  docker compose --profile alfresco --profile connector up -d plugin-batch-ingester
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

### The Filesystem Connector

`plugins/filesystem-connector/` ingests a local or mounted directory. It was an in-tree module group with a
service image, a Dockerfile stage pair and an opt-in compose profile of its own until #148, which is the
change that made the plugin jar the single route for a new source. Nothing about the source needed any of
that: it is the only connector with no runtime dependency at all, since the source is the filesystem the JDK
already talks to, so its jar carries its own classes and nothing else.

```bash
mvn -pl common/content-lake-spi -am install -DskipTests
mvn -f plugins/filesystem-connector/pom.xml package
cp plugins/filesystem-connector/target/filesystem-connector-1.0.0.jar \
   ../content-lake-app-deployment/connectors/
```

```bash
CONNECTOR_SOURCE_TYPE=filesystem FILESYSTEM_ROOT_PATH=/data/connector \
CONNECTOR_HOST_PATH=./filesystem-data \
CONNECTOR_SYNC_USERNAME=admin CONNECTOR_SYNC_PASSWORD=admin \
  docker compose --profile alfresco --profile connector up -d
```

| Setting | Meaning |
|---|---|
| `filesystem.root-path` | Absolute directory to ingest. Required, and validated at startup as a directory that exists |
| `filesystem.source-id` | Source alias stored as the second half of `cin_sourceId`; defaults to `filesystem` |
| `filesystem.read-principals` | Who may retrieve the ingested files; defaults to everyone |
| `filesystem.include-extensions` | Extensions to ingest, without the dot; empty means every file |
| `filesystem.exclude-patterns` | Path fragments that exclude a file or directory. Hidden entries are always skipped |
| `filesystem.page-size` | Entries fetched per directory listing |

**The setting names are unchanged from the in-tree module**, so an existing `FILESYSTEM_*` configuration keeps
working: what changed is the service that reads them. A deployment migrates by building the jar into
`connectors/` and using the `connector` profile instead of the retired `filesystem` one.

Two things worth stating where an operator will hit them:

- **A filesystem has no permissions to map**, so `filesystem.read-principals` is the only thing deciding who
  can retrieve the content, and it defaults to everyone. That is right for a corpus already shared with
  everyone who can reach the search endpoint, and wrong for anything else.
- **`root-path` is validated as a directory that must exist.** It is the one startup check here with real
  teeth: an ingester pointed at a path that was never mounted reports zero documents and reads as an empty
  source rather than as a misconfiguration.

There is no change feed, so every pass is a walk. Because the reconciliation sweep is then the only thing
that ever deletes, a deployment replacing a mounted file expects the sweep to be on.

### The SharePoint Online Connector

`plugins/sharepoint-connector/` ingests SharePoint Online through Microsoft Graph. Graph is the only
interface: Microsoft deprecated the SharePoint CMIS producer years ago, so the CMIS connector is not a route
to SharePoint, and Graph is cloud-only, which puts SharePoint Server on premises out of scope rather than
merely untested.

```bash
mvn -pl common/content-lake-spi -am install -DskipTests
mvn -f plugins/sharepoint-connector/pom.xml package
cp plugins/sharepoint-connector/target/sharepoint-connector-1.0.0.jar \
   ../content-lake-app-deployment/connectors/
```

```bash
# Against a tenant. Node ids are '<driveId>:<itemId>', so a single drive needs no CONNECTOR_ROOTS.
SHAREPOINT_TENANT_ID=<directory-tenant-id> SHAREPOINT_CLIENT_ID=<application-client-id> \
SHAREPOINT_CLIENT_SECRET=<secret> SHAREPOINT_DRIVE_IDS='b!<drive-id>' \
CONNECTOR_SOURCE_TYPE=sharepoint \
CONNECTOR_SYNC_USERNAME=admin CONNECTOR_SYNC_PASSWORD=admin \
  docker compose --profile alfresco --profile connector up -d plugin-batch-ingester
```

| Setting | Meaning |
|---|---|
| `sharepoint.drive-ids` | Drives to ingest, comma separated. Supply this, `sharepoint.site-url` or `sharepoint.site-id` |
| `sharepoint.site-url` | The site as a human has it. Resolved to its document libraries on first use, so drive ids need not be found out of band |
| `sharepoint.site-id` | The composite Graph site id, for a caller that already has it |
| `sharepoint.drive-names` | Document-library names to take from the site. Empty takes every library. Ignored when `drive-ids` is set |
| `sharepoint.folder-paths` | Folders to start a pass from, e.g. `/Finance`. Scopes the walk, unlike `include-paths` which filters after it |
| `sharepoint.client-id` | Application (client) id of the Entra ID app registration. Required |
| `sharepoint.tenant-id` | Directory (tenant) id; the authority is derived from it. Required for `client-credentials` and `device-code` unless `sharepoint.authority` is set |
| `sharepoint.authority` | Entra ID authority, for a sovereign cloud only. Must be `https`: msal4j rejects any other scheme |
| `sharepoint.auth-mode` | `client-credentials` (default, app-only), `device-code` (delegated, as a named user) or `static-token`; see below |
| `sharepoint.client-secret` | Client secret. Never printed. Supply this or a certificate, never both |
| `sharepoint.certificate-path` / `sharepoint.certificate-password` | PKCS#12 client certificate. Preferred over a secret |
| `sharepoint.access-token` | Bearer token for `static-token`. Development only, never printed |
| `sharepoint.token-cache-path` | File holding the msal4j token cache for `device-code`. Required for that mode. Marked secret: the file contains a refresh token |
| `sharepoint.scopes` | Delegated scopes for `device-code`. Defaults to `Sites.Read.All` plus `offline_access`. Ignored by the other modes |
| `sharepoint.graph-base-url` | Graph endpoint. Defaults to `https://graph.microsoft.com/v1.0`; point it at the mock to run without a tenant |
| `sharepoint.source-id` | Source alias stored in `cin_sourceId`. Defaults to the first drive id |
| `sharepoint.include-paths` / `sharepoint.exclude-paths` | Path scope. Excludes are applied after includes and win |
| `sharepoint.include-mime-types` / `sharepoint.exclude-mime-types` | MIME scope, `text/*` wildcards allowed |
| `sharepoint.acl-fallback` | `fail-closed` (default) or `public`; see below |
| `sharepoint.group-grants` | `map` (default) or `skip`; see below |
| `sharepoint.permissions-mode` | `per-item` (default, one 5-unit call per item) or `hierarchical` (resolve inherited ACLs from the sharing hierarchy, needs `Sites.FullControl.All`); see below |
| `sharepoint.everyone-claims` | Display names meaning every user in the tenant, for a tenant that words them differently |
| `sharepoint.resource-units-per-minute` | Graph budget to spend per minute, default 1000. Zero disables metering |
| `sharepoint.resource-unit-burst` | Units allowed to accumulate, default 200 |

#### Graph application permissions an administrator has to grant

Per Microsoft's reference for `driveItem: delta` and `List permissions`, the least privileged **application**
permission for both is `Files.Read.All`, with `Files.ReadWrite.All`, `Sites.Read.All` and
`Sites.ReadWrite.All` listed as higher privileged alternatives. The connector reads only, so `Files.Read.All`
is the ask, and admin consent is required because app-only permissions cannot be user-consented.

Two things worth raising with whoever grants it:

- **`Sites.Selected` is not listed for either API.** It is the obvious way to narrow the request to one site,
  and the reference tables for the two calls this connector depends on do not include it. Confirm it against
  the tenant before promising a per-site grant, because reading `/permissions` is the whole point and a grant
  that cannot do it is worse than none.
- **`Sites.FullControl.All` is only needed for the cheap crawl.** Microsoft's note on the delta scanning
  headers states that to process permissions correctly with `Prefer: deltashowsharingchanges` "your
  application will need to request **Sites.FullControl.All** permissions". The connector sends
  `Prefer: hierarchicalsharing` only when `sharepoint.permissions-mode` is `hierarchical`, so a deployment
  that cannot get that grant leaves the setting at its default and pays the per-item cost instead. It is not
  needed for correctness, only for cost.

#### Cost, which is metered rather than estimated

Graph meters SharePoint in resource units, not requests, and the prices are uneven: 1 for a single-item get,
a delta page with a token or a content download, 2 for a multi-item query, and **5 for any permission
operation**. The permissions collection cannot be `$expand`ed onto a `driveItem` get, so an ACL read is
always its own call.

The connector reports its own spend and its cost per document as it runs -- at the first document, at the
tenth, and every hundred after that -- so the figure for a real corpus is a log line rather than an estimate.
The default budget is set below the documented per-application per-tenant cap of 1250 units a minute, because
everything sharing the app registration draws on the same cap.

`sharepoint.permissions-mode` is what decides the figure, and it is the one setting to think about before a
first crawl:

| Mode | How an item's ACL is obtained | Measured, not estimated |
|---|---|---|
| `per-item` (default) | one 5-unit `/permissions` call each | **5.60 units per document**. The daily cap binds near 200,000 documents |
| `hierarchical` | read only where the sharing hierarchy says permissions are set; everything else inherits from the nearest ancestor | **1.10 units per document**, from **one** permissions call serving nine items. Roughly a fivefold cut, and bounded at sixfold because the 1-unit content download is then the whole cost |

Both figures are the ingester's own `Graph resource units ... per document` line at the tenth document of a
pass over the same fixture tree, from `test/test-sharepoint.sh` in the deployment repository (S25 and S26).

`hierarchical` sends `Prefer: hierarchicalsharing`, which makes Graph report the `shared` facet only on a
permission-hierarchy root rather than on every item that inherits from one. That facet's presence is then the
only signal distinguishing the two, which has three consequences worth knowing:

- It needs **`Sites.FullControl.All`**, and the connector **refuses to run** rather than fall back if
  `Preference-Applied` does not come back. A silent fallback would quintuple a crawl's spend without saying
  so, which against a daily cap is the difference between a crawl that finishes and one that does not. The
  refusal names the setting to change.
- The saving depends on how the tenant is administered, not on corpus size. A tenant where users share
  individual files heavily has more hierarchy roots and less to inherit, so measure rather than quote.
- Who may read a document does not change with the mode. An inheriting item's own permissions collection
  reports every entry it inherits, which is the same set the ancestor's collection reports, and there is a
  test asserting the two modes agree.

#### Enumeration is a change feed, and the first pass always walks

`delta` is the only enumeration Graph guarantees is complete under concurrent writes, and it warns that
paging a folder's `children` collection may miss items if writes happen during the walk. So the connector
implements the SPI change feed: a `deltaLink` becomes the host's cursor, a `deleted` facet becomes a
tombstone, and a `410 Gone` becomes an expired cursor that sends the host back to a walk.

`connector.change-feed.enabled` is off by default, and even when on, a pass with no stored cursor walks the
source in full and only then saves the position it read beforehand. So the walk is on the critical path for
every deployment's first pass. Path scope cannot be enforced on feed results, because Graph omits
`parentReference.path` from a delta response; a deployment that needs path scope enforced on every pass has
to leave the feed off.

#### ACLs, and the one limitation to state to users

Permissions are mapped fail-closed. A user grant emits both the Entra object id and the userPrincipalName,
which are one identity in the two forms that match before and after an Entra group resolver exists. A group
emits `GROUP_<objectId>` and never a display name, because Entra display names are not unique. An
organisation-scoped sharing link maps to everyone; an anonymous link grants nothing to any authenticated
caller. An unrecognised role or shape grants nothing and is counted, so an item under-shares rather than
over-shares.

**A document granted only to an Entra ID group is retrievable by nobody until the query-path resolver is
switched on.** Ingestion records the group correctly either way; expanding a caller's group membership at
query time is `rag-service`'s `EntraGroupResolver`, a conditional bean that exists only where
`rag.security.entra.enabled` is true (see [Entra ID group expansion](#entra-id-group-expansion)). Each run
logs how many documents this affects, and how many depend on site-local principals, which no resolver can ever
expand. `sharepoint.group-grants=skip` omits group principals entirely instead; there is deliberately no
setting that widens a group grant to the whole tenant.

`sharepoint.acl-fallback` decides what happens to an item whose permissions cannot be read: `fail-closed`
does not ingest it, `public` makes it readable by everyone. There is no `sync-account` option as there is for
CMIS, because app-only auth has no user account whose access could stand in for a document's.

#### Running it without a tenant

`sharepoint.auth-mode=static-token` with `sharepoint.graph-base-url` pointed at the mock Graph service runs
the whole connector locally: real protocol handling, real paging, real ACL mapping, real downloads. Only two
things differ from the cloud, and both are configuration. msal4j refuses an authority that is not `https`, so
the mock cannot double as an Entra ID and token acquisition is the one part a local run does not exercise;
the connector's own tests cover it against the real library.

A static token cannot be refreshed, so it is not a deployment mode and the connector says so at startup. It
is also how a developer validates ACL mapping against their own OneDrive, which is the only environment where
Graph returns a complete permission set to a non-administrator.

`content-lake-app-deployment/test/test-sharepoint.sh` is the end-to-end check, and the assertions that matter
are the ACL ones: a document granted to one named user is not returned to a caller its ACL excludes, and a
group-only document is returned to nobody.

#### Naming a site instead of finding drive ids, and scoping to folders

`sharepoint.drive-ids` used to be required, and there was no supported way to find out what to put in it: the
connector never called `GET /sites/{id}/drives`, so a drive id was copied out of a Graph Explorer session.
Naming `sharepoint.site-url` (or `sharepoint.site-id`, if you already have the composite form) resolves the
site's document libraries instead, and `sharepoint.drive-names` narrows that to particular libraries.

Resolution is lazy and memoised, and both halves of that matter. It cannot happen during construction, because
the host builds a client for every mounted jar across all six ingesters, so a Graph call there would let one
connector's outage break a deployment running a different one. It cannot happen per pass either, because roots
are resolved on every pass, so an unmemoised lookup would spend the tenant's budget on the same two calls
forever. Configured drive ids still win and cost no Graph call at all, so an existing deployment behaves
exactly as before.

One consequence worth knowing: with a site rather than drive ids, the source alias is derived from the site
string, not from the resolved libraries. Deriving it from a library would mean a change in the order Graph
returns them silently renames the source and orphans its cursor and everything already indexed under the old
name. Set `sharepoint.source-id` explicitly if you want something prettier than the derived slug.

**`sharepoint.folder-paths` scopes a pass; `sharepoint.include-paths` filters one.** They read alike and are
not the same. Include paths are applied by the host to each node *after* enumeration, so a run configured for
one folder of a large library still enumerates the whole library and pays the resource units for it. Since
permission reads dominate the crawl budget, that is the difference between a selection being cheap and being
the most expensive way to sync a folder. Folder paths are resolved to item ids through Graph's path-addressing
form and become the roots the walk starts from, so an unselected folder is never enumerated.

Two behaviours of folder paths are deliberate. A path that resolves in no drive at all is refused, because
falling back to the drive root would silently widen a deliberately narrow scope to the whole library. But a
path missing from *one* drive of several is skipped with a warning, because a site with multiple libraries will
not have the same folder in each and refusing the run would make the setting unusable.

**The change feed does not narrow with the selection.** Graph's delta feed is per drive, so with
`connector.change-feed.enabled=true` an incremental pass still reads the whole drive's feed and sees items
outside the selected folders. The saving is in what gets fetched, extracted, embedded and indexed, not in the
feed read itself. Note also that delta responses carry no `parentReference.path`, so path scope cannot be
enforced on a feed pass at all; a deployment that needs it enforced on every pass has to leave the feed off.

#### Running it as a named user, where the tenant will not grant application permissions

`sharepoint.auth-mode=device-code` authenticates as a person instead of as an application. A human signs in
once on the host with `scripts/sharepoint-device-login.sh` in the deployment repository, which prints a short
code and a URL and then writes a token cache; the connector reads that cache and refreshes silently
afterwards, so restarts need nobody. It is the mode for a tenant that will issue a public-client registration
but not application permissions.

Three things about it are not interchangeable with app-only, and all three should be understood before
choosing it:

- **It indexes one identity's view.** Graph returns item permissions by caller, so a nominal-user crawl
  produces a faithful ACL only where the crawling identity owns the content, and content that identity cannot
  read is *absent from the index* rather than present and unretrievable. That is a completeness limitation
  rather than a security one, but it means the index answers "what can this person see" rather than "what is
  in this site".
- **Recovery needs a human.** A refresh token can be ended by expiry, a password reset or a Conditional
  Access change, and only an interactive sign-in restores it. The connector reports the mode as not supported
  for production at startup for exactly this reason.
- **Sign-in cannot happen in the container.** The device-code call blocks for up to fifteen minutes, and the
  service has no way to put a human in front of a browser. An empty or spent cache is therefore reported as a
  configuration problem naming the command that fixes it, rather than as something a retry will resolve.

The delegated permission to ask for is `Sites.Read.All`, which is the least-privileged one measured to serve
every call this connector makes, including the item-permission reads the ACL mapping depends on, plus
`offline_access` for the refresh token. `sharepoint.scopes` overrides that pair; it does not apply to the
other two modes, because an app-only token is scoped by the permissions granted to the registration and Entra
rejects resource scopes in a client-credentials request.

The cache file holds a refresh token, which outlives the access tokens it mints and can be redeemed from
anywhere. Treat it as a credential: the deployment mounts it read-only, keeps it out of version control, and
narrows its permissions. Because the mount is read-only the connector holds rotated tokens in memory and says
so once, which costs nothing until the stored token finally expires.

### Connector Schema And Startup Validation

Each source connector publishes the settings it needs, and every ingester checks its configuration
against that schema before it starts serving. A missing or malformed setting is reported by name instead
of surfacing later as a downstream symptom, such as a filesystem connector that finds no documents because
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

Group expansion itself is per source type, not global: `rag-service` holds one `SourceGroupResolver` bean per
type and ships three, for `alfresco`, `nuxeo` and `sharepoint`. A source of any other type contributes a
clause built from the caller's own authorities only, so its group-granted documents are retrievable by
nobody. That is logged once per source at WARN, and the remedy is a resolver bean for that type rather than a
setting. Cache hit-rate is exposed as `cache.gets{cache=rag.security.groups}` under `/actuator/metrics`.

##### Entra ID group expansion

`EntraGroupResolver` is the `sharepoint` one, and unlike the other two it is a **conditional bean**: with
`rag.security.entra.enabled` unset there is no resolver for that source type at all. That is deliberate. A
resolver that exists and cannot reach its directory is worse than none, because `group-resolution-failure`
would then cost every caller the whole source rather than only its group-granted documents.

It calls `GET /users/{identity}/transitiveMemberOf/microsoft.graph.group` -- transitive, because nested group
grants are ordinary in Entra -- and emits `GROUP_<objectId>`, matching what the SharePoint connector's ACL
mapper writes at ingest. Never a display name on either side: Entra display names are not unique, so a
principal built from one would match documents granted to a different group of the same name.

```yaml
rag:
  security:
    entra:
      enabled: ${RAG_SECURITY_ENTRA_ENABLED:false}
      source-type: ${RAG_SECURITY_ENTRA_SOURCE_TYPE:sharepoint}
      graph-base-url: ${RAG_SECURITY_ENTRA_GRAPH_BASE_URL:https://graph.microsoft.com/v1.0}
      tenant-id: ${RAG_SECURITY_ENTRA_TENANT_ID:}
      client-id: ${RAG_SECURITY_ENTRA_CLIENT_ID:}
      client-secret: ${RAG_SECURITY_ENTRA_CLIENT_SECRET:}
      certificate-path: ${RAG_SECURITY_ENTRA_CERTIFICATE_PATH:}
      certificate-password: ${RAG_SECURITY_ENTRA_CERTIFICATE_PASSWORD:}
      auth-mode: ${RAG_SECURITY_ENTRA_AUTH_MODE:client-credentials}
      access-token: ${RAG_SECURITY_ENTRA_ACCESS_TOKEN:}
      username-suffix: ${RAG_SECURITY_ENTRA_USERNAME_SUFFIX:}
```

| Setting | Effect |
|---|---|
| `enabled` | Whether the bean exists at all. Without it, a SharePoint document granted only to a group is retrievable by nobody |
| `source-type` | Which source type this resolver answers for. Only worth changing if a connector reports a different type |
| `tenant-id` / `client-id` | Required when enabled. A separate app registration from the connector's is fine, and needs `GroupMember.Read.All` or equivalent to read membership |
| `client-secret` / `certificate-path` | Exactly one of the two. A certificate is preferred: a secret expires on a date nobody diarises |
| `auth-mode` | `client-credentials` (app-only) or `static-token`, the latter for a local run against the mock Graph service, since msal4j refuses an authority that is not `https` |
| `username-suffix` | Appended to a bare username to form the Entra identity, so a caller known to the repository as `sarah` is looked up as `sarah@contoso.com`. Anything already containing `@` is left alone |

It consumes the same `group-cache` and `group-resolution-failure` settings as the other two resolvers, so a
membership change takes effect within the cache TTL and a Graph outage costs what
`group-resolution-failure` says it costs. A 404 for an identity is "no such user here", which costs that
caller only this source's group grants; anything else is a failure and is never cached.

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
