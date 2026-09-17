# plugins

Everything in this directory is a connector, or something that makes connectors. **Nothing here is built
by the Maven reactor**, and nothing here may be added to the root POM's `<modules>` list.

| Directory | What it is | Coordinates |
|---|---|---|
| `archetype/` | Maven archetype that generates a connector skeleton | `org.hyland:content-lake-connector-archetype:1.0.0-SNAPSHOT` (`maven-archetype`) |
| `cmis-connector/` | A shipped connector: any CMIS 1.1 repository as a source | `org.hyland.contentlake:cmis-connector:1.0.0` (jar) |
| `examples/sample-directory-connector/` | A worked example, not a supported source | `org.hyland.example:sample-directory-connector:1.0.0` (jar) |

The three sit at three points in one lifecycle: `archetype/` generates a connector,
`examples/sample-directory-connector/` is one to read, and `cmis-connector/` is one that ships.

## `plugins/` is not `connector/`

Two directories at the repository root sound alike and are not:

- `connector/` is a **reactor module group**. It holds `connector-batch-ingester`, the Spring Boot host
  application (port 9096) that loads plugin jars at runtime. It has no source adapter of its own.
- `plugins/` is **this directory**: the plugins themselves, which the reactor never builds.

A third name, `connectors/`, exists only in the `content-lake-app-deployment` repository, where it is the
runtime drop directory that built jars are copied into.

## Why these are outside the reactor

Every service Dockerfile in the deployment repository enumerates the reactor's modules explicitly. Adding a
connector to `<modules>` would mean editing all of them, and omitting one breaks a service that has nothing
to do with that connector. A connector is built, shipped and iterated on independently, which is the entire
point of the plugin mechanism.

That independence is also why the versions here do not track the reactor's `1.0.0-SNAPSHOT`: a plugin is
released on its own cadence, against the SPI it was compiled with.

## Building

Each project builds on its own, with `content-lake-spi` installed in the local Maven repository first:

```bash
# from the repository root
mvn -pl common/content-lake-spi -am install -DskipTests

mvn -f plugins/cmis-connector/pom.xml package
mvn -f plugins/examples/sample-directory-connector/pom.xml package
mvn -f plugins/archetype/pom.xml install
```

Then copy the jar into a deployment's plugin directory:

```bash
cp plugins/cmis-connector/target/cmis-connector-1.0.0.jar \
   ../content-lake-app-deployment/connectors/
```

Only `connector-batch-ingester` **ingests** with a plugin. The Alfresco, Nuxeo and filesystem ingesters each
drive a client they were compiled against, so they only list what they found in the plugin directory.

They still load it, so a jar is not inert: with fail-closed validation, one whose settings are configured
for `connector-batch-ingester` alone would stop those five at startup. They default
`content-lake.connector.validation` to `warn` for that reason, reporting the jar under `problems[]` on
`GET /api/connectors` and starting anyway. `connector-batch-ingester` keeps the `fail` default, since a
connector it cannot load leaves it with nothing to ingest.

## Writing a new one

Start from `archetype/` (see [archetype/README.md](archetype/README.md) for its properties), and read
[examples/sample-directory-connector](examples/sample-directory-connector/README.md) for a complete
implementation. `cmis-connector/` is worth reading for how a real dependency is shaded in without shading
`content-lake-spi` or `spring-core`, which must stay `provided` or every cast against the host's SPI types
fails.

## Optional: implementing a change feed

A connector that implements nothing beyond `ContentSourceClient`'s abstract methods is walked in full on
every sync pass. That is a supported, fully functional choice and is what all three projects here do: the
host enumerates containers, ingests what it finds, and its reconciliation sweep works out what disappeared
by comparing the index against that enumeration.

A source that can report its own changes since a token can offer that instead, by overriding three default
methods:

| Method | Default | Override to |
|---|---|---|
| `supportsChangeFeed()` | `false` | `true`. This is the capability gate: with it false the other two are never called |
| `initialCursor()` | `null` | the source's **current** feed position, or keep `null` if it cannot name one |
| `changesSince(String cursor, int maxItems)` | throws `UnsupportedOperationException` | one page of changes as a `SourceChangePage` |

`changesSince` throws rather than returning an empty page because an empty page means "nothing changed", and
a host that took a stub's silence for that answer would index nothing and treat the empty deletion list as
authoritative.

What the host guarantees in return:

- **It stores the cursor, and the connector persists nothing.** Whatever `nextCursor` a page carries comes
  back on the next pass, keyed by source type and id.
- **A cursor is an opaque `String`.** Nothing compares, parses or orders it, so a `deltaLink`, a page token,
  an ISO timestamp or a sequence number are all equally valid.
- **`initialCursor()` is never a licence to skip work.** A host with no stored cursor reads the position,
  walks the source in full, and only then saves what it read. Returning a "now" token cannot lose the
  content that already exists, and a change made while the walk ran is replayed by the next pass rather than
  falling between the two mechanisms.
- **Expiry is expected, not an error.** Set `cursorExpired` on the page (or return `SourceChangePage.expired()`)
  and the host discards the window, forgets the cursor and falls back to a full walk with its sweep in the
  same job. Every real feed has this state: Graph invalidates a `deltaLink`, Drive rejects a stale
  `startPageToken`, S3 markers age out.
- **Deletions the feed reports are applied as stated.** A tombstone naming one node id is a first-hand
  statement about that node, so it bypasses the sweep's ratio and cap guards, which exist to catch an
  enumeration that came back suspiciously empty.

What that costs: on a pass driven by the feed, the sweep does not run, so a deletion the feed fails to
mention is a document that stays in the index. Deployments pay that back with
`connector.change-feed.full-walk-every`, which forces a walk plus sweep every Nth pass.

`SourceChangePage.of(changed, deleted, nextCursor, moreAvailable)` builds a page; set `moreAvailable` while
further pages remain and the host keeps calling with the cursor the previous page returned. Report a removed
node as `SourceTombstone.deleted(nodeId)`; a node that is still present but should no longer be indexed
belongs in `changed`, and the host turns it into an `OUT_OF_SCOPE` tombstone itself once its `ScopeResolver`
rejects it.
