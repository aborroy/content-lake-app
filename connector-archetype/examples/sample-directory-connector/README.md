# sample-directory-connector

A complete connector plugin, in two classes. It ingests the files in a directory.

This is what the archetype's skeleton becomes once the TODOs are filled in. The skeleton compiles and
loads but discovers nothing, which demonstrates the plugin mechanism and not an ingestion; this one
actually ingests, which makes it the thing to read when writing a connector and the thing the deployment
suite uses to prove the connector host works.

It is an example, not a supported source. For a real mounted directory use the in-tree filesystem
connector (`filesystem-batch-ingester`), which has scope patterns, exclusions and real ACL configuration.

## Build

The project has no parent POM and is not a module of `content-lake-app`, so it builds exactly the way a
third-party connector builds: against `content-lake-spi` alone. That artifact has to be in the local Maven
repository first.

```bash
# From this directory, with JDK 25 for the reactor build
mvn -f ../../../pom.xml -pl common/content-lake-spi -am install -DskipTests
mvn package
```

The jar lands in `target/sample-directory-connector-1.0.0.jar`.

## Run

```bash
cp target/sample-directory-connector-1.0.0.jar <deployment>/connectors/

# From the deployment repository, on top of any base profile
SAMPLE_DIRECTORY_ROOT_PATH=/data/connector \
CONNECTOR_SYNC_USERNAME=admin CONNECTOR_SYNC_PASSWORD=admin \
  docker compose --profile alfresco --profile connector up -d

curl -u admin:admin http://localhost:9096/api/connectors
curl -u admin:admin -X POST http://localhost:9096/api/sync/configured
curl -u admin:admin http://localhost:9096/api/status
```

The directory has to be mounted into the `connector-batch-ingester` container at the path the setting
names. `CONNECTOR_HOST_PATH` is the compose variable for that mount.

## Settings

| Setting | Environment variable | Required | Meaning |
|---|---|---|---|
| `sample-directory.root-path` | `SAMPLE_DIRECTORY_ROOT_PATH` | yes | Directory to ingest, as seen from inside the container |
| `sample-directory.source-id` | `SAMPLE_DIRECTORY_SOURCE_ID` | no | Source alias stored as the second half of `cin_sourceId` |
| `sample-directory.read-principals` | `SAMPLE_DIRECTORY_READ_PRINCIPALS` | no | Comma-separated principals granted read access; defaults to everyone |
| `sample-directory.page-size` | `SAMPLE_DIRECTORY_PAGE_SIZE` | no | Entries per directory listing |

The host validates these against the schema before the client is built, so a missing `root-path`, or one
that is not a directory, is reported by name at startup rather than as an empty ingestion.

## What to copy from it

- **`getRootNodeId()`.** Implement it and the host needs no `connector.roots` setting: a batch pass starts
  where the connector says.
- **`downloadContent` returns a copy.** The pipeline deletes what it gets back after extraction, so
  returning the source file deletes the document that was just indexed.
- **A real `modifiedAt`.** It is what lets a re-sync skip unchanged content. Null, or a value that always
  moves, means every sync re-extracts and re-embeds everything.
- **Node ids are validated against the configured root.** An id makes a round trip through the index and
  comes back as a string; confining it keeps a stored id from reaching a file the deployment never mounted.
- **No `ScopeResolver` and no `TextExtractor`.** Both are optional, and leaving them out is the normal
  case: the host ingests every document it walks to and extracts with its own chain. Write a resolver only
  when the source has a notion of scope the host cannot see; write an extractor only when the source can
  convert content without a download.
- **Read principals decide retrieval.** They populate `sys_acl` and `cin_read`. A node whose permissions
  cannot be read must not be returned with a permissive set: leave it out instead.
