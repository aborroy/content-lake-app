# ${artifactId}

A Content Lake source connector for `${sourceType}`, shipped as a jar. No module in
`content-lake-app`, no edit to any service Dockerfile: an ingester scans its plugin directory at startup
and uses whatever it finds.

## Build and install

```bash
mvn package
cp target/${artifactId}-${version}.jar /path/to/content-lake-app-deployment/connectors/
docker compose --profile alfresco up -d --force-recreate batch-ingester
```

Then check it was picked up, from outside the container:

```bash
curl http://localhost:9090/api/connectors -u admin:admin
curl http://localhost:9090/api/connectors/schema -u admin:admin
```

The first response names every connector, the jar each came from, and anything that failed to load. A jar
that cannot be read, or whose configuration does not satisfy its schema, is reported there without stopping
the ingester.

## Configure it

`SourceConnectorPlugin.schema()` declares what this connector needs. Those names are read from the
ingester's environment, so they are set like any other setting:

```yaml
services:
  batch-ingester:
    environment:
      ${sourceType.toUpperCase()}_URL: http://repository.example.com
```

Spring's relaxed binding maps `${sourceType}.url` to `${sourceType.toUpperCase()}_URL`, so environment
variables work without declaring anything.

A setting marked `required` that is missing, or one whose value does not match its declared type, aborts
startup with a message naming it. Set `content-lake.connector.validation=warn` to start anyway.

## What to implement

`SourceConnectorClient` has four methods the pipeline needs, each stubbed so this project compiles and its
tests pass before the source's API is touched:

| Method | Purpose |
|---|---|
| `getNode` | One node's metadata, or `null` when it does not exist |
| `getChildren` | One page of a container's children |
| `downloadContent` | The binary as a temp `Resource`, which the caller deletes |
| `getContent` | The binary as bytes |

Two things to get right:

- **`readPrincipals` is a security boundary.** It is what the RAG service filters retrieval by. A node
  whose permissions cannot be read must be left out rather than returned with a permissive set, or its
  content becomes readable by everyone.
- **`modifiedAt` decides re-work.** It is what lets a re-sync skip unchanged content. Returning `null`, or
  a value that always moves, makes every sync re-extract and re-embed the whole corpus.

Text extraction is the host's job by default: its chain tries any configured engine, then in-process Tika.
Implement `createTextExtractor` only when the source can convert its own content and save the download.
