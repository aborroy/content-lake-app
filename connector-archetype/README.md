# Content Lake connector archetype

Generates a source connector project: the SPI implementation, its `META-INF/services` declaration, a
client stub and tests that pass as generated. The result builds to a jar that any ingester picks up from its
plugin directory.

## Use it

```bash
# Once: publish the SPI and this archetype to your local repository.
mvn -f ../pom.xml install -pl common/content-lake-spi -DskipTests
mvn install

mvn archetype:generate \
  -DarchetypeGroupId=org.hyland \
  -DarchetypeArtifactId=content-lake-connector-archetype \
  -DarchetypeVersion=1.0.0-SNAPSHOT \
  -DgroupId=com.example \
  -DartifactId=cmis-connector \
  -Dversion=1.0.0-SNAPSHOT \
  -Dpackage=com.example.cmis \
  -DsourceType=cmis \
  -DinteractiveMode=false

cd cmis-connector && mvn test
```

| Property | Meaning | Default |
|---|---|---|
| `sourceType` | Prefix of `cin_sourceId` for this connector's documents. `alfresco`, `nuxeo` and `filesystem` are taken; an ingester refuses a plugin that claims one it already has | `mysource` |
| `contentLakeVersion` | Version of `content-lake-spi` to compile against | `1.0.0-SNAPSHOT` |
| `springVersion` | Supplies the `Resource` type `ContentSourceClient` returns | `7.0.2` |

The generated project depends on `content-lake-spi` and `spring-core` as `provided`, and on nothing else.
That is the whole point: a connector is written against the SPI, so it neither compiles against core nor
carries a second copy of anything the ingester already has.

## A worked example

The generated client is a stub: `getChildren` returns nothing, so the jar loads and discovers zero
documents. [`examples/sample-directory-connector`](examples/sample-directory-connector) is the same project
with the TODOs filled in against a directory, which makes it the shortest complete connector there is and
the one to read for the details that are easy to get wrong (why `downloadContent` must return a copy, why
`modifiedAt` matters, why `getRootNodeId` is worth implementing).

## Where a connector actually ingests

A jar in an ingester's plugin directory is discovered, validated and listed at `GET /api/connectors` by
every ingester, but the Alfresco, Nuxeo and filesystem ingesters each drive a client they were compiled
against and never ask the registry for one. `connector-batch-ingester` is the service that does: it takes
its client, scope rules and optionally its extractor from the connector, and it is what a plugin connector
should be pointed at.

## Why this is not a module of the root POM

Every service Dockerfile enumerates the reactor's modules, so adding this to `<modules>` would mean editing
seven Dockerfiles to ship a tool whose reason for existing is that people should not have to edit seven
Dockerfiles. It builds on its own, from this directory.
