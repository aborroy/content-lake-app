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
drive a client they were compiled against, so for them a mounted jar is listed and otherwise inert.

## Writing a new one

Start from `archetype/` (see [archetype/README.md](archetype/README.md) for its properties), and read
[examples/sample-directory-connector](examples/sample-directory-connector/README.md) for a complete
implementation. `cmis-connector/` is worth reading for how a real dependency is shaded in without shading
`content-lake-spi` or `spring-core`, which must stay `provided` or every cast against the host's SPI types
fails.
