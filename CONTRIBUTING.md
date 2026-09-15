# Contributing

This project is part of the **Content Lake** PoC ecosystem. Contributions are welcome.

## Before You Start

- Read the [README](README.md) and [docs/architecture.md](docs/architecture.md).
- Check the open issues before starting new work.
- For significant changes, open an issue first to discuss the approach.

## Building and Testing

All commands run from `content-lake-app/`:

```bash
# Full build
mvn clean package

# Run all tests
mvn test

# Single module
mvn test -pl common/content-lake-core
```

See the [README](README.md) for prerequisites and the full build reference.

## Making Changes

1. Fork the repository and create a branch from `main`.
2. Make your changes. Keep commits focused -- one logical change per commit.
3. Ensure all tests pass: `mvn test`
4. Open a pull request. Describe what changed and why.

## SPI / Core Purity Rule

`content-lake-spi` and `content-lake-core` must have **zero** `org.alfresco.*` or `org.nuxeo.*`
imports. This is enforced at compile time. Do not add source-specific imports to shared modules.

## Adding a Maven Module

See [Adding a Maven Module](docs/architecture.md#adding-a-maven-module) for the checklist. It includes
the required changes to every service Dockerfile in the deployment repository, which are easy to miss
and break unrelated images when omitted.

A new **source** usually needs none of it: ship it as a connector jar under `plugins/` instead. See
[plugins/README.md](plugins/README.md).

## Commit Messages

Use the format: `type: short description` (or `#<issue> type: short description` for tracked work)

Types: `feat`, `fix`, `docs`, `refactor`, `test`, `chore`
