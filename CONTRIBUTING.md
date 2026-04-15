# Contributing to Ariadne

Thank you for your interest in contributing to Ariadne! This guide will help you get started.

## Reporting Bugs

Open a [GitHub issue](https://github.com/cjfravel-dev/ariadne/issues/new?template=bug_report.md) with:

- A clear description of the problem
- Steps to reproduce
- Expected vs actual behavior
- Spark version, Scala version, and Ariadne version

## Requesting Features

Open a [feature request](https://github.com/cjfravel-dev/ariadne/issues/new?template=feature_request.md) describing:

- The use case or problem you're trying to solve
- Your proposed solution (if any)

## Development Setup

### Prerequisites

- **Java 11** (`JAVA_HOME=/usr/lib/jvm/java-11-openjdk` or equivalent)
- **Maven 3.x**
- **scalafmt** (optional, for formatting)   

### Build and Test

```bash
# Run all tests (default: Spark 3.5 profile)
mvn test

# Run tests with Spark 3.4 profile
mvn test -P spark34

# Run a single test suite
mvn test -Dsuites="dev.cjfravel.ariadne.IndexTests"

# Package (produces shaded JAR)
mvn package
```

### Code Style

- Format with **scalafmt** (`runner.dialect = scala213`)
- Follow standard Scala naming: `camelCase` for methods/vals, `PascalCase` for types
- Use expression-oriented style (prefer `if/else` expressions over `return`)
- Use `import scala.collection.JavaConverters._` (not `CollectionConverters`)
- All public classes, traits, objects, and methods must have Scaladoc

## Submitting a Pull Request

1. Fork the repository and create your branch from `main`
2. Make your changes, ensuring tests pass (`mvn test`)
3. Update documentation if your changes affect public APIs
4. If you add a new version, ensure it appears in `README.md` (the build verifies this)
5. Open a pull request with a clear description of your changes

### Contributor License Agreement

By submitting a pull request, you agree to the terms of our [Contributor License Agreement](CLA.md). Your contribution will not be merged until the CLA is accepted.

## Questions?

Open a [GitHub Discussion](https://github.com/cjfravel-dev/ariadne/discussions) or email [ariadne-support@cjfravel.dev](mailto:ariadne-support@cjfravel.dev).
