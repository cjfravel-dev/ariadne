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
- **scalafmt** (required — all code must be formatted before submitting)   

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
4. **Do not change the version in `pom.xml` or `README.md`** — versioning is managed by the maintainer
5. Open a pull request with a clear description of your changes

### Contributor License Agreement

By submitting a pull request, you agree to the terms of our [Contributor License Agreement](CLA.md). Your contribution will not be merged until the CLA is accepted.

## AI-Assisted Contributions

We welcome contributions that use AI tools (Copilot, ChatGPT, etc.) to help write code, tests, or documentation. However, every pull request must reflect genuine human understanding:

- **You must review and comprehend all AI-generated code before submitting.** If you can't explain what it does and why, it's not ready.
- **Purely AI-generated PRs with no human review will be closed.** We need contributors who can discuss their changes, respond to feedback, and reason about edge cases.
- **Attribute AI assistance honestly.** There's no stigma — just be transparent.

The goal is quality contributions from people who understand what they're submitting, regardless of how they got there.

## Questions?

Open a [GitHub Discussion](https://github.com/cjfravel-dev/ariadne/discussions).
