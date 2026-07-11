# Changelog

All notable changes to this project are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed

- Multi-column temporal joins now calculate every temporal rank against the original rows before filtering, preventing
  stale rows from being promoted by an earlier temporal deduplication pass.
- Index updates now migrate legacy exploded-field column names in both main and staging tables under the update lock
  before file-size backfill or consolidation can reintroduce an old column.
- Lock acquisition now retries only true file-exists contention; other filesystem `IOException`s propagate immediately
  instead of being converted into misleading lock-contention failures.
- Opening an existing index no longer rewrites `metadata.json`, preserving unknown fields for forward compatibility;
  metadata is written only when creation or explicit schema/read-option changes require it.

### Changed

- Release version checks now cover the changelog and include a shell regression test in the Maven test phase.

## [0.1.4-beta]

### Added

- Spark 4.1 build profile. A new `spark41` Maven profile cross-builds the library for
  Spark 4.1.0 / Delta 4.1.0 on Scala 2.13.16 and Java 21 — published as
  `ariadne-spark41_2.13` — alongside the default `spark35` profile (Spark 3.5.5 /
  Delta 3.2.1, Scala 2.12, Java 11, `ariadne-spark35_2.12`). Build a line with
  `mvn -Pspark41` or the default `mvn` invocation. Spark-major-specific internals
  (`Dataset.ofRows` moved to `org.apache.spark.sql.classic` in Spark 4) live under
  `src/main/${spark.compat}/scala`, and CI builds and tests both lines.
- Enforced code style: Scala main and test sources are formatted with scalafmt
  (`.scalafmt.conf`), linted with scalafix (`.scalafix.conf`: `RemoveUnused` +
  `OrganizeImports`), and checked with scalastyle (`scalastyle-config.xml`, adapted from
  Apache Spark). All run in the build (`validate`/`verify`) and CI, so a violation fails
  the build. Run `mvn scalafmt:format` to reformat.

## [0.1.3-beta]

### Changed

- Pre-release hardening. Dropped Spark 3.4 support and replaced the reflection-based schema
  evolution path with a native `withSchemaEvolution` write.

### Fixed

- Legacy indexes that predate the `file_size` column now migrate in place, without forcing a
  full index rebuild.

### Added

- CI workflow running build, test, and code coverage on every push and pull request.

### Documentation

- Migrated the documentation to a hand-authored HTML site and published the Scaladoc API
  reference under `docs/api/`.
- Comprehensive documentation audit, expanded test coverage above the 80% statement floor, and
  governance/license files in preparation for the public release.

## [0.1.1-beta]

### Fixed

- Beta bug fixes across index build and query paths.

## [0.1.0-beta]

### Added

- Spark SQL catalog integration. Every index under the configured `storagePath` is exposed as a
  Spark SQL table through `AriadneCatalog` and the `AriadneSparkExtension` optimizer rule, with no
  per-index registration. Ships with runnable example notebooks.

### Documentation

- Rewrote the README with a user-focused introduction and quick-start.

## [0.0.1-alpha44]

The alpha series established the core library. Notable milestones, newest first:

### Added

- Backfill support: a newly added indexed column is populated from existing files without a full
  rebuild (`0.0.1-alpha44`).
- Per-index locking with auto-heal for concurrent writers (`0.0.1-alpha42`).
- Spark 3.5 / Delta support and multi-version Spark/Delta selection via Maven profiles
  (`0.0.1-alpha40`); temporal index (`0.0.1-alpha39`).
- Index types over datalake files: regular, computed (Spark SQL expression), temporal, range,
  bloom-filter, and exploded-field indexes, all backed by Delta storage.
- Index-to-DataFrame join that prunes to only the files matching the join values, plus management
  operations for existing indexes and logging support.

### Changed

- Join-path performance: removed redundant joins, dropped unnecessary `collect`/`distinct`/`cache`,
  and added repartition and debug-logging controls.
- Dependency shading of Guava and Gson under `dev.cjfravel.ariadne.shaded` with tightened
  `provided` scopes, and a switch from circe to Gson for JSON handling.
