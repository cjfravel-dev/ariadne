# Changelog

All notable changes to this project are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.9-beta]

### Added

- An opt-in, version-controlled pre-commit hook can run the same scalafmt, scalafix, and scalastyle gates as CI for
  staged Scala changes. The installer configures a repository-local hooks path, and the hook protects partially staged
  or unrelated files from being swept into a commit when formatting rewrites sources.

### Changed

- Bloom-filter queries now probe serialized filters on executors and collect only matching filenames instead of
  collecting every filter on the driver. Probe collection is bounded from each filter's false-positive rate, and large
  probe sets safely skip the optimization rather than truncating values and risking false negatives.
- Bloom filters and large indexes now stream values into their final representation instead of materializing unbounded
  per-file arrays. Large-file classification uses the same distinct-count semantics as each index builder, preserving
  files with empty exploded arrays and avoiding stale or missing large-index rows when cardinality changes.
- DataFrame range-index queries choose between per-value pruning and a bounding box from one bounded collection,
  eliminating a separate full distinct-count Spark job.
- The shaded Guava dependency is updated from 33.6.0-jre to 33.7.1-jre.

### Fixed

- Range queries with up to 1,000 probe values no longer build a linear-depth predicate tree that can overflow Delta
  Lake's recursive data-skipping analysis. Predicates are now combined as a balanced tree.
- Files whose indexed cardinality falls below `largeIndexLimit` no longer retain stale rows in the corresponding
  `large_indexes` table after re-indexing.
- Files are no longer omitted from an update when every indexed column is an exploded field and all of their arrays are
  null or empty.
- Large-index analysis no longer undercounts a regular or exploded index when another exploded array is null or empty.
  The previous mismatch could null the inline index without writing its values to `large_indexes`, causing query-time
  data loss. Each exploded configuration is now counted independently.
- Temporal index cardinality now includes the stored null group, preventing a file at the large-index boundary from
  being classified one value below its actual stored size.

## [0.1.8-beta]

### Fixed

- `join` and `joinDf` no longer ignore an active column selection when the file lookup matches no files. The no-match
  branch built its empty DataFrame from the full stored schema, so the result schema depended on whether any data
  matched: the same code returned different columns depending only on whether a key happened to match. This was
  visible on non-empty results too, since an outer join in the DataFrame-left direction null-padded the unselected
  columns onto otherwise valid left rows. The empty result is now derived from the same read path as the populated
  one, so the schema is identical either way.

- Index type mutual exclusivity is now enforced symmetrically. `addIndex` did not check computed or exploded field
  indexes, and `addBloomIndex`, `addTemporalIndex` and `addRangeIndex` did not check exploded field indexes, so whether
  a conflict was rejected depended on registration order: `addExplodedFieldIndex("items", "id", "x")` followed by
  `addTemporalIndex("x", ...)` was silently accepted, while the reverse order threw. A column carrying two index types
  produces a wrong result set at query time rather than an error, because deduplication partitions by a column the
  exploded projection also writes. All six `add*Index` methods now share one exclusivity check covering every type.

- Temporal indexes now support nested timestamp columns such as `meta.updatedAt`. `addTemporalIndex` accepted them,
  because dotted paths resolve against the schema, but `update` then aborted with Spark's `UNRESOLVED_COLUMN` analysis
  error: selecting a nested path flattens it to its leaf name, so aggregating by the original dotted path could not
  resolve. The timestamp projection is now aliased before aggregation, using a working name derived per configuration
  so it cannot collide with the indexed value column itself. Top-level timestamp columns are unaffected.

### Changed

- Nested (dotted) columns are rejected up front by `addIndex`, `addBloomIndex`, `addRangeIndex`, `addComputedIndex`,
  `addExplodedFieldIndex` and the value column of `addTemporalIndex`, with an `IllegalArgumentException` explaining the
  restriction. An indexed value column is persisted under its own name and read back with `col(name)`, so a dotted path
  was written as a literal column name but read as nested field access. Such a configuration could never be built or
  queried; it was previously accepted and persisted, then failed later during `update` with an opaque analysis error.
  No working configuration is affected, since these indexes could not be built before.

## [0.1.7-beta]

### Fixed

- Temporal deduplication no longer fails when `select()` omits a temporal index's timestamp column. The read pipeline
  pruned columns before `joinDf` applied deduplication, so a join such as `index.select("Id", "Value").join(df,
  Seq("Id"))` against a temporal index on `("Id", "UpdatedAt")` aborted with Spark's `UNRESOLVED_COLUMN` analysis error.
  The timestamp column is now read transparently for ranking and dropped afterward, leaving the caller's projection
  unchanged. Joins without `select()`, selections that already include the timestamp column, and the catalog read paths
  are unaffected.
- The Maven Central publish workflow now imports the GPG signing key in a single `setup-java` step. Importing it in both
  the Java 11 and Java 21 steps registered duplicate post-job cleanups that raced to delete the key, marking otherwise
  successful releases as failed (`gpg` exit 2). Release artifacts were unaffected.

## [0.1.6-beta]

### Fixed

- The `file_size` storage migration now evolves the Delta schema with a metastore-free `mergeSchema` append instead of a
  path-based `ALTER TABLE ADD COLUMNS`. The SQL DDL forced Hive metastore initialization on Azure Synapse, which failed
  with `IllegalArgumentException: null path` and aborted `update`. The new path works identically on Delta 3.2 / Spark 3.5
  and Delta 4.1 / Spark 4.1.
- Maven Central post-publication smoke checks now resolve artifacts from a neutral directory instead of parsing
  Ariadne's profile-interpolated source POM.

## [0.1.5-beta]

### Added

- Automated coordinated Maven Central releases: a published GitHub Release signs and validates the
  Spark 3.5 and Spark 4.1 artifacts independently, then publishes only after both deployments pass
  Central validation.
- Contributor Covenant 2.1 governance, citation metadata, structured issue forms with private security routing, and a
  PR contract requiring RED/GREEN and persisted-compatibility evidence.
- Repository-readiness contracts now enforce exact coordinated release metadata, canonical generated Scaladoc, and
  shaded JAR contents for both supported Spark artifacts without invoking publication.
- A tested persisted-index support matrix now documents alpha37+ release cohorts, migration checkpoints, failure
  behavior, and when operators must upgrade, retry, restore, or rebuild.
- Maven now enforces dependency convergence for both Spark profiles, while grouped weekly Dependabot updates keep
  Spark/Delta, shaded runtime dependencies, tests, and build plugins reviewable without crossing Spark or Scala majors.
- Scoverage now enforces a 73% total branch-coverage floor alongside the existing 80% statement-coverage floor.
- Explicit `metadata_version` and `storage_format_version` markers with lock-safe, ordered, idempotent migration
  preflight for the alpha37 compatibility floor. Queries, catalog scans, metadata mutations, updates, deletes,
  compaction, and vacuum migrate `file_size` and exploded-field storage before use; future versions fail explicitly.
- A 52 KB physical compatibility fixture generated by the real `0.0.1-alpha-37` tag now verifies query-triggered
  migration, file-size backfill, exploded alias migration, metadata versioning, and idempotent reopen behavior.

### Fixed

- Interrupted updates now recover stale staging even when no new files are pending. New staging rows carry batch/time
  ordering metadata; consolidation selects the latest complete row deterministically, while legacy rows use a stable
  completeness/hash fallback.
- Intelligent batching now keeps files together when their aggregate distinct count equals `largeIndexLimit`, avoiding
  unnecessary staging writes and consolidation cycles.
- Catalog `list`, `exists`, `get`, and `describe` now consistently expose only metadata-backed indexes, while `remove`
  still cleans directory-only and FileList-only recovery artifacts.
- Multi-column temporal joins now calculate every temporal rank against the original rows before filtering, preventing
  stale rows from being promoted by an earlier temporal deduplication pass.
- Index updates now migrate legacy exploded-field column names in both main and staging tables under the update lock
  before file-size backfill or consolidation can reintroduce an old column.
- Lock acquisition now retries only true file-exists contention; other filesystem `IOException`s propagate immediately
  instead of being converted into misleading lock-contention failures.
- Opening an existing index no longer rewrites `metadata.json`, preserving unknown fields for forward compatibility;
  metadata is written only when creation or explicit schema/read-option changes require it.

### Changed

- Runtime profiles now match their managed platforms: the deployed Synapse runtime uses Spark 3.5.5, Delta 3.2.1,
  Scala 2.12.17, and Java 11; Fabric Runtime 2.0 uses Spark 4.1.1, Delta 4.1.0, Scala 2.13.17, and Java 21. Runtime-owned dependency
  families are excluded from independent Dependabot upgrades.
- Dependabot also excludes Scala-2.13-only Scalafmt plugin releases and Java-17-only Scoverage releases that cannot
  satisfy the Synapse Scala 2.12 / Java 11 build.
- FileList duplicate detection now uses a distributed Delta merge, and legacy `file_size` migration processes bounded
  batches instead of materializing every tracked filename in driver memory.
- The examples image pins both base images by digest and verifies Spark SHA-512 plus Delta JAR SHA-256 checksums before
  extraction or use.
- Release version checks now cover the changelog and include a shell regression test in the Maven test phase.
- CI now uses current Node 24-based GitHub action majors, scopes pull-request write permission to the coverage job, and
  skips coverage publishing steps for fork pull requests with read-only tokens.
- Spark tests now use the four cores available on public GitHub-hosted Linux runners while reducing tiny fixture
  shuffles and Delta snapshot reconstruction to one partition. Spark 3.5 coverage CI runs one instrumented lifecycle
  followed by `verify -DskipTests`, eliminating a duplicate 24-minute suite execution while retaining formatting,
  lint, style, coverage, and packaging gates.

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
