# Deprecation Roadmap

This document tracks planned deprecations and follow-up work for Ariadne.

## Spark 3.4 / Delta Lake 2.4 support

**Status:** supported (default Maven profile is `spark34`).

**Plan:** drop after Azure Synapse no longer pins Spark 3.4 in supported runtimes.

**Why it matters:** Delta 2.4's `DeltaMergeBuilder` does not expose
`withSchemaEvolution()`, so the only way to enable schema auto-merge on a
specific MERGE is via the session-level config
`spark.databricks.delta.schema.autoMerge.enabled`. Ariadne mutates this flag
through the `AriadneContextUser.withSchemaAutoMerge` helper, which carries a
documented thread-safety caveat (concurrent `Index.update` calls against
different indexes sharing a `SparkSession` can race on the flag). We avoid
`SparkSession.newSession()` because it is not reliably supported on all
hosted Spark platforms.

### When Delta 2.4 support is removed

1. Remove the `spark34` Maven profile from `pom.xml` and make `spark35` the
   default (or rename it).
2. At each call site that currently wraps a Delta MERGE in
   `withSchemaAutoMerge { ... }`, replace the wrapper with a per-merge
   `.withSchemaEvolution()` call on the `DeltaMergeBuilder`. Current sites:
   - `Index.scala` — file-size backfill MERGE in `update`
   - `IndexBuildOperations.scala` — staging-to-main consolidation MERGE
3. Delete `AriadneContextUser.withSchemaAutoMerge`.
4. Remove the thread-safety `@note` from `Index.update`'s scaladoc.
5. Update `README.md` to drop the dual-artifact (`ariadne-spark34_2.12`)
   coordinates.
