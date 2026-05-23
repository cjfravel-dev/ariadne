<p align="center">
  <img src="docs/assets/ariadne.png" alt="Ariadne" width="420">
</p>

[![CI](https://github.com/cjfravel-dev/ariadne/actions/workflows/ci.yml/badge.svg)](https://github.com/cjfravel-dev/ariadne/actions/workflows/ci.yml)
[![Maven Central](https://img.shields.io/maven-central/v/dev.cjfravel/ariadne-spark35_2.12?label=Maven%20Central)](https://central.sonatype.com/artifact/dev.cjfravel/ariadne-spark35_2.12)
[![License](https://img.shields.io/badge/license-MIT%20%2B%20SaaS-blue)](LICENSE.md)
[![Spark](https://img.shields.io/badge/Apache%20Spark-3.5-E25A1C?logo=apachespark&logoColor=white)](https://spark.apache.org/)
[![Scala](https://img.shields.io/badge/Scala-2.12-DC322F?logo=scala&logoColor=white)](https://www.scala-lang.org/)

**Find the thread through your data lake.** Ariadne builds lightweight Delta-backed indexes over your Parquet, CSV, and JSON files so Spark joins read only the files they actually need.

📖 **[Full documentation →](https://cjfravel-dev.github.io/ariadne/)**

## Install

```xml
<!-- Spark 3.5 / Delta 3.2 -->
<dependency>
    <groupId>dev.cjfravel</groupId>
    <artifactId>ariadne-spark35_2.12</artifactId>
    <version>0.1.1-beta</version>
</dependency>
```

## Quick look

```scala
import dev.cjfravel.ariadne.Index
import dev.cjfravel.ariadne.Index._  // for df.join(index, ...) implicit

spark.conf.set("spark.ariadne.storagePath",
  s"abfss://$container@$account.dfs.core.windows.net/ariadne")

val index = Index("orders", orderSchema, "parquet")
index.addIndex("customer_id")
index.addFile(orderFiles: _*)
index.update

// Ariadne loads only the files that match
val result = customerDf.join(index, Seq("customer_id"), "inner")
```

## Documentation

Everything lives on the [docs site](https://cjfravel-dev.github.io/ariadne/):

- **[Getting Started](https://cjfravel-dev.github.io/ariadne/users/getting-started.html)** — install, configure, build your first index
- **[Index Types](https://cjfravel-dev.github.io/ariadne/users/index-types.html)** — regular, bloom, range, temporal, computed, exploded
- **[Usage Guide](https://cjfravel-dev.github.io/ariadne/users/usage.html)** — joins, column selection, Spark SQL catalog
- **[Configuration](https://cjfravel-dev.github.io/ariadne/users/configuration.html)** — every `spark.ariadne.*` setting
- **[Maintenance](https://cjfravel-dev.github.io/ariadne/users/maintenance.html)** — compact, vacuum, delete files
- **[Troubleshooting](https://cjfravel-dev.github.io/ariadne/users/troubleshooting.html)** — exceptions and common fixes
- **[Architecture](https://cjfravel-dev.github.io/ariadne/contributors/architecture.html)** — internals for contributors

## Examples

Interactive Jupyter notebooks demonstrating every feature are in [`examples/`](examples/). Run them with:

```bash
cd examples && docker compose up --build
```

Then open <http://localhost:8888>.

## Contributing

See the [contributor guide](https://cjfravel-dev.github.io/ariadne/contributors/index.html). All contributions are subject to the [Contributor License Agreement](https://cjfravel-dev.github.io/ariadne/contributors/cla.html).

## Security

To report a vulnerability, please follow the [security policy](https://cjfravel-dev.github.io/ariadne/contributors/security.html) — do not open a public issue.

## License

Ariadne is licensed under the MIT License with a SaaS provision. See [LICENSE.md](LICENSE.md).
