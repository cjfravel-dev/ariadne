# Ariadne Examples

Interactive Jupyter notebooks demonstrating all Ariadne features using a Scala kernel (spylon-kernel) backed by Apache Spark.

## Quick Start

```bash
# From the examples/ directory:
docker compose up --build
```

Open http://localhost:8888 in your browser. No token required.

## What's Included

### Sample Data (`data/`)

| File | Description |
|------|-------------|
| `customers_west.csv`, `customers_east.csv` | Customer records split by region |
| `orders_q1.csv`, `orders_q2.csv` | Order transactions by quarter |
| `events.json` | User events with nested tags and participant arrays |
| `sensor_readings_jan.csv`, `sensor_readings_feb.csv` | IoT sensor time-series data |

### Notebooks (`notebooks/`)

| Notebook | Description |
|----------|-------------|
| **01 Index Types** | Every index type with examples: regular, bloom, computed, temporal, range, exploded field |
| **02 Programmatic API** | Full Scala API walkthrough: create, query, join, manage indexes |
| **03 SQL Catalog** | Spark SQL access: SHOW TABLES, SELECT, WHERE, JOIN optimization |

## How It Works

The Docker image:
1. Builds the Ariadne JAR from source (multi-stage build)
2. Installs Spark 3.5.5 + Delta Lake 3.2.1
3. Installs Jupyter with a Scala kernel (spylon-kernel)
4. Places the shaded Ariadne JAR in Spark's classpath

Each notebook's first cell uses `%%init_spark` to configure the SparkSession with the Ariadne catalog and extensions **before** the SparkContext is created (required for `spark.sql.extensions`).

## Rebuilding

If you modify Ariadne source code and want to test changes:

```bash
docker compose up --build --force-recreate
```

Or build the JAR manually and copy it:

```bash
cd ..
mvn package -DskipTests
cp target/ariadne-spark35_2.12-*-shaded.jar examples/
# Then update the Dockerfile COPY to use the local JAR
```
