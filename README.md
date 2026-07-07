# Data Ingestion Pipelines into an Azure Data Lakehouse

Data ingestion coursework: batch and streaming ingestion patterns into an Azure Data Lakehouse (Databricks + ADLS + Delta Lake), plus a Kafka Python producer client.

## Contents

- `entrega/` — final graded activity: batch and streaming data-generation/ingestion notebooks, architecture diagram and full report.
- `final-project/` — the final assignment ("motor de ingesta"): a configurable ingestion engine with iterative versions (`motor_ingesta_v2` through `v10`) evolving from a simple batch loader to a more complete pipeline, plus schema and Kafka/Azure ingestion config.
- `batch-tarea/` — an earlier, simpler batch ingestion exercise (aggregations, daily flow).
- `ejercicios/` — weekly exercises: file formats (Parquet, Delta Lake), file-based ingestion with Databricks Autoloader (JSON/images, full and incremental), event-based ingestion (JSON/Avro, single/multiplex), an ingestion engine exercise, and a MovieLens medallion pipeline (bronze/silver/gold).
- `streaming-client/` — a minimal Confluent Cloud Kafka Python producer/consumer client. `client.properties.example` uses placeholder credentials — copy it to `client.properties` and fill in your own Confluent Cloud API key/secret.

**Nota:** se han excluido del repositorio los ficheros con credenciales reales encontrados en el material original (`azure_config.json`, `schema_registry.json`, una clave de API de Confluent en texto plano) y los `wheel`/entornos virtuales.

## Stack

Azure Databricks, ADLS Gen2, Delta Lake, Apache Kafka / Confluent Cloud, Python, PySpark.
