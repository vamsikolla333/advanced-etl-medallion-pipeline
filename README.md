# Advanced ETL Medallion Pipeline with Airflow & PySpark

End-to-end data pipeline demonstrating **Medallion Architecture** (Bronze → Silver → Gold) using:

- **Apache Airflow** for orchestration, scheduling, dependency management & monitoring
- **PySpark** for scalable distributed transformations
- Parquet storage (Delta Lake ready)

Example dataset: NYC Yellow Taxi trips (publicly available)

## Why this project?
- Bridges classic ETL with modern lakehouse patterns
- Shows real Airflow + Spark integration
- Production-like structure: modular jobs, validation, idempotency basics

## Architecture

1. **Bronze** → Raw ingestion (as-is data landing)
2. **Silver** → Data quality: cleaning, type casting, deduplication, metadata
3. **Gold** → Business value: daily aggregates by pickup zone (trip count, avg distance, revenue)

Orchestrated via a single Airflow DAG with SparkSubmitOperator tasks.

## Tech Stack

- Apache Airflow 2.10+
- PySpark 3.5+
- Parquet format
- Local execution (easily extensible to EMR, Databricks, Kubernetes)

## Quick Start (Local Development)

1. Install dependencies:
 ''bash
pip install -r requirements.txt
or manually:
pip install "apache-airflow[apache-spark]==2.10.5" pyspark==3.5.3

2.Add Spark connection in Airflow UI:
Admin → Connections
Conn Id: spark_default
Type: Spark
Extra (JSON): {"spark.master": "local[*]"}

3.Get sample data:

mkdir -p data/raw

curl -L -o data/raw/yellow_tripdata_2025-01.parquet \https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-01.parquet

4.Place DAG in $AIRFLOW_HOME/dags/ and spark_jobs/ folder next to it (or adjust paths).

5.Start Airflow → enable → trigger medallion_taxi_pipeline

# Output folders (relative to your home or project):
\~/airflow/data/silver/yellow_trip_silver/
\~/airflow/data/gold/daily_metrics/

# Production Notes / Next Steps:

1.Replace local paths with s3a:// or abfss://

2.Use Delta Lake format + MERGE for incremental processing

3.Add Great Expectations or custom checks

4.Deploy on MWAA / Astronomer / Kubernetes

5.Use Airflow Variables for paths & configs

Feel free to use / fork / improve!
