# Advanced ETL Medallion Pipeline – Airflow + PySpark

Hands-on project implementing **Medallion Architecture** (Bronze → Silver → Gold):

- **Apache Airflow** → orchestration, scheduling, monitoring, retries
- **PySpark** → distributed cleaning & aggregation
- Storage: Parquet files (local dev; production-ready for Delta/S3)

Uses NYC Yellow Taxi trip data as example.

## Project Goals
- Demonstrate end-to-end ETL with clear quality layers
- Show Airflow + Spark integration
- Keep it simple but production-pattern friendly

## Architecture
1. Bronze: raw landing zone (as-is data)
2. Silver: cleaned, typed, deduplicated, enriched
3. Gold: business-ready aggregates (daily metrics by zone)

## Local Setup

1. Install dependencies:
 ''bash
pip install -r requirements.txt
# or manually:
# pip install "apache-airflow[apache-spark]==2.10.5" pyspark==3.5.3

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

Output folders (relative to your home or project):
\~/airflow/data/silver/yellow_trip_silver/
\~/airflow/data/gold/daily_metrics/

Production Notes / Next Steps:
1.Replace local paths with s3a:// or abfss://
2.Use Delta Lake format + MERGE for incremental processing
3.Add Great Expectations or custom checks
4.Deploy on MWAA / Astronomer / Kubernetes
5.Use Airflow Variables for paths & configs
Feel free to use / fork / improve!
