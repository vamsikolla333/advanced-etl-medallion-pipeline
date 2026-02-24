from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.bash import BashOperator
from airflow.decorators import task


default_args = {
    'owner': 'vamsi',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


with DAG(
    dag_id='medallion_taxi_pipeline',
    default_args=default_args,
    description='Medallion ETL: Bronze → Silver → Gold (NYC Taxi example)',
    schedule='@daily',
    catchup=False,
    tags=['medallion', 'spark', 'etl'],
    max_active_runs=1,
) as dag:

    prepare_bronze = BashOperator(
        task_id='prepare_bronze',
        bash_command='echo "Assuming raw data is already in data/raw/"',
    )

    bronze_to_silver = SparkSubmitOperator(
        task_id='bronze_to_silver',
        application=os.path.expanduser('\~/airflow/spark_jobs/silver_clean.py'),
        conn_id='spark_default',
        conf={
            'spark.master': 'local[*]',
            'spark.executor.memory': '2g',
            'spark.driver.memory': '2g',
        },
        name='bronze_to_silver_job',
        verbose=True,
    )

    silver_to_gold = SparkSubmitOperator(
        task_id='silver_to_gold',
        application=os.path.expanduser('\~/airflow/spark_jobs/gold_aggregate.py'),
        conn_id='spark_default',
        conf={
            'spark.master': 'local[*]',
            'spark.executor.memory': '2g',
            'spark.driver.memory': '2g',
        },
        name='silver_to_gold_job',
        verbose=True,
    )

    @task(task_id='validate_gold')
    def check_gold_layer():
        try:
            from pyspark.sql import SparkSession
            spark = SparkSession.builder.appName('gold_validation').getOrCreate()
            df = spark.read.parquet(os.path.expanduser('\~/airflow/data/gold/daily_metrics'))
            count = df.count()
            print(f"Gold layer contains {count} rows")
            spark.stop()
        except Exception as e:
            raise ValueError(f"Gold validation failed: {str(e)}")

    validation = check_gold_layer()

    prepare_bronze >> bronze_to_silver >> silver_to_gold >> validation
