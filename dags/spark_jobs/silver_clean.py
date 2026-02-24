from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, current_timestamp


if __name__ == '__main__':
    spark = SparkSession.builder.appName('BronzeToSilver').getOrCreate()

    # These paths assume local Airflow home – adjust if needed
    bronze_path = '/home/vamsi/airflow/data/raw/yellow_tripdata_*.parquet'
    silver_path = '/home/vamsi/airflow/data/silver/yellow_trip_silver'

    print(f'Reading bronze data from: {bronze_path}')

    df = spark.read.parquet(bronze_path)

    df_clean = (
        df.filter(col('passenger_count') > 0)
          .withColumn('pickup_datetime', to_timestamp(col('tpep_pickup_datetime')))
          .withColumn('dropoff_datetime', to_timestamp(col('tpep_dropoff_datetime')))
          .withColumn('ingestion_ts', current_timestamp())
          .dropDuplicates(['VendorID', 'tpep_pickup_datetime', 'PULocationID'])
    )

    df_clean.write.mode('overwrite').parquet(silver_path)

    print(f'Silver layer written – {df_clean.count()} rows')

    spark.stop()
