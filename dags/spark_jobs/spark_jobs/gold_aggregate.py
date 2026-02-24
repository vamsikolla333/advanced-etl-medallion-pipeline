from pyspark.sql import SparkSession
from pyspark.sql.functions import date_trunc, count, avg, sum as _sum


if __name__ == '__main__':
    spark = SparkSession.builder.appName('SilverToGold').getOrCreate()

    silver_path = '/home/vamsi/airflow/data/silver/yellow_trip_silver'
    gold_path = '/home/vamsi/airflow/data/gold/daily_metrics'

    print(f'Reading silver data from: {silver_path}')

    df = spark.read.parquet(silver_path)

    df_gold = (
        df.withColumn('pickup_date', date_trunc('day', col('pickup_datetime')))
          .groupBy('pickup_date', 'PULocationID')
          .agg(
              count('*').alias('trip_count'),
              avg('trip_distance').alias('avg_distance_km'),
              _sum('total_amount').alias('total_revenue_usd')
          )
    )

    df_gold.write.mode('overwrite').partitionBy('pickup_date').parquet(gold_path)

    print(f'Gold layer written – {df_gold.count()} rows')

    spark.stop()
