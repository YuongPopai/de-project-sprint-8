from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, lit, unix_timestamp, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, LongType

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 2, 19),
    'retries': 1,
}

def process_data():
    spark_jars_packages = ",".join(
        [
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0",
            "org.postgresql:postgresql:42.4.0",
        ]
    )

    spark = SparkSession.builder \
        .appName("RestaurantSubscribeStreamingService") \
        .config("spark.sql.session.timeZone", "UTC") \
        .config("spark.jars.packages", spark_jars_packages) \
        .getOrCreate()

    restaurant_read_stream_df = spark.readStream \
        .format('kafka') \
        .option('kafka.bootstrap.servers', 'rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091') \
        .option('kafka.security.protocol', 'SASL_SSL') \
        .option('kafka.sasl.jaas.config', 'org.apache.kafka.common.security.scram.ScramLoginModule required username="login" password="password";') \
        .option('kafka.sasl.mechanism', 'SCRAM-SHA-512') \
        .option('subscribe', 'aksenov_nikita_in') \
        .load()

    incoming_message_schema = StructType([
        StructField("restaurant_id", StringType()),
        StructField("adv_campaign_id", StringType()),
        StructField("adv_campaign_content", StringType()),
        StructField("adv_campaign_owner", StringType()),
        StructField("adv_campaign_owner_contact", StringType()),
        StructField("adv_campaign_datetime_start", LongType()),
        StructField("adv_campaign_datetime_end", LongType()),
        StructField("datetime_created", LongType())
    ])

    current_timestamp_utc = int(round(unix_timestamp(current_timestamp())))

    filtered_read_stream_df = restaurant_read_stream_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), incoming_message_schema).alias("data")) \
        .select("data.*") \
        .filter((col("adv_campaign_datetime_start") <= current_timestamp_utc) & 
                (col("adv_campaign_datetime_end") >= current_timestamp_utc))

    subscribers_restaurant_df = spark.read \
        .format('jdbc') \
        .option('url', 'jdbc:postgresql://rc1a-fswjkpli01zafgjm.mdb.yandexcloud.net:6432/de') \
        .option('driver', 'org.postgresql.Driver') \
        .option('dbtable', 'subscribers_restaurants') \
        .option('user', 'student') \
        .option('password', 'de-student') \
        .load()

    result_df = filtered_read_stream_df.join(subscribers_restaurant_df, "restaurant_id") \
        .withColumn("datetime_created", lit(current_timestamp_utc))

    result_df.writeStream \
        .foreachBatch(foreach_batch_function) \
        .start() \
        .awaitTermination()

dag = DAG(
    'restaurant_subscribe_streaming_service',
    default_args=default_args,
    description='A DAG for processing restaurant subscription data from Kafka',
    schedule_interval=None,
)

process_task = PythonOperator(
    task_id='process_data',
    python_callable=process_data,
    dag=dag,
)

process_task
