from pyspark.sql import functions as f
from pyspark.sql.types import StructType, StringType, DateType, TimestampType

from core.spark_utils import spark_session


config = {
    'spark.jars.packages': 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2'
}


def process_stream(settings):
    master = f"spark://{settings.spark_master_host}:{settings.spark_master_port}"
    with spark_session(app="etl", config=config, master=master) as spark:
        topic = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", ','.join(settings.kafka_urls)) \
            .option("subscribePattern", settings.paper_details_topic_pattern) \
            .option("startingOffsets", "earliest") \
            .load()

        schema = StructType() \
            .add("abstract", StringType()) \
            .add("category", StringType()) \
            .add("doi", StringType()) \
            .add("posted", DateType()) \
            .add("created_at", TimestampType()) \

        df = topic.select(f.col("topic").alias("topic"),
                          f.from_json(f.col("value").cast(StringType()), schema).alias("blob"))

        exploded_df = df \
            .withColumn("abstract", f.col("blob.abstract"))\
            .withColumn("category", f.col("blob.category"))\
            .withColumn("doi", f.col("blob.doi"))\
            .withColumn("posted", f.col("blob.posted"))\
            .withColumn("created_at", f.col("blob.created_at")) \
            .withColumn("created_date", f.to_date("blob.created_at").cast(DateType())) \
            .drop("blob")

        write_stream = exploded_df.writeStream \
            .format("json") \
            .partitionBy("topic", "created_date") \
            .option("format", "append") \
            .option("path", "s3a://dwh/streaming/pape_details") \
            .option("checkpointLocation", "s3a://dwh/streaming_checkpoint/pape_details") \
            .trigger(processingTime='20 seconds') \
            .outputMode("append") \
            .start()

        write_stream.awaitTermination()
