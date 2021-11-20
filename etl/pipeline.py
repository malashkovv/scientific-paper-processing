from pyspark.sql import functions as f
from pyspark.sql.types import StructType, StringType, DateType, TimestampType

from core.spark_utils import spark_session


config = {
    'spark.jars.packages': 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2'
}


def process_stream(settings):
    with spark_session(config) as spark:
        topic = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", ','.join(settings.kafka_urls)) \
            .option("subscribe", settings.paper_details_topic) \
            .option("startingOffsets", "earliest") \
            .load()

        schema = StructType() \
            .add("abstract", StringType()) \
            .add("category", StringType()) \
            .add("doi", StringType()) \
            .add("posted", DateType()) \
            .add("created_at", TimestampType()) \

        df = topic.select(f.col("timestamp").alias("processed"), f.from_json(f.col("value").cast(StringType()), schema).alias("blob"))

        exploded_df = df \
            .withColumn("abstract", f.col("blob.abstract"))\
            .withColumn("category", f.col("blob.category"))\
            .withColumn("doi", f.col("blob.doi"))\
            .withColumn("posted", f.col("blob.posted"))\
            .withColumn("created_at", f.col("blob.created_at"))\
            .withColumn("posted_year", f.year("posted"))\
            .withColumn("posted_month", f.month("posted"))\
            .drop("blob")

        write_stream = exploded_df.writeStream \
            .format("json") \
            .partitionBy("posted_year", "posted_month") \
            .option("format", "append") \
            .option("path", "/data/papers") \
            .option("checkpointLocation", "/data/papers_check") \
            .outputMode("append") \
            .start()

        write_stream.awaitTermination()