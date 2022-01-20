from pyspark.sql import functions as f, types as t

from core.spark_utils import spark_session


config = {
    'spark.jars.packages': 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2',
    'spark.executor.memory': '1g',
    'spark.executor.instances': '4',
    'spark.executor.cores': '2'
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

        schema = t.StructType() \
            .add("title", t.StringType(), True) \
            .add("source_id", t.StringType(), True) \
            .add("abstract", t.StringType(), True) \
            .add("category", t.StringType(), True) \
            .add("doi", t.StringType(), True) \
            .add("venue", t.StringType(), True) \
            .add("posted", t.DateType()) \
            .add("created_at", t.TimestampType()) \
            .add("authors", t.ArrayType(t.StringType()), True)

        df = topic.select(f.col("topic").alias("topic"),
                          f.from_json(f.col("value").cast(t.StringType()), schema).alias("blob"))

        exploded_df = df \
            .withColumn("title", f.col("blob.title")) \
            .withColumn("source_id", f.col("blob.source_id")) \
            .withColumn("abstract", f.col("blob.abstract"))\
            .withColumn("category", f.col("blob.category"))\
            .withColumn("doi", f.col("blob.doi"))\
            .withColumn("venue", f.col("blob.venue"))\
            .withColumn("posted", f.col("blob.posted"))\
            .withColumn("created_at", f.col("blob.created_at")) \
            .withColumn("authors", f.col("blob.authors")) \
            .withColumn("created_date", f.to_date("blob.created_at").cast(t.DateType())) \
            .drop("blob")

        write_stream = exploded_df.writeStream \
            .format("json") \
            .partitionBy("topic", "created_date") \
            .option("format", "append") \
            .option("path", "s3a://dwh/streaming/paper_details") \
            .option("checkpointLocation", "s3a://dwh/streaming_checkpoint/paper_details") \
            .trigger(processingTime='20 seconds') \
            .outputMode("append") \
            .start()

        write_stream.awaitTermination()
