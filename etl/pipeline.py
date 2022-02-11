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

        df = topic.select(f.col("topic"),
                          f.from_json(f.col("value").cast(t.StringType()), schema).alias("blob"))

        exploded_df = df.select(
            f.col("blob.title").alias("title",),
            f.col("blob.source_id").alias("source_id"),
            f.col("blob.abstract").alias("abstract"),
            f.col("blob.category").alias("category"),
            f.col("blob.doi").alias("doi"),
            f.col("blob.venue").alias("venue"),
            f.col("blob.posted").alias("posted"),
            f.col("blob.created_at").alias("created_at"),
            f.col("blob.authors").alias("authors"),
            f.col("topic"),
            f.to_date("blob.created_at").cast(t.DateType()).alias("created_date")
        )

        write_stream = exploded_df.writeStream \
            .format("delta") \
            .partitionBy("topic", "created_date") \
            .trigger(processingTime='20 seconds') \
            .outputMode("append") \
            .option("path", "s3a://dwh/data/paper_details") \
            .option("checkpointLocation", "s3a://dwh/streaming_checkpoint/paper_details") \
            .toTable("warehouse.paper_details")

        write_stream.awaitTermination()
