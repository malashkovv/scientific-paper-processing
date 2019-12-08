from pyspark.sql import functions as f
from pyspark.sql.types import StructType, StringType, DateType, TimestampType

from core.spark_utils import spark_session


config = {
    'spark.jars.packages': 'org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.4'
}

with spark_session(config) as spark:
    topic = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka1:9092,kafka2:9092") \
        .option("subscribe", "science-papers") \
        .option("startingOffsets", "earliest") \
        .load()

    schema = StructType() \
        .add("abstract", StringType()) \
        .add("category", StringType()) \
        .add("doi", StringType()) \
        .add("path", StringType()) \
        .add("posted", DateType()) \
        .add("parsed", TimestampType()) \

    df = topic.select(f.col("timestamp").alias("processed"), f.from_json(f.col("value").cast(StringType()), schema).alias("blob"))

    exploded_df = df \
        .withColumn("abstract", f.col("blob.abstract"))\
        .withColumn("category", f.col("blob.category"))\
        .withColumn("doi", f.col("blob.doi"))\
        .withColumn("path", f.col("blob.path"))\
        .withColumn("posted", f.col("blob.posted"))\
        .withColumn("parsed", f.col("blob.parsed"))\
        .withColumn("posted_year", f.year("posted"))\
        .withColumn("posted_month", f.month("posted"))\
        .withColumn("abstract_distilled", f.regexp_replace(f.col("abstract", "<[^<]+>", "")))\
        .drop("blob")

    write_stream = exploded_df.writeStream \
        .format("json") \
        .partitionBy("posted_year", "posted_month", "category") \
        .option("format", "append") \
        .option("path", "/data/papers") \
        .option("checkpointLocation", "/data/papers_check") \
        .outputMode("append") \
        .start()

    write_stream.awaitTermination()