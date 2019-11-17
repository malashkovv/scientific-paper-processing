from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import StructType, StringType, DateType, TimestampType

spark = SparkSession.builder \
    .appName('science-papers-etl') \
    .master("spark://spark-master:7077") \
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.4') \
    .getOrCreate()

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
    .drop("blob") #\
    # .withWatermark("processed", "5 minutes") \
    # .dropDuplicates(["doi", "processed"])

write_stream = exploded_df.writeStream \
    .format("csv") \
    .partitionBy("posted_year", "posted_month", "category") \
    .option("format", "append") \
    .option("path", "/data/result") \
    .option("checkpointLocation", "/data/check") \
    .outputMode("append") \
    .start()

write_stream.awaitTermination()
