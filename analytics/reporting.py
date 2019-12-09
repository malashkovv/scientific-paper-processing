import os

from sklearn.manifold import TSNE

from core.log import logger
from core.spark_utils import spark_session
from core.reporting_utils import create_reporting_engine
from pyspark.sql import functions as f


if __name__ == '__main__':
    sql_engine = create_reporting_engine()
    with spark_session() as spark:
        df = spark.read.json("/data/papers") \
            .select("abstract_distilled", "category", "posted") \
            .withColumnRenamed("abstract_distilled", "abstract")

        categories = df.groupBy("category").agg(f.count("*").alias("cnt")).toPandas()
        categories.to_sql('categories_counts', con=sql_engine, if_exists='replace')

        years = df.withColumn("posted_year", f.year("posted"))\
            .groupBy("posted_year").agg(f.count("*").alias("cnt")).toPandas()
        years.to_sql('year_counts', con=sql_engine, if_exists='replace')