import os
from contextlib import contextmanager

from pyspark.sql import SparkSession

from .log import logger


@contextmanager
def spark_session(app, master="local[*]", config=None):
    pre_spark = SparkSession.builder \
        .appName(app) \
        .enableHiveSupport() \
        .master(master) \

    if config is not None:
        for key, value in config.items():
            pre_spark = pre_spark.config(key, value)

    spark = pre_spark.getOrCreate()
    logger.info("Created Spark session")
    try:
        yield spark
    finally:
        logger.info("Stopping Spark Session")
        spark.stop()
