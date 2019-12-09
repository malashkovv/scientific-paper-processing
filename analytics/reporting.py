import os
import numpy as np
import pandas as pd
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.types import Float

from sklearn.manifold import TSNE

from core.log import logger
from core.spark_utils import spark_session
from core.reporting_utils import create_reporting_engine
from pyspark.sql import functions as f


def tsne(embeddings, size=10, components=3):
    embeddings = np.vstack(embeddings.map(lambda x: x.toArray().reshape(size, )))
    tsne = TSNE(n_components=components)
    embedding_reduced_sample = tsne.fit_transform(embeddings)
    return pd.Series(embedding_reduced_sample.tolist())


if __name__ == '__main__':
    sql_engine = create_reporting_engine()
    with spark_session() as spark:
        df = spark.read.json("/data/papers") \
            .select("abstract_distilled", "category") \
            .withColumnRenamed("abstract_distilled", "abstract")

        categories = df.groupBy("category").agg(f.count("*").alias("cnt")).toPandas()
        categories.to_sql('categories_counts', con=sql_engine, if_exists='replace')

        years = df.groupBy("posted_year").agg(f.count("*").alias("cnt")).toPandas()
        years.to_sql('year_counts', con=sql_engine, if_exists='replace')

        embedding_sample = spark.read.parquet("/data/embeddings") \
            .sample(True, 0.05).toPandas()

    logger.info("Creating reduced embeddings with t-SNE.")
    embedding_sample['embedding_reduced_sample'] = tsne(embedding_sample.abstract_embedding)
    embedding_sample['category'] = pd.Categorical(embedding_sample['category'])
    embedding_sample['category_code'] = embedding_sample.category.cat.codes
    embedding_sample[['category', 'category_code', 'embedding_reduced_sample']] \
        .to_sql("embeddings", con=sql_engine, if_exists='replace', dtype={'embedding_reduced_sample': ARRAY(Float)})
