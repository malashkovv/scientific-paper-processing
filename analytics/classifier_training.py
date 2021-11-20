import os
import operator
import json

import numpy as np
import pandas as pd
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.types import Float

from pyspark.ml.classification import RandomForestClassifier, OneVsRest
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.tuning import TrainValidationSplit, ParamGridBuilder

from pyspark.ml.feature import RegexTokenizer, Word2Vec, StopWordsRemover, StringIndexer
from pyspark.ml import Pipeline

from sklearn.manifold import TSNE

from core.reporting_utils import create_reporting_engine
from core.log import logger
from core.spark_utils import spark_session


def create_pipeline():
    regex_tokenizer = RegexTokenizer(
        gaps=False,
        pattern='\\w+',
        inputCol='abstract',
        outputCol='abstract_token'
    )

    swr = StopWordsRemover(
        inputCol='abstract_token',
        outputCol='abstract_sw_removed'
    )

    word2vec = Word2Vec(
        minCount=5,
        inputCol='abstract_sw_removed',
        outputCol='abstract_embedding'
    )

    label_indexer = StringIndexer(
        inputCol="category",
        outputCol="indexed_category"
    )

    rf = RandomForestClassifier()

    ovr = OneVsRest(
        labelCol='indexed_category',
        featuresCol='abstract_embedding',
        predictionCol='predicted_indexed_category',
        classifier=rf
    )

    param_grid = ParamGridBuilder()\
        .addGrid(rf.numTrees, [75, 50])\
        .addGrid(word2vec.vectorSize, [20, 40])\
        .addGrid(rf.maxDepth, [10, 15])\
        .build()

    pipeline = Pipeline(stages=[regex_tokenizer, swr, word2vec, label_indexer, ovr])

    evaluator = MulticlassClassificationEvaluator(
        labelCol='indexed_category',
        predictionCol='predicted_indexed_category',
        metricName="accuracy"
    )

    tvs = TrainValidationSplit(
        estimator=pipeline,
        estimatorParamMaps=param_grid,
        evaluator=evaluator,
        trainRatio=0.8)

    return tvs


def tsne(embeddings, size=10, components=3):
    embeddings = np.vstack(embeddings.map(lambda x: x.toArray().reshape(size, )))
    tsne = TSNE(n_components=components)
    embedding_reduced_sample = tsne.fit_transform(embeddings)
    return pd.Series(embedding_reduced_sample.tolist())


config = {
    'spark.executor.memory': '2g'
}

if __name__ == '__main__':
    sql_engine = create_reporting_engine()
    with spark_session(config) as spark:
        df = spark.read.json(os.environ.get("PAPERS_PATH", "/data/papers"))\
            .select("abstract_distilled", "category")\
            .withColumnRenamed("abstract_distilled", "abstract")\
            .repartition(8)

        logger.info(f"Number of rows: {df.count()}")

        (training, test) = df.randomSplit([0.8, 0.2])

        logger.info("Creating and training model.")
        pipeline = create_pipeline()

        model = pipeline.fit(training.repartition(8))

        param_maps = [{f"{k.parent.split('_')[0]}.{k.name}": v for k, v in params.items()} 
                        for params in model.getEstimatorParamMaps()]

        metrics = pd.DataFrame(data={'param_maps': [json.dumps(i) for i in param_maps], 
                                     'metrics': model.validationMetrics})
        metrics.to_sql('metrics', con=sql_engine, if_exists='replace')

        logger.info("Param maps:")
        for params, metric in zip(param_maps, model.validationMetrics):
            logger.info(f"{params}: {metric}")

        logger.info("Preparing embeddings.")
        embeddings = model.transform(df).select("category", "abstract_embedding")
        embeddings.write.format("parquet") \
            .mode('overwrite').save("/data/embeddings")

        logger.info("Evaluating classifier.")
        evaluator = MulticlassClassificationEvaluator(
            labelCol='indexed_category',
            predictionCol='predicted_indexed_category',
            metricName="accuracy"
        )

        logger.info(f"Test accuracy = {evaluator.evaluate(model.transform(test))}")
        logger.info(f"Training accuracy = {evaluator.evaluate(model.transform(training))}")

        logger.info("Saving model.")
        model.bestModel.write().overwrite().save("/data/model/")

        embedding_sample = spark.read.parquet("/data/embeddings") \
            .sample(True, 0.05).toPandas()

        best_size = max(zip(param_maps, model.validationMetrics), key=operator.itemgetter(1))[0]['Word2Vec.vectorSize']
        
    logger.info(f"Vector size in choose model is: {best_size}")
    logger.info("Creating reduced embeddings with t-SNE.")
    embedding_sample['embedding_reduced_sample'] = tsne(
        embedding_sample.abstract_embedding, size=best_size)
    embedding_sample['category'] = pd.Categorical(embedding_sample['category'])
    embedding_sample['category_code'] = embedding_sample.category.cat.codes
    embedding_sample[['category', 'category_code', 'embedding_reduced_sample']] \
        .to_sql("embeddings", con=sql_engine, if_exists='replace', dtype={'embedding_reduced_sample': ARRAY(Float)})


