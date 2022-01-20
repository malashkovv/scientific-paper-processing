import numpy as np
import pandas as pd
from pyspark.ml import Pipeline
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.feature import RegexTokenizer, Word2Vec, StopWordsRemover
from pyspark.ml.tuning import TrainValidationSplit, ParamGridBuilder
from pyspark.sql import functions as f
from sklearn.manifold import TSNE

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
        outputCol='features'
    )

    clustering = KMeans(
        # Note: this argument is ignored when not "features" is used for some reason
        featuresCol='features',
        predictionCol='cluster'
    )

    param_grid = ParamGridBuilder()\
        .addGrid(clustering.k, [100, 75, 50])\
        .addGrid(word2vec.vectorSize, [20, 40])\
        .build()

    pipeline = Pipeline(stages=[regex_tokenizer, swr, word2vec, clustering])

    evaluator = ClusteringEvaluator(
        predictionCol='cluster',
        metricName='silhouette'
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
    # 'spark.executor.memory': '2g'
}


def train(settings):
    # sql_engine = create_reporting_engine()
    master = 'local[*]' ##f"spark://{settings.spark_master_host}:{settings.spark_master_port}"
    with spark_session(app="model_train", config=config, master=master) as spark:
        df = spark.read.json("s3a://dwh/streaming/paper_details/topic=source.dblp.paper-details/")\
            .select("abstract", "venue")\
            .filter(f.col("abstract").isNotNull())

        logger.info(f"Number of rows: {df.count()}")

        (training, test) = df.randomSplit([0.8, 0.2])

        logger.info("Creating and training model.")
        pipeline = create_pipeline()

        model = pipeline.fit(training.repartition(8))

        param_maps = [{f"{k.parent.split('_')[0]}.{k.name}": v for k, v in params.items()} 
                        for params in model.getEstimatorParamMaps()]

        # metrics = pd.DataFrame(data={'param_maps': [json.dumps(i) for i in param_maps],
        #                              'metrics': model.validationMetrics})
        # metrics.to_sql('metrics', con=sql_engine, if_exists='replace')

        logger.info("Param maps:")
        for params, metric in zip(param_maps, model.validationMetrics):
            logger.info(f"{params}: {metric}")

        logger.info("Preparing embeddings.")
        embeddings = model.transform(df).select("abstract", "venue", "cluster", "features")
        embeddings.write.format("parquet") \
            .mode('overwrite').save("s3a://dwh/papers_abstract_embeddings")

        logger.info("Saving model.")
        model.bestModel.write().overwrite().save("s3a://dwh/model/")

    #     embedding_sample = spark.read.parquet("s3a://dwh/papers_abstract_embeddings") \
    #         .sample(True, 0.05).toPandas()
    #
    #     best_size = max(zip(param_maps, model.validationMetrics), key=operator.itemgetter(1))[0]['Word2Vec.vectorSize']
    #
    # logger.info(f"Vector size in choose model is: {best_size}")
    # logger.info("Creating reduced embeddings with t-SNE.")
    # embedding_sample['embedding_reduced_sample'] = tsne(
    #     embedding_sample.abstract_embedding, size=best_size)
    # embedding_sample['venue'] = pd.Categorical(embedding_sample['category'])
    # embedding_sample[['category', 'embedding_reduced_sample']] \
    #     .to_sql("embeddings", con=sql_engine, if_exists='replace', dtype={'embedding_reduced_sample': ARRAY(Float)})


