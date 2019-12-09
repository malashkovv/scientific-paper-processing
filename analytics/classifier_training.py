from pyspark.ml.classification import RandomForestClassifier, OneVsRest
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.tuning import TrainValidationSplit, ParamGridBuilder

from pyspark.ml.feature import RegexTokenizer, Word2Vec, StopWordsRemover, StringIndexer
from pyspark.ml import Pipeline

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
        .addGrid(rf.numTrees, [100])\
        .addGrid(word2vec.vectorSize, [50])\
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

config = {
    'spark.executor.memory': '6g'
}

if __name__ == '__main__':
    with spark_session(config) as spark:

        df = spark.read.json("/data/papers")\
            .select("abstract_distilled", "category")\
            .withColumnRenamed("abstract_distilled", "abstract")\
            .repartition(8)

        logger.info(f"Number of rows: {df.count()}")

        (training, test) = df.randomSplit([0.8, 0.2])

        logger.info("Creating and training model.")
        pipeline = create_pipeline()

        model = pipeline.fit(training.repartition(8))

        logger.info("Param map:")
        for params, metric in zip(model.getEstimatorParamMaps(), model.validationMetrics):
            param_map = {f"{k.parent.split('_')[0]}.{k.name}": v for k, v in params.items()}
            logger.info(f"{param_map}: {metric}")

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

        accuracy = evaluator.evaluate(model.transform(test))
        logger.info(f"Test accuracy = {accuracy}")

        logger.info("Saving model.")
        model.bestModel.write().overwrite().save("/data/model/")


