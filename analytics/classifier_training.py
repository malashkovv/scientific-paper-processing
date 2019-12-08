from pyspark.ml.classification import RandomForestClassifier, OneVsRest
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

from pyspark.ml.feature import RegexTokenizer, Word2Vec, StopWordsRemover, StringIndexer
from pyspark.ml import Pipeline

from core.log import logger
from core.spark_utils import spark_session


def create_pipeline(embedding_size=10, trees=10):
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
        vectorSize=embedding_size,
        minCount=5,
        inputCol='abstract_sw_removed',
        outputCol='abstract_embedding'
    )

    label_indexer = StringIndexer(
        inputCol="category",
        outputCol="indexed_category"
    )

    rf = RandomForestClassifier(numTrees=trees)

    ovr = OneVsRest(
        labelCol='indexed_category',
        featuresCol='abstract_embedding',
        predictionCol='predicted_indexed_category',
        classifier=rf
    )

    return Pipeline(stages=[regex_tokenizer, swr, word2vec, label_indexer, ovr])


if __name__ == '__main__':
    with spark_session() as spark:

        df = spark.read.json("/data/papers")\
            .select("abstract_distilled", "category")\
            .withColumnRenamed("abstract_distilled", "abstract")

        logger.info(f"Number of rows: {df.count()}")

        (training, test) = df.randomSplit([0.7, 0.3])

        logger.info("Creating and training model.")
        pipeline = create_pipeline()

        model = pipeline.fit(training)

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
        logger.info(f"Test Error = {(1.0 - accuracy)}")

        logger.info("Saving model.")
        pipeline.write().overwrite().save("/data/model/")


