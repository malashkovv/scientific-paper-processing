from pydantic import BaseSettings, validator


class Settings(BaseSettings):

    kafka_urls = "kafka1:9092,kafka2:9092"

    spark_master_host = "master"
    spark_master_port = "7077"

    paper_details_topic_pattern = "source.*.paper-details"

    @validator("kafka_urls")
    def split_kafka_urls(cls, v):
        return v.split(",")


settings = Settings()
