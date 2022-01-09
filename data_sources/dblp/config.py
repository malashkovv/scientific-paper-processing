from pydantic import BaseSettings, validator


class Settings(BaseSettings):

    kafka_urls = "kafka1:9092,kafka2:9092"
    paper_details_topic = "source.dblp.paper-details"

    @validator("kafka_urls")
    def split_kafka_urls(cls, v):
        return v.split(",")


settings = Settings()
