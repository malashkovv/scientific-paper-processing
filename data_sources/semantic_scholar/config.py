from pydantic import BaseSettings, validator


class Settings(BaseSettings):

    kafka_urls = "kafka1:9092,kafka2:9092"

    api_call_interval = "2"

    paper_ids_topic = "source.semantic-scholar.paper-ids"
    paper_details_topic = "source.semantic-scholar.paper-details"

    @validator("kafka_urls")
    def split_kafka_urls(cls, v):
        return v.split(",")

    @validator("api_call_interval")
    def convert_api_call_interval(cls, v):
        return float(v)


settings = Settings()
