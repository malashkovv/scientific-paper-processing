from pydantic import BaseSettings, validator


class Settings(BaseSettings):

    spark_master_host = "master"
    spark_master_port = "7077"


settings = Settings()
