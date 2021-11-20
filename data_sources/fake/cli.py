import json
import datetime as dt

import typer
from kafka import KafkaProducer

from core.log import logger
from crawler import start_crawling
from config import settings

app = typer.Typer()


@app.command()
def crawl():
    start_crawling()


@app.command()
def seed(doi: str):
    logger.info(f"Seeding: {doi}")
    producer = KafkaProducer(bootstrap_servers=settings.kafka_urls)
    reference_paper = json.dumps({
        'doi': doi,
        'created_at': dt.datetime.utcnow().isoformat()
    })
    producer.send(settings.paper_ids_topic, value=reference_paper.encode("utf-8"))


if __name__ == "__main__":
    app()
