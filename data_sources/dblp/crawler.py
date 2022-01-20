import json
import time
import random
import datetime as dt


from kafka import KafkaProducer

from config import settings
from core.log import logger


def generate_record(row):
    return {
        'source_id': row.get('id'),
        'abstract': row.get('abstract'),
        'authors': row.get('authors'),
        'venue': row.get('authors'),
        'title': row.get('title'),
        'posted': dt.date(int(row['year']), 1, 1).isoformat(),
        'created_at': dt.datetime.utcnow().isoformat()
    }


files = [f'/data/dblp-ref/dblp-ref-{i}.json' for i in range(1)]


def start_crawling():
    producer = KafkaProducer(bootstrap_servers=settings.kafka_urls)
    for file_name in files:
        with open(file_name) as fl:
            for row in fl:
                time.sleep(0.1)
                record = generate_record(json.loads(row))
                logger.info(f"Processing paper: {record['source_id']}")
                producer.send(settings.paper_details_topic, json.dumps(record).encode("utf-8"))
