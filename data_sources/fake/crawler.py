import json
import time
import random
import datetime as dt


from faker import Faker
from kafka import KafkaConsumer, KafkaProducer

from core.log import logger

from config import settings


fake = Faker()


def generate_doi():
    return fake.numerify(text='##.####/######')


def generate_record(doi):
    return {
        'doi': doi,
        'title': fake.sentence(nb_words=10),
        'abstract': fake.paragraph(nb_sentences=15),
        'posted': fake.date_between(start_date='-50y').isoformat(),
        'category': f"test_category_{fake.pyint(max_value=20)}",
        'created_at': dt.datetime.utcnow().isoformat()
    }


def start_crawling():
    consumer = KafkaConsumer(
        settings.paper_ids_topic,
        bootstrap_servers=settings.kafka_urls,
        group_id='scrapper'
    )
    producer = KafkaProducer(bootstrap_servers=settings.kafka_urls)

    logger.info("Initialised")
    for paper_id in consumer:
        time.sleep(settings.api_call_interval)
        paper_doi = json.loads(paper_id.value.decode('utf-8'))['doi']
        logger.info(f"Processing paper: {paper_doi}")
        producer.send(settings.paper_details_topic, json.dumps(generate_record(paper_doi)).encode("utf-8"))
        for i in range(random.randint(5, 15)):
            reference_paper = json.dumps({
                'doi': generate_doi(),
                'created_at': dt.datetime.utcnow().isoformat()
            })
            logger.info(f"Sending reference paper: {reference_paper}")
            producer.send(settings.paper_ids_topic, value=reference_paper.encode("utf-8"))
