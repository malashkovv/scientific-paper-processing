import json
import time
import uuid
import datetime as dt

from kafka import KafkaConsumer, KafkaProducer

from core.log import logger

from config import settings


def start_crawling():
    consumer = KafkaConsumer(
        settings.paper_ids_topic,
        bootstrap_servers=settings.kafka_urls,
        group_id='scrapper'
    )
    producer = KafkaProducer(bootstrap_servers=settings.kafka_urls)

    logger.info("Initialised")
    for paper in consumer:
        time.sleep(settings.api_call_interval)
        message_value = json.loads(paper.value.decode('utf-8'))
        logger.info(f"Processing paper: {message_value}")
        # producer.send(settings.paper_details_topic, "")
        for i in range(5):
            reference_paper = json.dumps({
                'id': str(uuid.uuid4()),
                'created_at': dt.datetime.utcnow().isoformat()
            })
            logger.info(f"Sending reference paper: {reference_paper}")
            producer.send(settings.paper_ids_topic, value=reference_paper.encode("utf-8"))
