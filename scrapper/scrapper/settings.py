# -*- coding: utf-8 -*-
import os

BOT_NAME = 'scrapper'

SPIDER_MODULES = ['scrapper.spiders']
NEWSPIDER_MODULE = 'scrapper.spiders'

ROBOTSTXT_OBEY = True

DOWNLOAD_DELAY = 0.5

EXTENSIONS = {
    'scrapy_kafka_export.KafkaItemExporterExtension': 1,
}

KAFKA_EXPORT_ENABLED = True
KAFKA_BROKERS = os.environ.get("KAFKA_BROKERS", 'kafka1:9092,kafka2:9092').split(',')
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", 'science-papers')

BIORXIV_CATEGORIES = 'pharmacology-and-toxicology,cell-biology,cancer-biology,genetics,neuroscience,bioinformatics'.split(',')
BIORXIV_START_PAGE = int(os.environ.get('BIORXIV_START_PAGE', '0'))

BIORXIV_END_PAGE = os.environ.get('BIORXIV_END_PAGE', None)
if BIORXIV_END_PAGE is not None:
    BIORXIV_END_PAGE = int(BIORXIV_END_PAGE)
