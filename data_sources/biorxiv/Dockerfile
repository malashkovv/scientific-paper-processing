FROM python:3.7

WORKDIR /usr/src/app/

RUN pip install Scrapy scrapy-kafka-export

COPY ./scrapy.cfg scrapy.cfg
COPY ./scrapper/ scrapper/

CMD ["scrapy", "crawl", "biorxiv"]