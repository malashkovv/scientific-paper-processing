#!/usr/bin/env bash

docker-compose down -v

docker-compose up -d zookeeper
docker-compose up -d kafka1 kafka2

sleep 1s

docker-compose exec kafka1 kafka-topics.sh \
    --create --zookeeper zookeeper:2181 \
    --replication-factor 2 --partitions 3 \
    --topic source.fake.paper-ids

docker-compose exec kafka1 kafka-topics.sh \
    --create --zookeeper zookeeper:2181 \
    --replication-factor 2 --partitions 3 \
    --topic source.fake.paper-details

docker-compose up -d minio

docker-compose up -d spark-master spark-worker1 spark-worker2