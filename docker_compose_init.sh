#!/usr/bin/env bash

docker-compose down -v

docker-compose up -d zookeeper

sleep 1s
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

docker-compose exec kafka1 kafka-topics.sh \
    --create --zookeeper zookeeper:2181 \
    --replication-factor 2 --partitions 3 \
    --topic source.dblp.paper-details

docker-compose up -d minio

sleep 2s
minio_init="mc alias set local http://minio:9000 minio_access_key minio_secret_key
mc mb local/dwh
"

docker-compose run minio-mc /bin/bash -c $minio_init

docker-compose up -d spark-master spark-worker1 spark-worker2
docker-compose up -d reporting-db

docker-compose up -d mariadb

sleep 3s
docker-compose run hive-metastore init
docker-compose up -d hive-metastore

docker-compose run driver spark-sql -f /dwh/warehouse.sql