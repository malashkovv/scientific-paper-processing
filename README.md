# scientific-paper-processing

# Scrapping and streaming

## Kafka setup

```bash
docker-compose up -d zookeeper kafka1 kafka2 
```

After kafka is up and running, make sure that topic is created 
```bash
docker-compose exec kafka1 kafka-topics.sh \
    --create --zookeeper zookeeper:2181 \
    --replication-factor 2 --partitions 3 \
    --topic science-papers
```

Boot up scrappers:
```bash
docker-compose up -d scrapper1 scrapper2
```

## Spark

You need to make sure that Spark cluster is running

```bash
docker-compose up -d spark-master spark-worker1 spark-worker2
```

Then run streaming job:
```bash
docker-compose run etl
```

# ML

## Spark

You need to make sure that Spark cluster is running

```bash
docker-compose up -d spark-master spark-worker1 spark-worker2
```

Then run
```bash
docker-compose run ml
```

# Dashboard

Run 

```bash
docker-compose up -d dashboard
```

It will be available at [localhost:8050](http://localhost:8050)