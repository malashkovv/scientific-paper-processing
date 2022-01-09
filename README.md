# scientific-paper-processing

# Build

You need to have Docker and docker-compose on your computer in order to run.

Then simply do 
```bash
docker-compose build
```

Run 
```bash
source docker_compose_init.sh
```
for initialization

# Scrapping and streaming

NOTE: biorxiv scrapper does not work, so it's outdated.

Boot up scrappers:
```bash
docker-compose up -d fake
```

Then run streaming job:
```bash
docker-compose run etl
```

# ML

## Spark

You need to make sure that Spark cluster is running with reporting database

```bash
docker-compose up -d spark-master spark-worker1 spark-worker2 reporting-db
```

Then you have 2 ways to run:
1. with live data from etl streaming
1. with pre-collected data

For live streaming run
```bash
docker-compose run classifier-training
```
to train classification model.


In order to get analytics data run 
```bash
docker-compose run reporting
```

For pre-collected data run 
```bash
docker-compose run classifier-training-collected
```
to train classification model.


In order to get analytics data run 
```bash
docker-compose run reporting-collected
```

# Dashboard

In order to boot up dashboard run 

```bash
docker-compose up -d dashboard
```

It will be available at [localhost:8050](http://localhost:8050)