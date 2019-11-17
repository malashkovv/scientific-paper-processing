# scientific-paper-processing

# Kafka

After kafka is up and running, make sure that topic is created 
```bash
docker-compose exec kafka1 kafka-topics.sh \
    --create --zookeeper zookeeper:2181 \
    --replication-factor 2 --partitions 3 \
    --topic science-papers
```

Examples:
https://www.biorxiv.org/content/10.1101/771626v1