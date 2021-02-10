# Docker Compose
Docker Compose equivalent of the source connector demo. Utilizes a single-node instances of everything for the sake of simplicity.

## Usage
Navigate to `docs/demo/docker/kafka-connect-redis` and download the plugin:
```
curl -O https://repo1.maven.org/maven2/io/github/jaredpetersen/kafka-connect-redis/1.1.0/kafka-connect-redis-1.1.0.jar
```

Build images and spin up containers using Docker Compose:
```
docker-compose up -d --build
```

Follow along with the Kafka Connect startup process:
```
docker-compose logs -f kafka-connect
```

Configure the container (Avro):
```
curl --request POST \
    --url "localhost:8083/connectors" \
    --header 'content-type: application/json' \
    --data '{
        "name": "demo-redis-source-connector",
        "config": {
            "connector.class": "io.github.jaredpetersen.kafkaconnectredis.source.RedisSourceConnector",
            "key.converter": "io.confluent.connect.avro.AvroConverter",
            "key.converter.schema.registry.url": "http://kafka-schema-registry:8081",
            "value.converter": "io.confluent.connect.avro.AvroConverter",
            "value.converter.schema.registry.url": "http://kafka-schema-registry:8081",
            "tasks.max": "1",
            "topic": "redis.events",
            "redis.uri": "redis://IEPfIr0eLF7UsfwrIlzy80yUaBG258j9@redis",
            "redis.cluster.enabled": false,
            "redis.channels": "__key*__:*",
            "redis.channels.pattern.enabled": true
        }
    }'
```

Run a new container inside the network to tail Kafka records:
```
docker run -it --rm --name kafka-tail-records --network docker-compose_default confluentinc/cp-schema-registry:6.0.0 /bin/bash
```

```
kafka-avro-console-consumer \
    --bootstrap-server kafka-broker:9092 \
    --property schema.registry.url='http://kafka-schema-registry:8081' \
    --property print.key=true \
    --property key.separator='|' \
    --topic redis.events \
    --from-beginning
```

Start putting data in Redis:
```
redis-cli -u 'redis://IEPfIr0eLF7UsfwrIlzy80yUaBG258j9@localhost'
```

```
SET {user.1}.username jetpackmelon22 EX 2
SET {user.2}.username anchorgoat74 EX 2
SADD {user.1}.interests reading
EXPIRE {user.1}.interests 2
SADD {user.2}.interests sailing woodworking programming
EXPIRE {user.2}.interests 2
GET {user.1}.username
GET {user.2}.username
SMEMBERS {user.1}.interests
SMEMBERS {user.2}.interests
```

Observe as records show up in the previously created kafka-tail-records container.