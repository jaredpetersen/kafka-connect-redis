# Demo: Kafka Connect Source
## Install Connector
Send a request to the Kafka Connect REST API to configure it to use Kafka Connect Redis. We'll be listening to all events in Redis by using the channel `__key*__:*`.

### Avro
```bash
curl --request POST \
    --url "$(minikube -n kcr-demo service kafka-connect --url)/connectors" \
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
            "redis.uri": "redis://IEPfIr0eLF7UsfwrIlzy80yUaBG258j9@redis-cluster",
            "redis.cluster.enabled": true,
            "redis.channels": "__key*__:*",
            "redis.channels.pattern.enabled": true
        }
    }'
```

### Connect JSON
```bash
curl --request POST \
    --url "$(minikube -n kcr-demo service kafka-connect --url)/connectors" \
    --header 'content-type: application/json' \
    --data '{
        "name": "demo-redis-source-connector4",
        "config": {
            "connector.class": "io.github.jaredpetersen.kafkaconnectredis.source.RedisSourceConnector",
            "key.converter": "org.apache.kafka.connect.json.JsonConverter",
            "value.converter": "org.apache.kafka.connect.json.JsonConverter",
            "tasks.max": "1",
            "topic": "redis.events",
            "redis.uri": "redis://IEPfIr0eLF7UsfwrIlzy80yUaBG258j9@redis-cluster",
            "redis.cluster.enabled": true,
            "redis.channels": "__key*__:*",
            "redis.channels.pattern.enabled": true
        }
    }'
```

## Create Redis Events
Create Redis client pod:
```bash
kubectl -n kcr-demo run -it --rm redis-client --image redis:6 -- /bin/bash
```

Use redis-cli to connect to the cluster:
```bash
redis-cli -c -u 'redis://IEPfIr0eLF7UsfwrIlzy80yUaBG258j9@redis-cluster'
```

Run commands to create some different events:
```bash
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

## Validate
### Avro
Create an interactive ephemeral query pod:
```bash
kubectl -n kcr-demo run -it --rm kafka-tail-records --image confluentinc/cp-schema-registry:6.0.0 --command /bin/bash
```

Tail the topic, starting from the beginning:
```bash
kafka-console-avro-consumer \
    --broker-list kafka-broker-0.kafka-broker:9092 \
    --property schema.registry.url='http://kafka-schema-registry:8081' \
    --property value.schema='{}' \
    --topic redis.events
    --from-beginning
```

### Connect JSON
Create an interactive ephemeral query pod:
```bash
kubectl -n kcr-demo run -it --rm kafka-tail-records --image confluentinc/cp-kafka:6.0.0 --command /bin/bash
```

Tail the topic, starting from the beginning:
```bash
kafka-console-consumer \
    --bootstrap-server kafka-broker-0.kafka-broker:9092 \
    --topic redis.events \
    --from-beginning
```

## Teardown
Remove all manifests:
```bash
k delete -k kubernetes
```

Delete the minikube cluster
```bash
minikube delete
```
