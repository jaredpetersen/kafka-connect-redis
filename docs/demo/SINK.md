# Demo: Kafka Connect Sink
## Install Connector
Send a request to the Kafka Connect REST API to configure it to use Kafka Connect Redis:

### Avro
```bash
curl --request POST \
    --url "$(minikube -n kcr-demo service kafka-connect --url)/connectors" \
    --header 'content-type: application/json' \
    --data '{
        "name": "demo-redis-sink-connector",
        "config": {
            "connector.class": "io.github.jaredpetersen.kafkaconnectredis.sink.RedisSinkConnector",
            "key.converter": "io.confluent.connect.avro.AvroConverter",
            "key.converter.schema.registry.url": "http://kafka-schema-registry:8081",
            "value.converter": "io.confluent.connect.avro.AvroConverter",
            "value.converter.schema.registry.url": "http://kafka-schema-registry:8081",
            "tasks.max": "1",
            "topics": "redis.commands",
            "redis.uri": "redis://IEPfIr0eLF7UsfwrIlzy80yUaBG258j9@redis-cluster",
            "redis.cluster.enabled": true
        }
    }'
```

### Connect JSON
```bash
curl --request POST \
    --url "$(minikube -n kcr-demo service kafka-connect --url)/connectors" \
    --header 'content-type: application/json' \
    --data '{
        "name": "demo-redis-sink-connector",
        "config": {
            "connector.class": "io.github.jaredpetersen.kafkaconnectredis.sink.RedisSinkConnector",
            "key.converter": "org.apache.kafka.connect.json.JsonConverter",
            "value.converter": "org.apache.kafka.connect.json.JsonConverter",
            "tasks.max": "1",
            "topics": "redis.commands",
            "redis.uri": "redis://IEPfIr0eLF7UsfwrIlzy80yUaBG258j9@redis-cluster",
            "redis.cluster.enabled": true
        }
    }'
```

## Write Records
### Avro
Create an interactive ephemeral query pod:
```bash
kubectl -n kcr-demo run -it --rm kafka-write-records --image confluentinc/cp-schema-registry:5.4.3 --command /bin/bash
```

Write records to the `redis.commands` topic:
```bash
kafka-avro-console-producer \
    --broker-list kafka-broker-0.kafka-broker:9092 \
    --property schema.registry.url='http://kafka-schema-registry:8081' \
    --property value.schema='{"namespace":"io.github.jaredpetersen.kafkaconnectredis","name":"RedisSetCommand","type":"record","fields":[{"name":"key","type":"string"},{"name":"value","type":"string"},{"name":"expiration","type":["null",{"name":"RedisSetCommandExpiration","type":"record","fields":[{"name":"type","type":{"name":"RedisSetCommandExpirationType","type":"enum","symbols":["EX","PX","KEEPTTL"]}},{"name":"time","type":["null","long"]}]}],"default":null},{"name":"condition","type":["null",{"name":"RedisSetCommandCondition","type":"enum","symbols":["NX","XX","KEEPTTL"]}],"default":null}]}' \
    --topic redis.commands
>{"key":"{user.1}.username","value":"jetpackmelon22","expiration":null,"condition":null}
>{"key":"{user.2}.username","value":"anchorgoat74","expiration":{"io.github.jaredpetersen.kafkaconnectredis.RedisSetCommandExpiration":{"type":"EX","time":{"long":2100}}},"condition":{"io.github.jaredpetersen.kafkaconnectredis.RedisSetCommandCondition":"NX"}}
```

```bash
// TODO
```

### Connect JSON
Create an interactive ephemeral query pod:
```bash
kubectl -n kcr-demo run -it --rm kafka-write-records --image confluentinc/cp-kafka:5.4.3 --command /bin/bash
```

Write records to the `redis.commands` topic:
```bash
kafka-console-producer \
    --broker-list kafka-broker-0.kafka-broker:9092 \
    --topic redis.commands
>{"payload":{"key":"{user.1}.username","value":"jetpackmelon22"},"schema":{"name":"io.github.jaredpetersen.kafkaconnectredis.RedisSetCommand","type":"struct","fields":[{"field":"key","type":"string","optional":false},{"field":"value","type":"string","optional":false},{"field":"expiration","type":"struct","fields":[{"field":"type","type":"string","optional":false},{"field":"time","type":"int64","optional":false}],"optional":true},{"field":"condition","type":"string","optional":true}],"optional":false}}
>{"payload":{"key":"{user.2}.username","value":"anchorgoat74","expiration":{"type":"EX","time":2100},"condition":"NX"},"schema":{"name":"io.github.jaredpetersen.kafkaconnectredis.RedisSetCommand","type":"struct","fields":[{"field":"key","type":"string","optional":false},{"field":"value","type":"string","optional":false},{"field":"expiration","type":"struct","fields":[{"field":"type","type":"string","optional":false},{"field":"time","type":"int64","optional":false}],"optional":true},{"field":"condition","type":"string","optional":true}],"optional":false}}
```

## Validate
Create Redis client pod:
```bash
kubectl -n kcr-demo run -it --rm redis-client --image redis:6 -- /bin/bash
```

Use redis-cli to connect to the cluster:
```bash
redis-cli -c -u 'redis://IEPfIr0eLF7UsfwrIlzy80yUaBG258j9@redis-cluster'
```

Run commands to confirm the commands were applied correctly:
```bash
KEYS *
GET '{user.1}.username'
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