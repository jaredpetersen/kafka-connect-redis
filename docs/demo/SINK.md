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
kubectl -n kcr-demo run -it --rm kafka-write-records --image confluentinc/cp-schema-registry:6.0.0 --command /bin/bash
```

Write records to the `redis.commands` topic:

**IMPORTANT:** The following Avro example does not completely work [due to a bug in the Avro console producer](https://github.com/confluentinc/schema-registry/issues/898). A fix has been merged but Confluent has not published a new Docker image for it yet (6.1.0+). In the meantime, you may only issue one Redis command type per topic if you are using the Avro console producer.

```bash
kafka-avro-console-producer \
    --broker-list kafka-broker-0.kafka-broker:9092 \
    --property schema.registry.url='http://kafka-schema-registry:8081' \
    --property value.schema='{"namespace":"io.github.jaredpetersen.kafkaconnectredis","name":"RedisSetCommand","type":"record","fields":[{"name":"key","type":"string"},{"name":"value","type":"string"},{"name":"expiration","type":["null",{"name":"RedisSetCommandExpiration","type":"record","fields":[{"name":"type","type":{"name":"RedisSetCommandExpirationType","type":"enum","symbols":["EX","PX","KEEPTTL"]}},{"name":"time","type":["null","long"]}]}],"default":null},{"name":"condition","type":["null",{"name":"RedisSetCommandCondition","type":"enum","symbols":["NX","XX","KEEPTTL"]}],"default":null}]}' \
    --property value.subject.name.strategy='io.confluent.kafka.serializers.subject.TopicRecordNameStrategy' \
    --topic redis.commands
>{"key":"{user.1}.username","value":"jetpackmelon22","expiration":null,"condition":null}
>{"key":"{user.2}.username","value":"anchorgoat74","expiration":{"io.github.jaredpetersen.kafkaconnectredis.RedisSetCommandExpiration":{"type":"EX","time":{"long":2100}}},"condition":{"io.github.jaredpetersen.kafkaconnectredis.RedisSetCommandCondition":"NX"}}
```

```bash
kafka-avro-console-producer \
    --broker-list kafka-broker-0.kafka-broker:9092 \
    --property schema.registry.url='http://kafka-schema-registry:8081' \
    --property value.schema='{"namespace":"io.github.jaredpetersen.kafkaconnectredis","name":"RedisSaddCommand","type":"record","fields":[{"name":"key","type":"string"},{"name":"values","type":{"type":"array","items":"string"}}]}' \
    --property value.subject.name.strategy='io.confluent.kafka.serializers.subject.TopicRecordNameStrategy' \
    --topic redis.commands
>{"key":"{user.1}.interests","values":["reading"]}
>{"key":"{user.2}.interests","values":["sailing","woodworking","programming"]}
```

```bash
kafka-avro-console-producer \
    --broker-list kafka-broker-0.kafka-broker:9092 \
    --property schema.registry.url='http://kafka-schema-registry:8081' \
    --property value.schema='{"namespace":"io.github.jaredpetersen.kafkaconnectredis","name":"RedisGeoaddCommand","type":"record","fields":[{"name":"key","type":"string"},{"name":"values","type":{"type":"array","items":{"name":"RedisGeoaddCommandGeolocation","type":"record","fields":[{"name":"longitude","type":"double"},{"name":"latitude","type":"double"},{"name":"member","type":"string"}]}}}]}' \
    --property value.subject.name.strategy='io.confluent.kafka.serializers.subject.TopicRecordNameStrategy' \
    --topic redis.commands
>{"key":"Sicily","values":[{"longitude":13.361389,"latitude":13.361389,"member":"Palermo"},{"longitude":15.087269,"latitude":37.502669,"member":"Catania"}]}
```

### Connect JSON
Create an interactive ephemeral query pod:
```bash
kubectl -n kcr-demo run -it --rm kafka-write-records --image confluentinc/cp-kafka:6.0.0 --command /bin/bash
```

Write records to the `redis.commands` topic:
```bash
kafka-console-producer \
    --broker-list kafka-broker-0.kafka-broker:9092 \
    --topic redis.commands
>{"payload":{"key":"{user.1}.username","value":"jetpackmelon22"},"schema":{"name":"io.github.jaredpetersen.kafkaconnectredis.RedisSetCommand","type":"struct","fields":[{"field":"key","type":"string","optional":false},{"field":"value","type":"string","optional":false},{"field":"expiration","type":"struct","fields":[{"field":"type","type":"string","optional":false},{"field":"time","type":"int64","optional":true}],"optional":true},{"field":"condition","type":"string","optional":true}]}}
>{"payload":{"key":"{user.2}.username","value":"anchorgoat74","expiration":{"type":"EX","time":2100},"condition":"NX"},"schema":{"name":"io.github.jaredpetersen.kafkaconnectredis.RedisSetCommand","type":"struct","fields":[{"field":"key","type":"string","optional":false},{"field":"value","type":"string","optional":false},{"field":"expiration","type":"struct","fields":[{"field":"type","type":"string","optional":false},{"field":"time","type":"int64","optional":true}],"optional":true},{"field":"condition","type":"string","optional":true}]}}
>{"payload":{"key":"{user.1}.interests","values":["reading"]},"schema":{"name":"io.github.jaredpetersen.kafkaconnectredis.RedisSaddCommand","type":"struct","fields":[{"field":"key","type":"string","optional":false},{"field":"values","type":"array","items":{"type":"string"},"optional":false}]}}
>{"payload":{"key":"{user.2}.interests","values":["sailing","woodworking","programming"]},"schema":{"name":"io.github.jaredpetersen.kafkaconnectredis.RedisSaddCommand","type":"struct","fields":[{"field":"key","type":"string","optional":false},{"field":"values","type":"array","items":{"type":"string"},"optional":false}]}}
>{"payload":{"key":"Sicily","values":[{"longitude":13.361389,"latitude":13.361389,"member":"Palermo"},{"longitude":15.087269,"latitude":37.502669,"member":"Catania"}]},"schema":{"name":"io.github.jaredpetersen.kafkaconnectredis.RedisGeoaddCommand","type":"struct","fields":[{"field":"key","type":"string","optional":false},{"field":"values","type":"array","items":{"type":"struct","fields":[{"field":"longitude","type":"double","optional":false},{"field":"latitude","type":"double","optional":false},{"field":"member","type":"string","optional":false}]},"optional":false}]}}
>{"payload":{},"schema":{}}
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
GET {user.1}.username
GET {user.2}.username
SMEMBERS {user.1}.interests
SMEMBERS {user.2}.interests
GEOPOS Sicily Catania
```
