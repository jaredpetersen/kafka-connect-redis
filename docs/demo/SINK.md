# Demo: Kafka Connect Sink
## Install Connector
Send a request to the Kafka Connect REST API to configure it to use Kafka Connect Redis.

First, expose the Kafka Connect server:
```bash
kubectl -n kcr-demo port-forward service/kafka-connect :rest
```

Kubectl will choose an available port for you that you will need to use for the cURLs. Set this port to `$PORT`.

### Avro
```bash
curl --request POST \
    --url "localhost:$PORT/connectors" \
    --header 'content-type: application/json' \
    --data '{
        "name": "demo-redis-sink-connector",
        "config": {
            "connector.class": "io.github.jaredpetersen.kafkaconnectredis.sink.RedisSinkConnector",
            "key.converter": "io.confluent.connect.avro.AvroConverter",
            "key.converter.schema.registry.url": "http://kafka-schema-registry:8081",
            "key.converter.key.subject.name.strategy": "io.confluent.kafka.serializers.subject.TopicRecordNameStrategy",
            "value.converter": "io.confluent.connect.avro.AvroConverter",
            "value.converter.schema.registry.url": "http://kafka-schema-registry:8081",
            "value.converter.value.subject.name.strategy": "io.confluent.kafka.serializers.subject.TopicRecordNameStrategy",
            "tasks.max": "3",
            "topics": "redis.commands",
            "redis.uri": "redis://IEPfIr0eLF7UsfwrIlzy80yUaBG258j9@redis-cluster",
            "redis.cluster.enabled": true
        }
    }'
```

### Connect JSON
```bash
curl --request POST \
    --url "localhost:$PORT/connectors" \
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
kubectl -n kcr-demo run -it --rm kafka-write-records --image confluentinc/cp-schema-registry:6.1.0 --command /bin/bash
```

Write records to the `redis.commands` topic:

```bash
kafka-avro-console-producer \
    --broker-list kafka-broker-0.kafka-broker:9092 \
    --property schema.registry.url='http://kafka-schema-registry:8081' \
    --property value.subject.name.strategy='io.confluent.kafka.serializers.subject.TopicRecordNameStrategy' \
    --property value.schema='{"namespace":"io.github.jaredpetersen.kafkaconnectredis","name":"RedisSetCommand","type":"record","fields":[{"name":"key","type":"string"},{"name":"value","type":"string"},{"name":"expiration","type":["null",{"name":"RedisSetCommandExpiration","type":"record","fields":[{"name":"type","type":{"name":"RedisSetCommandExpirationType","type":"enum","symbols":["EX","PX","KEEPTTL"]}},{"name":"time","type":["null","long"]}]}],"default":null},{"name":"condition","type":["null",{"name":"RedisSetCommandCondition","type":"enum","symbols":["NX","XX","KEEPTTL"]}],"default":null}]}' \
    --topic redis.commands
>{"key":"{user.1}.username","value":"jetpackmelon22","expiration":null,"condition":null}
>{"key":"{user.2}.username","value":"anchorgoat74","expiration":{"io.github.jaredpetersen.kafkaconnectredis.RedisSetCommandExpiration":{"type":"EX","time":{"long":2100}}},"condition":{"io.github.jaredpetersen.kafkaconnectredis.RedisSetCommandCondition":"NX"}}
>{"key":"product.milk","value":"$2.29","expiration":null,"condition":null}
>{"key":"product.bread","value":"$5.49","expiration":null,"condition":null}
>{"key":"product.waffles","value":"$2.59","expiration":null,"condition":null}
```

```bash
kafka-avro-console-producer \
    --broker-list kafka-broker-0.kafka-broker:9092 \
    --property schema.registry.url='http://kafka-schema-registry:8081' \
    --property value.subject.name.strategy='io.confluent.kafka.serializers.subject.TopicRecordNameStrategy' \
    --property value.schema='{"namespace":"io.github.jaredpetersen.kafkaconnectredis","name":"RedisExpireCommand","type":"record","fields":[{"name":"key","type":"string"},{"name":"seconds","type":"long"}]}' \
    --topic redis.commands
>{"key":"product.milk","seconds":1800}
```

```bash
kafka-avro-console-producer \
    --broker-list kafka-broker-0.kafka-broker:9092 \
    --property schema.registry.url='http://kafka-schema-registry:8081' \
    --property value.subject.name.strategy='io.confluent.kafka.serializers.subject.TopicRecordNameStrategy' \
    --property value.schema='{"namespace":"io.github.jaredpetersen.kafkaconnectredis","name":"RedisExpireatCommand","type":"record","fields":[{"name":"key","type":"string"},{"name":"timestamp","type":"long"}]}' \
    --topic redis.commands
>{"key":"product.bread","timestamp":4130464553}
```

```bash
kafka-avro-console-producer \
    --broker-list kafka-broker-0.kafka-broker:9092 \
    --property schema.registry.url='http://kafka-schema-registry:8081' \
    --property value.subject.name.strategy='io.confluent.kafka.serializers.subject.TopicRecordNameStrategy' \
    --property value.schema='{"namespace":"io.github.jaredpetersen.kafkaconnectredis","name":"RedisPexpireCommand","type":"record","fields":[{"name":"key","type":"string"},{"name":"milliseconds","type":"long"}]}' \
    --topic redis.commands
>{"key":"product.waffles","milliseconds":1800000}
```

```bash
kafka-avro-console-producer \
    --broker-list kafka-broker-0.kafka-broker:9092 \
    --property schema.registry.url='http://kafka-schema-registry:8081' \
    --property value.subject.name.strategy='io.confluent.kafka.serializers.subject.TopicRecordNameStrategy' \
    --property value.schema='{"namespace":"io.github.jaredpetersen.kafkaconnectredis","name":"RedisSaddCommand","type":"record","fields":[{"name":"key","type":"string"},{"name":"values","type":{"type":"array","items":"string"}}]}' \
    --topic redis.commands
>{"key":"{user.1}.interests","values":["reading"]}
>{"key":"{user.2}.interests","values":["sailing","woodworking","programming"]}
```

```bash
kafka-avro-console-producer \
    --broker-list kafka-broker-0.kafka-broker:9092 \
    --property schema.registry.url='http://kafka-schema-registry:8081' \
    --property value.subject.name.strategy='io.confluent.kafka.serializers.subject.TopicRecordNameStrategy' \
    --property value.schema='{"namespace":"io.github.jaredpetersen.kafkaconnectredis","name":"RedisGeoaddCommand","type":"record","fields":[{"name":"key","type":"string"},{"name":"values","type":{"type":"array","items":{"name":"RedisGeoaddCommandGeolocation","type":"record","fields":[{"name":"longitude","type":"double"},{"name":"latitude","type":"double"},{"name":"member","type":"string"}]}}}]}' \
    --topic redis.commands
>{"key":"Sicily","values":[{"longitude":13.361389,"latitude":13.361389,"member":"Palermo"},{"longitude":15.087269,"latitude":37.502669,"member":"Catania"}]}
```

```bash
kafka-avro-console-producer \
    --broker-list kafka-broker-0.kafka-broker:9092 \
    --property schema.registry.url='http://kafka-schema-registry:8081' \
    --property value.subject.name.strategy='io.confluent.kafka.serializers.subject.TopicRecordNameStrategy' \
    --property value.schema='{"namespace":"io.github.jaredpetersen.kafkaconnectredis","name":"RedisArbitraryCommand","type":"record","fields":[{"name":"command","type":"string"},{"name":"arguments","type":{"type":"array","items":"string"}}]}' \
    --topic redis.commands
>{"command":"TS.CREATE","arguments":["temperature:3:11", "RETENTION", "60", "LABELS", "sensor_id", "2", "area_id", "32"]}
>{"command":"TS.ADD","arguments":["temperature:3:11", "1548149181", "30"]}
>{"command":"TS.ADD","arguments":["temperature:3:11", "1548149191", "42"]}
```

### Connect JSON
Create an interactive ephemeral query pod:
```bash
kubectl -n kcr-demo run -it --rm kafka-write-records --image confluentinc/cp-kafka:6.1.0 --command /bin/bash
```

Write records to the `redis.commands` topic:
```bash
kafka-console-producer \
    --broker-list kafka-broker-0.kafka-broker:9092 \
    --topic redis.commands
>{"payload":{"key":"{user.1}.username","value":"jetpackmelon22"},"schema":{"name":"io.github.jaredpetersen.kafkaconnectredis.RedisSetCommand","type":"struct","fields":[{"field":"key","type":"string","optional":false},{"field":"value","type":"string","optional":false},{"field":"expiration","type":"struct","fields":[{"field":"type","type":"string","optional":false},{"field":"time","type":"int64","optional":true}],"optional":true},{"field":"condition","type":"string","optional":true}]}}
>{"payload":{"key":"{user.2}.username","value":"anchorgoat74","expiration":{"type":"EX","time":2100},"condition":"NX"},"schema":{"name":"io.github.jaredpetersen.kafkaconnectredis.RedisSetCommand","type":"struct","fields":[{"field":"key","type":"string","optional":false},{"field":"value","type":"string","optional":false},{"field":"expiration","type":"struct","fields":[{"field":"type","type":"string","optional":false},{"field":"time","type":"int64","optional":true}],"optional":true},{"field":"condition","type":"string","optional":true}]}}
>{"payload":{"key":"product.milk","value":"$2.29"},"schema":{"name":"io.github.jaredpetersen.kafkaconnectredis.RedisSetCommand","type":"struct","fields":[{"field":"key","type":"string","optional":false},{"field":"value","type":"string","optional":false},{"field":"expiration","type":"struct","fields":[{"field":"type","type":"string","optional":false},{"field":"time","type":"int64","optional":true}],"optional":true},{"field":"condition","type":"string","optional":true}]}}
>{"payload":{"key":"product.milk","seconds":1800},"schema":{"name":"io.github.jaredpetersen.kafkaconnectredis.RedisExpireCommand","type":"struct","fields":[{"field":"key","type":"string","optional":false},{"field":"seconds","type":"int64","optional":false}]}}
>{"payload":{"key":"product.bread","value":"$5.49"},"schema":{"name":"io.github.jaredpetersen.kafkaconnectredis.RedisSetCommand","type":"struct","fields":[{"field":"key","type":"string","optional":false},{"field":"value","type":"string","optional":false},{"field":"expiration","type":"struct","fields":[{"field":"type","type":"string","optional":false},{"field":"time","type":"int64","optional":true}],"optional":true},{"field":"condition","type":"string","optional":true}]}}
>{"payload":{"key":"product.bread","timestamp":4130464553},"schema":{"name":"io.github.jaredpetersen.kafkaconnectredis.RedisExpireatCommand","type":"struct","fields":[{"field":"key","type":"string","optional":false},{"field":"timestamp","type":"int64","optional":false}]}}
>{"payload":{"key":"product.waffles","value":"$2.59"},"schema":{"name":"io.github.jaredpetersen.kafkaconnectredis.RedisSetCommand","type":"struct","fields":[{"field":"key","type":"string","optional":false},{"field":"value","type":"string","optional":false},{"field":"expiration","type":"struct","fields":[{"field":"type","type":"string","optional":false},{"field":"time","type":"int64","optional":true}],"optional":true},{"field":"condition","type":"string","optional":true}]}}
>{"payload":{"key":"product.waffles","milliseconds":1800000},"schema":{"name":"io.github.jaredpetersen.kafkaconnectredis.RedisPexpireCommand","type":"struct","fields":[{"field":"key","type":"string","optional":false},{"field":"milliseconds","type":"int64","optional":false}]}}
>{"payload":{"key":"{user.1}.interests","values":["reading"]},"schema":{"name":"io.github.jaredpetersen.kafkaconnectredis.RedisSaddCommand","type":"struct","fields":[{"field":"key","type":"string","optional":false},{"field":"values","type":"array","items":{"type":"string"},"optional":false}]}}
>{"payload":{"key":"{user.2}.interests","values":["sailing","woodworking","programming"]},"schema":{"name":"io.github.jaredpetersen.kafkaconnectredis.RedisSaddCommand","type":"struct","fields":[{"field":"key","type":"string","optional":false},{"field":"values","type":"array","items":{"type":"string"},"optional":false}]}}
>{"payload":{"key":"Sicily","values":[{"longitude":13.361389,"latitude":13.361389,"member":"Palermo"},{"longitude":15.087269,"latitude":37.502669,"member":"Catania"}]},"schema":{"name":"io.github.jaredpetersen.kafkaconnectredis.RedisGeoaddCommand","type":"struct","fields":[{"field":"key","type":"string","optional":false},{"field":"values","type":"array","items":{"type":"struct","fields":[{"field":"longitude","type":"double","optional":false},{"field":"latitude","type":"double","optional":false},{"field":"member","type":"string","optional":false}]},"optional":false}]}}
>{"payload":{"command":"TS.CREATE","arguments":["temperature:3:11", "RETENTION", "60", "LABELS", "sensor_id", "2", "area_id", "32"]},"schema":{"name":"io.github.jaredpetersen.kafkaconnectredis.RedisArbitraryCommand","type":"struct","fields":[{"field":"command","type":"string","optional":false},{"field":"arguments","type":"array","items":{"type":"string"},"optional":false}]}}
>{"payload":{"command":"TS.ADD","arguments":["temperature:3:11", "1548149181", "30"]},"schema":{"name":"io.github.jaredpetersen.kafkaconnectredis.RedisArbitraryCommand","type":"struct","fields":[{"field":"command","type":"string","optional":false},{"field":"arguments","type":"array","items":{"type":"string"},"optional":false}]}}
>{"payload":{"command":"TS.ADD","arguments":["temperature:3:11", "1548149191", "42"]},"schema":{"name":"io.github.jaredpetersen.kafkaconnectredis.RedisArbitraryCommand","type":"struct","fields":[{"field":"command","type":"string","optional":false},{"field":"arguments","type":"array","items":{"type":"string"},"optional":false}]}}
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
GET product.bread
TTL product.bread
GET product.milk
TTL product.milk
GET product.waffles
PTTL product.waffles
SMEMBERS {user.1}.interests
SMEMBERS {user.2}.interests
GEOPOS Sicily Catania
TS.RANGE temperature:3:11 1548149180 1548149210 AGGREGATION avg 5
```
