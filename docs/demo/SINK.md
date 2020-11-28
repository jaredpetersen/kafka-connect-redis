# Demo: Kafka Connect Sink
## Install Connector
Send a request to the Kafka Connect REST API to configure it to use Kafka Connect Redis:

### Avro
**IMPORTANT:** The Avro demo utilizes multiple topics in order to work around [a bug in the Avro console producer](https://github.com/confluentinc/schema-registry/issues/898). A fix has been merged but Confluent has not published a new Docker image for it yet (6.1.0+). Kafka Connect Redis works with Avro on a single topic; this is just a problem with the console producer provided by Confluent.

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
            "topics": "redis.commands.set,redis.commands.expire,redis.commands.expireat,redis.commands.pexpire,redis.commands.sadd,redis.commands.geoadd,redis.commands.arbitrary",
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

Write records to the `redis.commands` topics:

```bash
kafka-avro-console-producer \
    --broker-list kafka-broker-0.kafka-broker:9092 \
    --property schema.registry.url='http://kafka-schema-registry:8081' \
    --property value.schema='{"namespace":"io.github.jaredpetersen.kafkaconnectredis","name":"RedisSetCommand","type":"record","fields":[{"name":"key","type":"string"},{"name":"value","type":"string"},{"name":"expiration","type":["null",{"name":"RedisSetCommandExpiration","type":"record","fields":[{"name":"type","type":{"name":"RedisSetCommandExpirationType","type":"enum","symbols":["EX","PX","KEEPTTL"]}},{"name":"time","type":["null","long"]}]}],"default":null},{"name":"condition","type":["null",{"name":"RedisSetCommandCondition","type":"enum","symbols":["NX","XX","KEEPTTL"]}],"default":null}]}' \
    --topic redis.commands.set
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
    --property value.schema='{"namespace":"io.github.jaredpetersen.kafkaconnectredis","name":"RedisExpireCommand","type":"record","fields":[{"name":"key","type":"string"},{"name":"seconds","type":"long"}]}' \
    --topic redis.commands.expire
>{"key":"product.milk","seconds":1800}
```

```bash
kafka-avro-console-producer \
    --broker-list kafka-broker-0.kafka-broker:9092 \
    --property schema.registry.url='http://kafka-schema-registry:8081' \
    --property value.schema='{"namespace":"io.github.jaredpetersen.kafkaconnectredis","name":"RedisExpireatCommand","type":"record","fields":[{"name":"key","type":"string"},{"name":"timestamp","type":"long"}]}' \
    --topic redis.commands.expireat
>{"key":"product.bread","timestamp":4130464553}
```

```bash
kafka-avro-console-producer \
    --broker-list kafka-broker-0.kafka-broker:9092 \
    --property schema.registry.url='http://kafka-schema-registry:8081' \
    --property value.schema='{"namespace":"io.github.jaredpetersen.kafkaconnectredis","name":"RedisPexpireCommand","type":"record","fields":[{"name":"key","type":"string"},{"name":"milliseconds","type":"long"}]}' \
    --topic redis.commands.pexpire
>{"key":"product.waffles","milliseconds":1800000}
```

```bash
kafka-avro-console-producer \
    --broker-list kafka-broker-0.kafka-broker:9092 \
    --property schema.registry.url='http://kafka-schema-registry:8081' \
    --property value.schema='{"namespace":"io.github.jaredpetersen.kafkaconnectredis","name":"RedisSaddCommand","type":"record","fields":[{"name":"key","type":"string"},{"name":"values","type":{"type":"array","items":"string"}}]}' \
    --topic redis.commands.sadd
>{"key":"{user.1}.interests","values":["reading"]}
>{"key":"{user.2}.interests","values":["sailing","woodworking","programming"]}
```

```bash
kafka-avro-console-producer \
    --broker-list kafka-broker-0.kafka-broker:9092 \
    --property schema.registry.url='http://kafka-schema-registry:8081' \
    --property value.schema='{"namespace":"io.github.jaredpetersen.kafkaconnectredis","name":"RedisGeoaddCommand","type":"record","fields":[{"name":"key","type":"string"},{"name":"values","type":{"type":"array","items":{"name":"RedisGeoaddCommandGeolocation","type":"record","fields":[{"name":"longitude","type":"double"},{"name":"latitude","type":"double"},{"name":"member","type":"string"}]}}}]}' \
    --topic redis.commands.geoadd
>{"key":"Sicily","values":[{"longitude":13.361389,"latitude":13.361389,"member":"Palermo"},{"longitude":15.087269,"latitude":37.502669,"member":"Catania"}]}
```

```bash
kafka-avro-console-producer \
    --broker-list kafka-broker-0.kafka-broker:9092 \
    --property schema.registry.url='http://kafka-schema-registry:8081' \
    --property value.schema='{"namespace":"io.github.jaredpetersen.kafkaconnectredis","name":"RedisArbitraryCommand","type":"record","fields":[{"name":"command","type":"string"},{"name":"arguments","type":{"type":"array","items":"string"}}]}' \
    --topic redis.commands.arbitrary
>{"command":"SET","arguments":["arbitrary", "value"]}
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
>{"payload":{"key":"product.milk","value":"$2.29"},"schema":{"name":"io.github.jaredpetersen.kafkaconnectredis.RedisSetCommand","type":"struct","fields":[{"field":"key","type":"string","optional":false},{"field":"value","type":"string","optional":false},{"field":"expiration","type":"struct","fields":[{"field":"type","type":"string","optional":false},{"field":"time","type":"int64","optional":true}],"optional":true},{"field":"condition","type":"string","optional":true}]}}
>{"payload":{"key":"product.milk","seconds":1800},"schema":{"name":"io.github.jaredpetersen.kafkaconnectredis.RedisExpireCommand","type":"struct","fields":[{"field":"key","type":"string","optional":false},{"field":"seconds","type":"int64","optional":false}]}}
>{"payload":{"key":"product.bread","value":"$5.49"},"schema":{"name":"io.github.jaredpetersen.kafkaconnectredis.RedisSetCommand","type":"struct","fields":[{"field":"key","type":"string","optional":false},{"field":"value","type":"string","optional":false},{"field":"expiration","type":"struct","fields":[{"field":"type","type":"string","optional":false},{"field":"time","type":"int64","optional":true}],"optional":true},{"field":"condition","type":"string","optional":true}]}}
>{"payload":{"key":"product.bread","timestamp":4130464553},"schema":{"name":"io.github.jaredpetersen.kafkaconnectredis.RedisExpireatCommand","type":"struct","fields":[{"field":"key","type":"string","optional":false},{"field":"timestamp","type":"int64","optional":false}]}}
>{"payload":{"key":"product.waffles","value":"$2.59"},"schema":{"name":"io.github.jaredpetersen.kafkaconnectredis.RedisSetCommand","type":"struct","fields":[{"field":"key","type":"string","optional":false},{"field":"value","type":"string","optional":false},{"field":"expiration","type":"struct","fields":[{"field":"type","type":"string","optional":false},{"field":"time","type":"int64","optional":true}],"optional":true},{"field":"condition","type":"string","optional":true}]}}
>{"payload":{"key":"product.waffles","milliseconds":1800000},"schema":{"name":"io.github.jaredpetersen.kafkaconnectredis.RedisPexpireCommand","type":"struct","fields":[{"field":"key","type":"string","optional":false},{"field":"milliseconds","type":"int64","optional":false}]}}
>{"payload":{"key":"{user.1}.interests","values":["reading"]},"schema":{"name":"io.github.jaredpetersen.kafkaconnectredis.RedisSaddCommand","type":"struct","fields":[{"field":"key","type":"string","optional":false},{"field":"values","type":"array","items":{"type":"string"},"optional":false}]}}
>{"payload":{"key":"{user.2}.interests","values":["sailing","woodworking","programming"]},"schema":{"name":"io.github.jaredpetersen.kafkaconnectredis.RedisSaddCommand","type":"struct","fields":[{"field":"key","type":"string","optional":false},{"field":"values","type":"array","items":{"type":"string"},"optional":false}]}}
>{"payload":{"key":"Sicily","values":[{"longitude":13.361389,"latitude":13.361389,"member":"Palermo"},{"longitude":15.087269,"latitude":37.502669,"member":"Catania"}]},"schema":{"name":"io.github.jaredpetersen.kafkaconnectredis.RedisGeoaddCommand","type":"struct","fields":[{"field":"key","type":"string","optional":false},{"field":"values","type":"array","items":{"type":"struct","fields":[{"field":"longitude","type":"double","optional":false},{"field":"latitude","type":"double","optional":false},{"field":"member","type":"string","optional":false}]},"optional":false}]}}
>{"payload":{"command":"SET","arguments":["arbitrary", "value"]},"schema":{"name":"io.github.jaredpetersen.kafkaconnectredis.RedisArbitraryCommand","type":"struct","fields":[{"field":"command","type":"string","optional":false},{"field":"arguments","type":"array","items":{"type":"string"},"optional":false}]}}
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
ACL GETUSER kcr-demo-user
```
