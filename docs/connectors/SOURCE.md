# Kafka Connect Redis - Source
Subscribes to Redis channels/patterns (including [keyspace notifications](https://redis.io/topics/notifications)) and writes the received messages to Kafka.

## Record Schema

### Key
#### Avro
```json
{
    "namespace": "io.github.jaredpetersen.kafkaconnectredis",
    "name": "RedisSubscriptionEventKey",
    "type": "record",
    "fields": [
        {
            "name": "channel",
            "type": "string"
        },
        {
            "name": "pattern",
            "type": [null, "string"]
        }
    ]
}
```

#### Connect JSON
```json
{
    "name": "io.github.jaredpetersen.kafkaconnectredis.RedisSubscriptionEventKey",
    "type": "struct",
    "fields": [
        {
            "field": "channel",
            "type": "string",
            "optional": false
        },
        {
            "field": "pattern",
            "type": "string",
            "optional": true
        }
    ]
}
```

### Value
#### Avro
```json
{
    "namespace": "io.github.jaredpetersen.kafkaconnectredis",
    "name": "RedisSubscriptionEventValue",
    "type": "record",
    "fields": [
        {
            "name": "message",
            "type": "string"
        }
    ]
}
```

#### Connect JSON
```json
{
    "name": "io.github.jaredpetersen.kafkaconnectredis.RedisSubscriptionEventValue",
    "type": "struct",
    "fields": [
        {
            "field": "message",
            "type": "string",
            "optional": false
        }
    ]
}
```

## Partitions
Records are partitioned using the [`DefaultPartitioner`](https://github.com/apache/kafka/blob/trunk/clients/src/main/java/org/apache/kafka/clients/producer/internals/DefaultPartitioner.java) class. This means that the record key is used to determine which partition the record is assigned to.

If you would prefer a different partitioning strategy, you may implement your own [`Partitioner`](https://github.com/apache/kafka/blob/trunk/clients/src/main/java/org/apache/kafka/clients/producer/Partitioner.java) and configure the connector to use it via [`partitioner.class`](https://kafka.apache.org/documentation/#partitioner.class). Alternatively, you may also implement a custom [Single Message Transform (SMT)](https://docs.confluent.io/current/connect/transforms/index.html).

## Parallelization
Splitting the workload between multiple tasks via the configuration property `max.tasks` is not supported at this time. Support for this will be added in the future.

## Configuration
### Connector Properties
| Name                              | Type    | Default | Importance | Description                                             |
| --------------------------------- | ------- | ------- | ---------- | ------------------------------------------------------- |
| `redis.uri`                       | string  |         | High       | Redis connection information provided via a URI string. |
| `redis.cluster.enabled`           | boolean | false   | High       | Target Redis is running as a cluster.                   |
| `redis.channels`                  | string  |         | High       | Redis channels to subscribe to separated by commas.     |
| `redis.channels.patterns.enabled` | boolean |         | High       | Redis channels use patterns (PSUBSCRIBE).               |
