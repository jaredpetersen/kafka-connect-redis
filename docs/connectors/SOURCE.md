# Kafka Connect Redis - Source
Subscribes to Redis channels/patterns (including [keyspace notifications](https://redis.io/topics/notifications)) and writes the received messages to Kafka.

## Record Schema

### Key
#### Avro
```json
{
  "name":"io.github.jaredpetersen.kafkaconnectredis.RedisSubscriptionEventKey",
  "type":"string"
}
```

#### Connect JSON
```json
{
  "name":"io.github.jaredpetersen.kafkaconnectredis.RedisSubscriptionEventKey",
  "type":"string",
  "optional":false
}
```

### Value
#### Avro
```json
{
  "name":"io.github.jaredpetersen.kafkaconnectredis.RedisSubscriptionEventValue",
  "type":"string"
}
```

#### Connect JSON
```json
{
  "name":"io.github.jaredpetersen.kafkaconnectredis.RedisSubscriptionEventValue",
  "type":"string",
  "optional":false
}
```

## Partitions
Records are partitioned using the [`DefaultPartitioner`](https://github.com/apache/kafka/blob/trunk/clients/src/main/java/org/apache/kafka/clients/producer/internals/DefaultPartitioner.java) class. This means that the record key is used to determine which partition the record is assigned to.

If you would prefer a different partitioning strategy, you may implement your own [`Partitioner`](https://github.com/apache/kafka/blob/trunk/clients/src/main/java/org/apache/kafka/clients/producer/Partitioner.java) and configure the connector to use it via [`partitioner.class`](https://kafka.apache.org/documentation/#partitioner.class). Alternatively, you may also implement a custom [Single Message Transform (SMT)](https://docs.confluent.io/current/connect/transforms/index.html).

## Parallelization
Splitting the workload between multiple tasks via the configuration property `max.tasks` is not supported at this time. Support for this will be added in the future.

## Configuration
### Connector Properties
| Name                              | Description                                       | Type    | Default | Importance |
| --------------------------------- | ------------------------------------------------- | ------- | ------- | ---------- |
| `redis.uri`                       | Redis URI                                         | string  |         | high       |
| `redis.cluster.enabled`           | Enable cluster mode                               | boolean | false   | high       |
| `redis.channels`                  | Redis channel(s) to subscribe to, comma-separated | string  |         | high       |
| `redis.channels.patterns.enabled` | Redis channels utilize patterns                   | boolean | false   | high       |
