# Kafka Connect Redis - Source
Subscribes to Redis channels/patterns (including [keyspace notifications](https://redis.io/topics/notifications)) and writes the received messages to Kafka.

**WARNING** Delivery of keyspace notifications is not reliable for Redis clusters. Keyspace notifications are node-local and adding new upstream nodes to your Redis cluster may involve a short period where events on the new node are not picked up until the connector discovers the node and issues a `SUBSCRIBE` command to it. This is a limitation of keyspace notifications that the Redis organization would like to overcome in the future.

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

In the case of subscribing to Redis keyspace notifications, it may be useful to avoid partitioning the data so that multiple event types can arrive in order as a single event stream. This can be accomplished by configuring the connector to publish to a Kafka topic that only contains a single partition, forcing the DefaultPartitioner to only utilize the single partition.

The plugin can be configured to use an alternative partitioning strategy if desired. Set the configuration property `connector.client.config.override.policy` to value `All` on the Kafka Connect worker (the overall Kafka Connect application that runs plugins). This will allow the override of the internal Kafka producer and consumer configurations. To override the partitioner for an individual connector plugin, add the configuration property `producer.override.partitioner.class` to the connector plugin with a value that points to a class implementing the [Partitioner](https://github.com/apache/kafka/blob/trunk/clients/src/main/java/org/apache/kafka/clients/producer/Partitioner.java) interface, e.g. `org.apache.kafka.clients.producer.internals.DefaultPartitioner`.

## Parallelization
Splitting the workload between multiple tasks is possible via the configuration property `tasks.max`. The connector splits the work based on the number of configured channels/patterns. If the max tasks configuration exceeds the number of channels/patterns, the number of channels/patterns will be used instead as the maximum.

## Configuration
### Connector Properties
| Name                              | Type    | Default        | Importance | Description                                             |
| --------------------------------- | ------- | -------------- | ---------- | ------------------------------------------------------- |
| `topic`                           | string  |                | High       | Topic to write to.                                      |
| `redis.uri`                       | string  |                | High       | Redis connection information provided via a URI string. |
| `redis.cluster.enabled`           | boolean | false          | High       | Target Redis is running as a cluster.                   |
| `redis.channels`                  | string  |                | High       | Redis channels to subscribe to separated by commas.     |
| `redis.channels.patterns.enabled` | boolean |                | High       | Redis channels use patterns (PSUBSCRIBE).               |
