# Kafka Connect Redis - Source
Subscribes to Redis channels (including [keyspace notifications](https://redis.io/topics/notifications)) and writes the received messages to Kafka.

Both channels (`SUBSCRIBE`) and patterns (`PSUBSCRIBE`) are supported.

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
Records are produced to a single topic with a single partition by default. If you would like to have records partitioned, you must use a custom [Single Message Transform (SMT)](https://docs.confluent.io/current/connect/transforms/index.html).

## Configuration
### Connector Properties
| Name                              | Description                                       | Type    | Default | Importance |
| --------------------------------- | ------------------------------------------------- | ------- | ------- | ---------- |
| `redis.uri`                       | Redis URI                                         | string  |         | high       |
| `redis.cluster.enabled`           | Enable cluster mode                               | boolean | false   | high       |
| `redis.channels`                  | Redis channel(s) to subscribe to, comma-separated | string  |         | high       |
| `redis.channels.patterns.enabled` | Redis channels utilize patterns                   | boolean | false   | high       |
