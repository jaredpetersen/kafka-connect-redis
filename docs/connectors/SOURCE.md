# Kafka Connect Redis - Source
Subscribes to Redis channels (including [keyspace notifications](https://redis.io/topics/notifications)) and writes the received messages to Kafka.

Both patterns and strict channels are supported.

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
