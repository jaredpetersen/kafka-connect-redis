# Kafka Connect Redis - Source
Subscribes to Redis channels (including [keyspace notifications](https://redis.io/topics/notifications)) and writes the received messages to Kafka.

Both patterns and strict channels are supported.

## Record Schema

### Key
#### Avro
```json
{
  "type": "string",
  "name": "RedisSubscriptionEventKey",
  "version": 1
}
```

#### Connect JSON

### Value
#### Avro
```json
{

}
```

#### Connect JSON
```json
{

}
```
