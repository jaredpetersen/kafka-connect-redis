# Kafka Connect Redis
[![Build Status](https://github.com/jaredpetersen/kafka-connect-redis/workflows/Release/badge.svg)](https://github.com/jaredpetersen/kafka-connect-redis/actions)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/io.github.jaredpetersen/kafka-connect-redis/badge.svg)](https://maven-badges.herokuapp.com/maven-central/io.github.jaredpetersen/kafka-connect-redis)

Kafka Source and Sink Connector for Redis

## Connectors
### Source
Subscribes to Redis channels/patterns using the Pub/Sub feature and writes the received messages to Kafka. [Keyspace notifications](https://redis.io/topics/notifications) are supported.

For more information, see the [detailed documentation](/docs/source).

### Sink
Consumes Kafka records in a Redis command format (SET, SADD, etc.) and applies them to Redis.

For more information, see the [detailed documentation](/docs/connectors).

## Demo
Check out the [demo](/docs/demo) for a hands-on experience that shows the connector in action on a Redis and Kafka cluster!

## Compatibility
Requires Redis 2.4 or higher.
