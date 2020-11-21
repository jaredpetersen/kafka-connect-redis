# Kafka Connect Redis
[![Build Status](https://github.com/jaredpetersen/kafka-connect-redis/workflows/Release/badge.svg)](https://github.com/jaredpetersen/kafka-connect-redis/actions)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/io.github.jaredpetersen/kafka-connect-redis/badge.svg)](https://maven-badges.herokuapp.com/maven-central/io.github.jaredpetersen/kafka-connect-redis)

Kafka Source and Sink Connector for Redis

## Connectors
### Source
Kafka Connect Redis Source subscribes to Redis channels/patterns (including [keyspace notifications](https://redis.io/topics/notifications)) using the Redis Pub/Sub and write the received messages to Kafka.

For more information, see the [detailed documentation](/docs/connectors/SOURCE.md).

### Sink
Kafka Connect Redis Sink consumes Kafka records in a Redis command format (`SET`, `GEOADD`, etc.) and applies them to Redis.

For more information, see the [detailed documentation](/docs/connectors/SINK.md).

## Demo
Check out the [demo](/docs/demo) for a hands-on experience that shows the connector in action!

This demonstration will walk you through setting up Kubernetes on your local machine, installing the connector, and using the connector to either write data into a Redis Cluster or pull data from Redis into Kafka.

## Compatibility
Requires Redis 2.6 or higher.