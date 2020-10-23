# Kafka Connect Redis
[![Build Status](https://github.com/jaredpetersen/kafka-connect-redis/workflows/Release/badge.svg)](https://github.com/jaredpetersen/kafka-connect-redis/actions)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/io.github.jaredpetersen/kafka-connect-redis/badge.svg)](https://maven-badges.herokuapp.com/maven-central/io.github.jaredpetersen/kafka-connect-redis)

Kafka Sink Connector for Redis. Source compatibility is in the works.

## Connectors
### Sink
Consumes Kafka records in a Redis command format (SET, SADD, etc.) and applies them to Redis.

For more information, see the [detailed documentation](/docs/connectors).

### Source (Future)
Subscribes to Redis keyspace notifications and writes the received messages to Kafka.

This functionality does not yet exist but is on the roadmap.

For more information, see the [detailed documentation](/docs/source).

## Demo
Try out the [demo](/docs/demo) for a step-by-step example on how to integrate Kafka Connect Redis into a Kafka cluster.

## Compatibility
Requires Redis 2.4 or higher.
