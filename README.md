# Kafka Connect Redis
[![Build Status](https://github.com/jaredpetersen/kafka-connect-redis/workflows/Release/badge.svg)](https://github.com/jaredpetersen/kafka-connect-redis/actions)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/io.github.jaredpetersen/kafka-connect-redis/badge.svg)](https://maven-badges.herokuapp.com/maven-central/io.github.jaredpetersen/kafka-connect-redis)

Kafka Connect Sink Connector for Redis

## Usage
Kafka Connect Redis is a Kafka Connector that takes data modification Redis commands and applies them to Redis. Only sinking data is supported at this time.

Requires Redis 2.4 or higher.

A full example of how Kafka Connect Redis can be integrated into a Kafka cluster is available in the [development documentation](/docs/development/).

### Record Formats and Structures
The following record formats are supported:
- Avro (Recommended)
- JSON with Schema
- Plain JSON

The structure of the message value dictates the data modification command that is to be run against Redis. The topic name and message key are not used when sinking data into Redis. Messages without a schema are not supported.

Since the message value requires a very specific format, you may find it necessary to leverage a Kafka Streams application to transform your data.

#### Schema
```json
{
  "namespace": "io.github.jaredpetersen.kafkaconnectredis",
  "name": "RedisCommand",
  "type": "record",
  "fields": [
    {
      "field": "operation",
      "type": "enum",
      "symbols": [ "SET", "SADD", "GEOADD" ],
      "optional": false
    },
    {
      "field": "payload",
      "type": [
        {
          "name": "SET",
          "type": "record",
          "fields": [
            {
              "field": "key",
              "type": "string",
              "optional": false
            },
            {
              "field": "value",
              "type": "string",
              "optional": false
            },
            {
              "field": "expirationType",
              "type": "string",
              "optional": true
            }
          ]
        }
      ],
      "optional": false
    }
  ]
}
```

##### Commands
The following Redis commands are supported at this time:
- SET
- SADD
- GEOADD

###### SET
[Redis official command documentation](https://redis.io/commands/set)

Samples:
```json
{
  "operation": "SET",
  "payload": {
    "key": "{user.1}.username",
    "value": "jetpackmelon22"
  } 
}
```

```json
{
  "operation": "SET",
  "payload": {
    "key": "{user.2}.username",
    "value": "anchorgoat74",
    "expirationType": "EX",
    "expiration": 2100,
    "condition": "NX"
  } 
}
```

###### SADD
[Redis official command documentation](https://redis.io/commands/sadd)

Schema:
```json

```

Sample value:
```json
{
  "operation": "SADD", 
  "payload": {
    "key": "sicily",
    "values": [
    ]
  }
}
```

###### GEOADD
[Redis official command documentation](https://redis.io/commands/geoadd)

Schema:
```json

```

Sample value:
```json
{
  "operation": "GEOADD", 
  "payload": {
    "key": "sicily",
    "values": [ 
      {
        "longitude": 13.361389,
        "latitude": 38.115556,
        "member": "palermo"
      },
      {
        "longitude": 15.087269,
        "latitude": 37.502669,
        "member": "catania"
      }
    ]
  }
}
```

## Configuration
### Connector Properties
| Name                    | Description                         | Type     | Default | Importance |
| ----------------------- | ----------------------------------- | -------- | ------- | ---------- |
| `redis.uri`             | Redis URI                           | string   |         | high       |
| `redis.cluster.enabled` | Target Redis instance is a cluster  | boolean  | false   | high       |
