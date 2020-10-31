# Kafka Connect Redis - Sink
Consume messages from Kafka and apply them to Redis in the form of commands.

The following commands are supported at this time:
- [SET](https://redis.io/commands/set)
- [SADD](https://redis.io/commands/sadd)
- [GEOADD](https://redis.io/commands/geoadd)

Support for additional write-based commands will be added in the future.

## Record Schema
Records must adhere to a specific schema in order to be processed by the connector.

If you are utilizing a schema registry, you must configure the sink topic to be able to utilize multiple schemas. For Confluent's schema registry, you must [configure the subject](https://docs.confluent.io/current/schema-registry/schema-validation.html#change-the-subject-naming-strategy-for-a-topic) to use either `RecordNameStrategy` or `TopicRecordNameStrategy` strategies.

### Key
Keys are ignored.

### Value
#### SET
##### Avro
```json
{
    "namespace": "io.github.jaredpetersen.kafkaconnectredis",
    "name": "RedisSetCommand",
    "type": "record",
    "fields": [
        {
            "name": "key",
            "type": "string"
        },
        {
            "name": "value",
            "type": "string"
        },
        {
            "name": "expiration",
            "type": [
                "null",
                {
                    "name": "RedisSetCommandExpiration",
                    "type": "record",
                    "fields": [
                        {
                            "name": "type",
                            "type": {
                                "name": "RedisSetCommandExpirationType",
                                "type": "enum",
                                "symbols": [
                                    "EX",
                                    "PX",
                                    "KEEPTTL"
                                ]
                            }
                        },
                        {
                            "name": "time",
                            "type": [
                                "null",
                                "long"
                            ]
                        }
                    ]
                }
            ],
            "default": null
        },
        {
            "name": "condition",
            "type": [
                "null",
                {
                    "name": "RedisSetCommandCondition",
                    "type": "enum",
                    "symbols": [
                        "NX",
                        "XX",
                        "KEEPTTL"
                    ]
                }
            ],
            "default": null
        }
    ]
}
```

##### Connect JSON
```json
{
    "name": "io.github.jaredpetersen.kafkaconnectredis.RedisSetCommand",
    "type": "struct",
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
            "field": "expiration",
            "type": "struct",
            "fields": [
                {
                    "field": "type",
                    "type": "string",
                    "optional": false
                },
                {
                    "field": "time",
                    "type": "int64",
                    "optional": true
                }
            ],
            "optional": true
        },
        {
            "field": "condition",
            "type": "string",
            "optional": true
        }
    ]
}
```

#### SADD
##### Avro
```json
{
    "namespace": "io.github.jaredpetersen.kafkaconnectredis",
    "name": "RedisSaddCommand",
    "type": "record",
    "fields": [
        {
            "name": "key",
            "type": "string"
        },
        {
            "name": "values",
            "type": {
                "type": "array",
                "items": "string"
            }
        }
    ]
}
```

##### Connect JSON
```json
{
    "name": "io.github.jaredpetersen.kafkaconnectredis.RedisSaddCommand",
    "type": "struct",
    "fields": [
        {
            "field": "key",
            "type": "string",
            "optional": false
        },
        {
            "field": "values",
            "type": "array",
            "items": "string",
            "optional": false
        }
    ]
}
```

#### GEOADD
##### Avro
```json
{
    "namespace": "io.github.jaredpetersen.kafkaconnectredis",
    "name": "RedisGeoaddCommand",
    "type": "record",
    "fields": [
        {
            "name": "key",
            "type": "string"
        },
        {
            "name": "values",
            "type": {
                "type": "array",
                "items": {
                    "name": "RedisGeoaddCommandGeolocation",
                    "type": "record",
                    "fields": [
                        {
                            "name": "longitude",
                            "type": "double"
                        },
                        {
                            "name": "latitude",
                            "type": "double"
                        },
                        {
                            "name": "member",
                            "type": "double"
                        }
                    ]
                }
            }
        }
    ]
}
```

##### Connect JSON
```json
{
    "name": "io.github.jaredpetersen.kafkaconnectredis.RedisGeoaddCommand",
    "type": "struct",
    "fields": [
        {
            "field": "key",
            "type": "string",
            "optional": false
        },
        {
            "field": "values",
            "type": "array",
            "items": {
                "type": "struct",
                "fields": [
                    {
                        "field": "longitude",
                        "type": "double",
                        "optional": false
                    },
                    {
                        "field": "latitude",
                        "type": "double",
                        "optional": false
                    },
                    {
                        "field": "member",
                        "type": "string",
                        "optional": false
                    }
                ]
            },
            "optional": false
        }
    ]
}
```

## Configuration
### Connector Properties
| Name                    | Description         | Type     | Default | Importance |
| ----------------------- | ------------------- | -------- | ------- | ---------- |
| `redis.uri`             | Redis URI           | string   |         | high       |
| `redis.cluster.enabled` | Enable cluster mode | boolean  | false   | high       |