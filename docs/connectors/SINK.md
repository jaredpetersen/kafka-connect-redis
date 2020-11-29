# Kafka Connect Redis - Sink
Consume messages from Kafka and apply them to Redis in the form of commands.

The following commands are supported at this time:
- [SET](https://redis.io/commands/set)
- [EXPIRE](https://redis.io/commands/expire)
- [EXPIREAT](https://redis.io/commands/expireat)
- [PEXPIRE](https://redis.io/commands/pexpire)
- [SADD](https://redis.io/commands/sadd)
- [GEOADD](https://redis.io/commands/geoadd)
- Arbitrary -- useful for Redis modules

Support for additional write-based commands will be added in the future.

## Record Schema
Records must adhere to a specific schema in order to be processed by the connector.

Each Redis command type has its own schema. This can be an issue if you are utilizing a schema registry because the default subject naming strategy assumes that topics will only have one schema. To overcome this, you must configure the subject naming strategy to be either `RecordNameStrategy` or `TopicRecordNameStrategy`. Check out the [official documentation](https://docs.confluent.io/6.0.0/schema-registry/serdes-develop/index.html#sr-schemas-subject-name-strategy) and this [Confluent blog post](https://www.confluent.io/blog/put-several-event-types-kafka-topic/) for more information.

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

#### EXPIRE
##### Avro
```json
{
    "namespace": "io.github.jaredpetersen.kafkaconnectredis",
    "name": "RedisExpireCommand",
    "type": "record",
    "fields": [
        {
            "name": "key",
            "type": "string"
        },
        {
            "name": "seconds",
            "type": "long"
        }
    ]
}
```

##### Connect JSON
```json
{
    "name": "io.github.jaredpetersen.kafkaconnectredis.RedisExpireCommand",
    "type": "struct",
    "fields": [
        {
            "field": "key",
            "type": "string",
            "optional": false
        },
        {
            "field": "seconds",
            "type": "int64",
            "optional": false
        }
    ]
}
```

#### EXPIREAT
##### Avro
```json
{
    "namespace": "io.github.jaredpetersen.kafkaconnectredis",
    "name": "RedisExpireatCommand",
    "type": "record",
    "fields": [
        {
            "name": "key",
            "type": "string"
        },
        {
            "name": "timestamp",
            "type": "long"
        }
    ]
}
```

##### Connect JSON
```json
{
    "name": "io.github.jaredpetersen.kafkaconnectredis.RedisExpireatCommand",
    "type": "struct",
    "fields": [
        {
            "field": "key",
            "type": "string",
            "optional": false
        },
        {
            "field": "timestamp",
            "type": "int64",
            "optional": false
        }
    ]
}
```

#### PEXPIRE
##### Avro
```json
{
    "namespace": "io.github.jaredpetersen.kafkaconnectredis",
    "name": "RedisPexpireCommand",
    "type": "record",
    "fields": [
        {
            "name": "key",
            "type": "string"
        },
        {
            "name": "milliseconds",
            "type": "long"
        }
    ]
}
```

##### Connect JSON
```json
{
    "name": "io.github.jaredpetersen.kafkaconnectredis.RedisPexpireCommand",
    "type": "struct",
    "fields": [
        {
            "field": "key",
            "type": "string",
            "optional": false
        },
        {
            "field": "milliseconds",
            "type": "int64",
            "optional": false
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
            "items": {
              "type": "string"
            },
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
                            "type": "string"
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

#### Arbitrary
##### Avro
```json
{
    "namespace": "io.github.jaredpetersen.kafkaconnectredis",
    "name": "RedisArbitraryCommand",
    "type": "record",
    "fields": [
        {
            "name": "command",
            "type": "string"
        },
        {
            "name": "arguments",
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
    "name": "io.github.jaredpetersen.kafkaconnectredis.RedisArbitraryCommand",
    "type": "struct",
    "fields": [
        {
            "field": "command",
            "type": "string",
            "optional": false
        },
        {
            "field": "arguments",
            "type": "array",
            "items": {
              "type": "string"
            },
            "optional": false
        }
    ]
}
```

## Configuration
### Connector Properties
| Name                    | Type    | Default | Importance | Description                                             |
| ----------------------- | ------- | ------- | ---------- | ------------------------------------------------------- |
| `redis.uri`             | string  |         | High       | Redis connection information provided via a URI string. |
| `redis.cluster.enabled` | boolean | false   | High       | Target Redis is running as a cluster.                   |