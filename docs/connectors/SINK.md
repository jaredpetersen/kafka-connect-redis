# Kafka Connect Redis - Sink
Consume messages from Kafka and apply them to Redis in the form of commands.

The following commands are supported at this time:
- [SET](https://redis.io/commands/set)
- [SADD](https://redis.io/commands/sadd)
- [GEOADD](https://redis.io/commands/geoadd)

Support for additional write-based commands will be added in the future.

## Record Schema
Records must adhere to a specific schema in order to be processed by the connector.

### Key
Keys are ignored.

### Value
#### Avro
```json
{
    "type": "record",
    "name": "RedisCommandRecord",
    "namespace": "io.github.jaredpetersen.kafkaconnectredis",
    "fields": [
        {
            "name": "command",
            "type": {
                "name": "RedisCommand",
                "type": "enum",
                "symbols": [
                    "SET",
                    "SADD",
                    "GEOAD"
                ]
            }
        },
        {
            "name": "payload",
            "type": [
                {
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
                                    "name": "SetCondition",
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
                },
                {
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
                },
                {
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
                                    "name": "GeoaddGeolocation",
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
            ]
        }
    ]
}
```

#### Connect JSON
##### SET
```json
{
    "type": "struct",
    "fields": [
        {
            "field": "command",
            "type": "string",
            "optional": false
        },
        {
            "field": "payload",
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
                            "optional": false
                        }
                    ],
                    "optional": true
                },
                {
                    "field": "condition",
                    "type": "string",
                    "optional": true
                }
            ],
            "optional": false
        }
    ],
    "optional": false
}
```

##### SADD
```json
{
    "type": "struct",
    "fields": [
        {
            "field": "command",
            "type": "string",
            "optional": false
        },
        {
            "field": "payload",
            "type": "struct",
            "fields": [
                {
                    "field": "key",
                    "type": "string",
                    "optional": false
                },
                {
                    "field": "value",
                    "type": "array",
                    "items": "string",
                    "optional": false
                }
            ],
            "optional": false
        }
    ],
    "optional": false
}
```

##### GEOADD
```json
{
    "type": "struct",
    "fields": [
        {
            "field": "command",
            "type": "string",
            "optional": false
        },
        {
            "field": "payload",
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
                            },
                        ]
                    },
                    "optional": true
                }
            ],
            "optional": false
        }
    ],
    "optional": false
}
```

## Configuration
### Connector Properties
| Name                    | Description         | Type     | Default | Importance |
| ----------------------- | ------------------- | -------- | ------- | ---------- |
| `redis.uri`             | Redis URI           | string   |         | high       |
| `redis.cluster.enabled` | Enable cluster mode | boolean  | false   | high       |