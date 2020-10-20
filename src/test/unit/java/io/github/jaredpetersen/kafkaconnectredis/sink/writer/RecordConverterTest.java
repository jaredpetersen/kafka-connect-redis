package io.github.jaredpetersen.kafkaconnectredis.sink.writer;

import io.github.jaredpetersen.kafkaconnectredis.sink.writer.record.RedisCommand;
import io.github.jaredpetersen.kafkaconnectredis.sink.writer.record.RedisSetCommand;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

public class RecordConverterTest {
  private static final Schema REDIS_SET_COMMAND_SCHEMA = SchemaBuilder.struct()
      .field("command", SchemaBuilder.STRING_SCHEMA)
      .field("payload", SchemaBuilder.struct()
          .field("key", SchemaBuilder.STRING_SCHEMA)
          .field("value", SchemaBuilder.STRING_SCHEMA)
          .field("expiration", SchemaBuilder.struct()
            .field("type", SchemaBuilder.STRING_SCHEMA)
            .field("time", SchemaBuilder.INT64_SCHEMA)
            .optional())
          .field("condition", SchemaBuilder.OPTIONAL_STRING_SCHEMA)
          .required()
          .build())
      .required();

  @Test
  public void convertTransformsPartialSinkRecordToRedisSetCommand() {
    final String topic = "rediscommands";
    final int partition = 0;
    final Schema keySchema = null;
    final Object key = null;
    final Schema valueSchema = REDIS_SET_COMMAND_SCHEMA;
    final Object value = new Struct(valueSchema)
        .put("command", "SET")
        .put("payload", new Struct(valueSchema.field("payload").schema())
          .put("key", "{user.1}.username")
          .put("value", "jetpackmelon22"));
    final long offset = 0L;
    final SinkRecord sinkRecord = new SinkRecord(topic, partition, keySchema, key, valueSchema, value, offset);

    final RedisCommand expectedRedisCommand = RedisSetCommand.builder()
        .payload(RedisSetCommand.Payload.builder()
          .key("{user.1}.username")
          .value("jetpackmelon22")
          .build())
        .build();

    final RecordConverter recordConverter = new RecordConverter();
    final Mono<RedisCommand> redisCommandMono = recordConverter.convert(sinkRecord);

    StepVerifier.create(redisCommandMono)
        .expectNext(expectedRedisCommand)
        .verifyComplete();
  }

  @Test
  public void convertTransformsSinkRecordToRedisSetCommand() {
    final String topic = "rediscommands";
    final int partition = 0;
    final Schema keySchema = null;
    final Object key = null;
    final Schema valueSchema = REDIS_SET_COMMAND_SCHEMA;
    final Schema valuePayloadSchema = valueSchema.field("payload").schema();
    final Schema valuePayloadExpirationSchema = valuePayloadSchema.field("expiration").schema();
    final Object value = new Struct(valueSchema)
        .put("command", "SET")
        .put("payload", new Struct(valuePayloadSchema)
          .put("key", "{user.2}.username")
          .put("value", "anchorgoat74")
          .put("expiration", new Struct(valuePayloadExpirationSchema)
            .put("type", "EX")
            .put("time", 2100L))
          .put("condition", "NX"));
    final long offset = 0L;
    final SinkRecord sinkRecord = new SinkRecord(topic, partition, keySchema, key, valueSchema, value, offset);

    final RedisCommand expectedRedisCommand = RedisSetCommand.builder()
        .payload(RedisSetCommand.Payload.builder()
            .key("{user.2}.username")
            .value("anchorgoat74")
            .expiration(RedisSetCommand.Payload.Expiration.builder()
              .type(RedisSetCommand.Payload.Expiration.Type.EX)
              .time(2100L)
              .build())
            .condition(RedisSetCommand.Payload.Condition.NX)
            .build())
        .build();

    final RecordConverter recordConverter = new RecordConverter();
    final Mono<RedisCommand> redisCommandMono = recordConverter.convert(sinkRecord);

    StepVerifier.create(redisCommandMono)
        .expectNext(expectedRedisCommand)
        .verifyComplete();
  }
}
