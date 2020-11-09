package io.github.jaredpetersen.kafkaconnectredis.sink.writer;

import io.github.jaredpetersen.kafkaconnectredis.sink.writer.record.RedisCommand;
import io.github.jaredpetersen.kafkaconnectredis.sink.writer.record.RedisGeoaddCommand;
import io.github.jaredpetersen.kafkaconnectredis.sink.writer.record.RedisSaddCommand;
import io.github.jaredpetersen.kafkaconnectredis.sink.writer.record.RedisSetCommand;
import java.util.Arrays;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

public class RecordConverterTest {
  private static final Schema REDIS_SET_COMMAND_SCHEMA = SchemaBuilder.struct()
    .name("io.github.jaredpetersen.kafkaconnectredis.RedisSetCommand")
    .field("key", SchemaBuilder.STRING_SCHEMA)
    .field("value", SchemaBuilder.STRING_SCHEMA)
    .field("expiration", SchemaBuilder.struct()
      .field("type", SchemaBuilder.STRING_SCHEMA)
      .field("time", SchemaBuilder.INT64_SCHEMA)
      .optional())
    .field("condition", SchemaBuilder.OPTIONAL_STRING_SCHEMA)
    .required()
    .build();
  private static final Schema REDIS_SADD_COMMAND_SCHEMA = SchemaBuilder.struct()
    .name("io.github.jaredpetersen.kafkaconnectredis.RedisSaddCommand")
    .field("key", SchemaBuilder.STRING_SCHEMA)
    .field("values", SchemaBuilder.array(SchemaBuilder.STRING_SCHEMA).required().build())
    .required()
    .build();
  private static final Schema REDIS_GEOADD_COMMAND_SCHEMA = SchemaBuilder.struct()
    .name("io.github.jaredpetersen.kafkaconnectredis.RedisGeoaddCommand")
    .field("key", SchemaBuilder.STRING_SCHEMA)
    .field("values", SchemaBuilder
      .array(SchemaBuilder.struct()
        .field("longitude", SchemaBuilder.FLOAT64_SCHEMA)
        .field("latitude", SchemaBuilder.FLOAT64_SCHEMA)
        .field("member", SchemaBuilder.STRING_SCHEMA)
        .required()
        .build())
      .required()
      .build())
    .required()
    .build();

  @Test
  public void convertTransformsPartialSinkRecordToRedisSetCommand() {
    final String topic = "rediscommands";
    final int partition = 0;
    final Schema keySchema = null;
    final Object key = null;
    final Schema valueSchema = REDIS_SET_COMMAND_SCHEMA;
    final Object value = new Struct(valueSchema)
      .put("key", "{user.1}.username")
      .put("value", "jetpackmelon22");
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
    final Object value = new Struct(valueSchema)
      .put("key", "{user.2}.username")
      .put("value", "anchorgoat74")
      .put("expiration", new Struct(valueSchema.field("expiration").schema())
        .put("type", "EX")
        .put("time", 2100L))
      .put("condition", "NX");
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

  @Test
  public void convertTransformsSinkRecordToRedisSaddCommand() {
    final String topic = "rediscommands";
    final int partition = 0;
    final Schema keySchema = null;
    final Object key = null;
    final Schema valueSchema = REDIS_SADD_COMMAND_SCHEMA;
    final Object value = new Struct(valueSchema)
      .put("key", "boats")
      .put("values", Arrays.asList("fishing", "sport", "tug"));
    final long offset = 0L;
    final SinkRecord sinkRecord = new SinkRecord(topic, partition, keySchema, key, valueSchema, value, offset);

    final RedisCommand expectedRedisCommand = RedisSaddCommand.builder()
      .payload(RedisSaddCommand.Payload.builder()
        .key("boats")
        .values(Arrays.asList("fishing", "sport", "tug"))
        .build())
      .build();

    final RecordConverter recordConverter = new RecordConverter();
    final Mono<RedisCommand> redisCommandMono = recordConverter.convert(sinkRecord);

    StepVerifier.create(redisCommandMono)
        .expectNext(expectedRedisCommand)
        .verifyComplete();
  }

  @Test
  public void convertTransformsSinkRecordToRedisGeoaddCommand() {
    final String topic = "rediscommands";
    final int partition = 0;
    final Schema keySchema = null;
    final Object key = null;
    final Schema valueSchema = REDIS_GEOADD_COMMAND_SCHEMA;
    final Schema valueLocationSchema = valueSchema.field("values").schema().valueSchema();
    final Object value = new Struct(valueSchema)
      .put("key", "sicily")
      .put("values", Arrays.asList(
        new Struct(valueLocationSchema)
          .put("longitude", 13.361389d)
          .put("latitude", 38.115556d)
          .put("member", "Palermo"),
        new Struct(valueLocationSchema)
          .put("longitude", 15.087269d)
          .put("latitude", 37.502669d)
          .put("member", "Catania")));
    final long offset = 0L;
    final SinkRecord sinkRecord = new SinkRecord(topic, partition, keySchema, key, valueSchema, value, offset);

    final RedisCommand expectedRedisCommand = RedisGeoaddCommand.builder()
      .payload(RedisGeoaddCommand.Payload.builder()
        .key("sicily")
        .values(Arrays.asList(
          RedisGeoaddCommand.Payload.GeoLocation.builder()
            .longitude(13.361389d)
            .latitude(38.115556d)
            .member("Palermo")
            .build(),
          RedisGeoaddCommand.Payload.GeoLocation.builder()
            .longitude(15.087269d)
            .latitude(37.502669d)
            .member("Catania")
            .build()))
        .build())
      .build();

    final RecordConverter recordConverter = new RecordConverter();
    final Mono<RedisCommand> redisCommandMono = recordConverter.convert(sinkRecord);

    StepVerifier.create(redisCommandMono)
      .expectNext(expectedRedisCommand)
      .verifyComplete();
  }
}
