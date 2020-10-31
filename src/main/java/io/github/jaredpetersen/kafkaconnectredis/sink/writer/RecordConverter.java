package io.github.jaredpetersen.kafkaconnectredis.sink.writer;

import io.github.jaredpetersen.kafkaconnectredis.sink.writer.record.RedisCommand;
import io.github.jaredpetersen.kafkaconnectredis.sink.writer.record.RedisGeoaddCommand;
import io.github.jaredpetersen.kafkaconnectredis.sink.writer.record.RedisSaddCommand;
import io.github.jaredpetersen.kafkaconnectredis.sink.writer.record.RedisSetCommand;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class RecordConverter {
  private static final Logger LOG = LoggerFactory.getLogger(RecordConverter.class);

  /**
   * Convert sink record to Redis command.
   *
   * @param sinkRecord Record to convert.
   * @return Redis command.
   */
  public Mono<RedisCommand> convert(SinkRecord sinkRecord) {
    LOG.debug("converting record {}", sinkRecord);

    final Struct recordValue = (Struct) sinkRecord.value();
    final String recordValueSchemaName = recordValue.schema().name();

    final Mono<RedisCommand> redisCommandMono;

    switch (recordValueSchemaName) {
      case "io.github.jaredpetersen.kafkaconnectredis.RedisSetCommand":
        redisCommandMono = convertSet(recordValue);
        break;
      case "io.github.jaredpetersen.kafkaconnectredis.RedisSaddCommand":
        redisCommandMono = convertSadd(recordValue);
        break;
      case "io.github.jaredpetersen.kafkaconnectredis.RedisGeoaddCommand":
        redisCommandMono = convertGeoadd(recordValue);
        break;
      default:
        redisCommandMono = Mono.error(new ConnectException("unsupported command schema " + recordValueSchemaName));
    }

    return redisCommandMono;
  }

  private Mono<RedisCommand> convertSet(Struct value) {
    return Mono.fromCallable(() -> {
      final Struct expirationStruct = value.getStruct("expiration");
      final RedisSetCommand.Payload.Expiration expiration = (expirationStruct == null)
        ? null
        : RedisSetCommand.Payload.Expiration.builder()
          .type(RedisSetCommand.Payload.Expiration.Type
            .valueOf(expirationStruct.getString("type")))
          .time(expirationStruct.getInt64("time"))
          .build();

      final String conditionString = value.getString("condition");
      final RedisSetCommand.Payload.Condition condition = (conditionString == null)
        ? null
        : RedisSetCommand.Payload.Condition.valueOf(conditionString.toUpperCase());

      final RedisSetCommand.Payload payload = RedisSetCommand.Payload.builder()
        .key(value.getString("key"))
        .value(value.getString("value"))
        .expiration(expiration)
        .condition(condition)
        .build();

      return RedisSetCommand.builder()
        .payload(payload)
        .build();
    });
  }

  private Mono<RedisCommand> convertSadd(Struct value) {
    return Mono.fromCallable(() -> {
      final RedisSaddCommand.Payload payload = RedisSaddCommand.Payload.builder()
        .key(value.getString("key"))
        .values(value.getArray("values"))
        .build();

      return RedisSaddCommand.builder()
        .payload(payload)
        .build();
    });
  }

  private Mono<RedisCommand> convertGeoadd(Struct value) {
    return Flux
      .fromIterable(value.getArray("values"))
      .flatMap(rawGeolocation -> Mono.fromCallable(() -> {
        final Struct rawGeolocationStruct = (Struct) rawGeolocation;
        return RedisGeoaddCommand.Payload.GeoLocation.builder()
          .latitude(Double.parseDouble(rawGeolocationStruct.getString("latitude")))
          .longitude(Double.parseDouble(rawGeolocationStruct.getString("longitude")))
          .member(rawGeolocationStruct.getString("member"))
          .build();
      }))
      .collectList()
      .flatMap(geolocations -> Mono.fromCallable(() -> {
        final RedisGeoaddCommand.Payload payload = RedisGeoaddCommand.Payload.builder()
          .key(value.getString("key"))
          .values(geolocations)
          .build();

        return RedisGeoaddCommand.builder()
          .payload(payload)
          .build();
      }));
  }
}
