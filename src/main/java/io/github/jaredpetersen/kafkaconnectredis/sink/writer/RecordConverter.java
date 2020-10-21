package io.github.jaredpetersen.kafkaconnectredis.sink.writer;

import io.github.jaredpetersen.kafkaconnectredis.sink.writer.record.RedisCommand;
import io.github.jaredpetersen.kafkaconnectredis.sink.writer.record.RedisGeoaddCommand;
import io.github.jaredpetersen.kafkaconnectredis.sink.writer.record.RedisSaddCommand;
import io.github.jaredpetersen.kafkaconnectredis.sink.writer.record.RedisSetCommand;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.util.List;

public class RecordConverter {
  public Mono<RedisCommand> convert(SinkRecord sinkRecord) {
    final Mono<Struct> valueMono = Mono.just((Struct) sinkRecord.value());
    final Mono<RedisCommand.Command> commandTypeMono = valueMono
      .flatMap(value -> Mono.just(RedisCommand.Command.valueOf(value.getString("command").toUpperCase())));

    return Mono
      .zip(commandTypeMono, valueMono)
      .flatMap(tuple -> {
        final RedisCommand.Command commandType = tuple.getT1();
        final Struct value = tuple.getT2();

        final Mono<RedisCommand> redisCommandMono;

        switch(commandType) {
          case SET:
            redisCommandMono = convertSet(value);
            break;
          case SADD:
            redisCommandMono = convertSadd(value);
            break;
          case GEOADD:
            redisCommandMono = convertGeoadd(value);
            break;
          default:
            redisCommandMono = Mono.error(new UnsupportedOperationException("redis command type does not exist"));
        }

        return redisCommandMono;
      });
  }

  private Mono<RedisCommand> convertSet(Struct value) {
    return Mono.fromCallable(() -> {
      final Struct rawPayload = value.getStruct("payload");

      final Struct rawPayloadExpiration = rawPayload.getStruct("expiration");
      final RedisSetCommand.Payload.Expiration expiration = (rawPayloadExpiration == null)
        ? null
        : RedisSetCommand.Payload.Expiration.builder()
          .type(RedisSetCommand.Payload.Expiration.Type.valueOf(rawPayloadExpiration.getString("type")))
          .time(rawPayloadExpiration.getInt64("time"))
          .build();

      final RedisSetCommand.Payload.Condition condition = (rawPayload.get("condition") == null)
        ? null
        : RedisSetCommand.Payload.Condition.valueOf((rawPayload.getString("condition")).toUpperCase());

      final RedisSetCommand.Payload payload = RedisSetCommand.Payload.builder()
        .key(rawPayload.getString("key"))
        .value(rawPayload.getString("value"))
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
      final Struct rawPayload = value.getStruct("payload");

      final RedisSaddCommand.Payload payload = RedisSaddCommand.Payload.builder()
        .key(rawPayload.getString("key"))
        .values(rawPayload.getArray("values"))
        .build();

      return RedisSaddCommand.builder()
        .payload(payload)
        .build();
    });
  }

  private Mono<RedisCommand> convertGeoadd(Struct value) {
    final Mono<Struct> rawPayloadMono = Mono.just(value.getStruct("payload"));

    final Flux<RedisGeoaddCommand.Payload.GeoLocation> geoLocationFlux = rawPayloadMono
      .flatMapIterable(rawPayload -> rawPayload.getArray("values"))
      .flatMap(rawGeolocation -> Mono.fromCallable(() -> {
        final Struct rawGeolocationStruct = (Struct) rawGeolocation;
        return RedisGeoaddCommand.Payload.GeoLocation.builder()
          .latitude(new BigDecimal(rawGeolocationStruct.getString("latitude")))
          .longitude(new BigDecimal(rawGeolocationStruct.getString("longitude")))
          .member(rawGeolocationStruct.getString("member"))
          .build();
      }));

    return Mono
      .zip(rawPayloadMono, geoLocationFlux.collectList())
      .flatMap(tuple -> Mono.fromCallable(() -> {
        final Struct rawPayload = tuple.getT1();
        final List<RedisGeoaddCommand.Payload.GeoLocation> geoLocations = tuple.getT2();

        final RedisGeoaddCommand.Payload payload = RedisGeoaddCommand.Payload.builder()
          .key(rawPayload.getString("key"))
          .values(geoLocations)
          .build();

        return RedisGeoaddCommand.builder()
          .payload(payload)
          .build();
      }));
  }
}
