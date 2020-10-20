package io.github.jaredpetersen.kafkaconnectredis.sink.writer;

import io.github.jaredpetersen.kafkaconnectredis.sink.writer.record.RedisCommand;
import io.github.jaredpetersen.kafkaconnectredis.sink.writer.record.RedisGeoaddCommand;
import io.github.jaredpetersen.kafkaconnectredis.sink.writer.record.RedisSaddCommand;
import io.github.jaredpetersen.kafkaconnectredis.sink.writer.record.RedisSetCommand;
import io.lettuce.core.SetArgs;
import io.lettuce.core.api.reactive.RedisReactiveCommands;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Arrays;
import java.util.List;

public class Writer {
  private final RedisReactiveCommands<String, String> redisCommands;

  public Writer(RedisReactiveCommands<String, String> redisCommands) {
    this.redisCommands = redisCommands;
  }

  public Mono<Void> write(RedisCommand redisCommand) {
    final Mono<Void> response;

    switch(redisCommand.getCommand()) {
      case SET:
        response = set((RedisSetCommand) redisCommand);
        break;
      case SADD:
        response = sadd((RedisSaddCommand) redisCommand);
        break;
      case GEOADD:
        response = geoadd((RedisGeoaddCommand) redisCommand);
        break;
      default:
        response = Mono.error(new UnsupportedOperationException("redis command " + redisCommand + " is not supported"));
    }

    return response;
  }

  private Mono<Void> set(RedisSetCommand setCommand) {
    final Mono<RedisSetCommand.Payload> payloadMono = Mono.just(setCommand.getPayload());

    final Mono<SetArgs> setArgsMono = payloadMono
      .flatMap(payload -> Mono.fromCallable(() -> {
        final SetArgs setArgs = new SetArgs();

        if (payload.getExpiration() != null) {
          final RedisSetCommand.Payload.Expiration expiration = payload.getExpiration();

          if (expiration.getType() == RedisSetCommand.Payload.Expiration.Type.EX && expiration.getTime() != null) {
            setArgs.ex(expiration.getTime());
          }
          else if (expiration.getType() == RedisSetCommand.Payload.Expiration.Type.PX && expiration.getTime() != null) {
            setArgs.px(expiration.getTime());
          }
          else if (expiration.getType() == RedisSetCommand.Payload.Expiration.Type.KEEPTTL) {
            setArgs.keepttl();
          }
        }

        if (payload.getCondition() != null) {
          if (payload.getCondition() == RedisSetCommand.Payload.Condition.NX) {
            setArgs.nx();
          }
          else if (payload.getCondition() == RedisSetCommand.Payload.Condition.XX) {
            setArgs.xx();
          }
        }

        return setArgs;
      }));

    return Mono
      .zip(payloadMono, setArgsMono)
      .flatMap(tuple -> {
        final RedisSetCommand.Payload payload = tuple.getT1();
        final SetArgs setArgs = tuple.getT2();

        return this.redisCommands.set(payload.getKey(), payload.getValue(), setArgs);
      })
      .then();
  }

  private Mono<Void> sadd(RedisSaddCommand saddCommand) {
    return Mono
      .just(saddCommand.getPayload())
      .flatMap(payload -> this.redisCommands.sadd(payload.getKey(), payload.getValues().toArray(new String[0])))
      .then();
  }

  private Mono<Void> geoadd(RedisGeoaddCommand geoaddCommand) {
    final Mono<RedisGeoaddCommand.Payload> payloadMono = Mono.just(geoaddCommand.getPayload());

    final Flux<Object> geoLocationFlux = payloadMono
      .flatMapIterable(RedisGeoaddCommand.Payload::getValues)
      .flatMapIterable(geoLocation ->
        (geoLocation.getLongitude() != null && geoLocation.getLatitude() != null && geoLocation.getMember() != null)
          ? Arrays.asList(geoLocation.getLongitude(), geoLocation.getLatitude(), geoLocation.getMember())
          : null);

    return Mono
      .zip(payloadMono, geoLocationFlux.collectList())
      .flatMap(tuple -> {
        final RedisGeoaddCommand.Payload payload = tuple.getT1();
        final List<Object> latitudeLongitudeMembers = tuple.getT2();

        return this.redisCommands.geoadd(payload.getKey(), latitudeLongitudeMembers.toArray(new Object[0]));
      })
      .then();
  }
}
