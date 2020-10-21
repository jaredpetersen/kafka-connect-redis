package io.github.jaredpetersen.kafkaconnectredis.sink.writer;

import io.github.jaredpetersen.kafkaconnectredis.sink.writer.record.RedisCommand;
import io.github.jaredpetersen.kafkaconnectredis.sink.writer.record.RedisGeoaddCommand;
import io.github.jaredpetersen.kafkaconnectredis.sink.writer.record.RedisSaddCommand;
import io.github.jaredpetersen.kafkaconnectredis.sink.writer.record.RedisSetCommand;
import io.lettuce.core.SetArgs;
import io.lettuce.core.api.reactive.RedisReactiveCommands;
import io.lettuce.core.cluster.api.reactive.RedisClusterReactiveCommands;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Arrays;

public class Writer {
  private final RedisReactiveCommands<String, String> redisStandaloneCommands;
  private final RedisClusterReactiveCommands<String, String> redisClusterCommands;
  private final boolean clusterEnabled;

  public Writer(RedisReactiveCommands<String, String> redisStandaloneCommands) {
    this.redisStandaloneCommands = redisStandaloneCommands;
    this.redisClusterCommands = null;
    this.clusterEnabled = false;
  }

  public Writer(RedisClusterReactiveCommands<String, String> redisClusterCommands) {
    this.redisStandaloneCommands = null;
    this.redisClusterCommands = redisClusterCommands;
    this.clusterEnabled = true;
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

        return (this.clusterEnabled)
          ? this.redisClusterCommands.set(payload.getKey(), payload.getValue(), setArgs)
          : this.redisStandaloneCommands.set(payload.getKey(), payload.getValue(), setArgs);
      })
      .then();
  }

  private Mono<Void> sadd(RedisSaddCommand saddCommand) {
    return Mono
      .just(saddCommand.getPayload())
      .flatMap(payload -> {
        final String[] members = payload.getValues().toArray(new String[0]);
        return (this.clusterEnabled)
            ? this.redisClusterCommands.sadd(payload.getKey(), members)
            : this.redisStandaloneCommands.sadd(payload.getKey(), members);
      })
      .then();
  }

  private Mono<Void> geoadd(RedisGeoaddCommand geoaddCommand) {
    final Flux<Object> geoLocationFlux = Flux
      .fromIterable(geoaddCommand.getPayload().getValues())
      .flatMapIterable(geoLocation ->
        (geoLocation.getLongitude() != null && geoLocation.getLatitude() != null && geoLocation.getMember() != null)
          ? Arrays.asList(geoLocation.getLongitude(), geoLocation.getLatitude(), geoLocation.getMember())
          : null);

    return Mono
      .zip(Mono.just(geoaddCommand.getPayload().getKey()), geoLocationFlux.collectList())
      .flatMap(tuple -> {
        final String key = tuple.getT1();
        final Object[] longitudeLatitudeMembers = tuple.getT2().toArray(new Object[0]);
        return (this.clusterEnabled)
          ? this.redisClusterCommands.geoadd(key, longitudeLatitudeMembers)
          : this.redisStandaloneCommands.geoadd(key, longitudeLatitudeMembers);
      })
      .then();
  }
}
