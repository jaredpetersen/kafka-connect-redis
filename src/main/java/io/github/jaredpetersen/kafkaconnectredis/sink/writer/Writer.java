package io.github.jaredpetersen.kafkaconnectredis.sink.writer;

import io.github.jaredpetersen.kafkaconnectredis.sink.writer.record.RedisCommand;
import io.github.jaredpetersen.kafkaconnectredis.sink.writer.record.RedisExpireatCommand;
import io.github.jaredpetersen.kafkaconnectredis.sink.writer.record.RedisExpireCommand;
import io.github.jaredpetersen.kafkaconnectredis.sink.writer.record.RedisGeoaddCommand;
import io.github.jaredpetersen.kafkaconnectredis.sink.writer.record.RedisPexpireCommand;
import io.github.jaredpetersen.kafkaconnectredis.sink.writer.record.RedisSaddCommand;
import io.github.jaredpetersen.kafkaconnectredis.sink.writer.record.RedisSetCommand;
import io.lettuce.core.SetArgs;
import io.lettuce.core.api.reactive.RedisReactiveCommands;
import io.lettuce.core.cluster.api.reactive.RedisClusterReactiveCommands;
import java.util.Arrays;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class Writer {
  private final RedisReactiveCommands<String, String> redisStandaloneCommands;
  private final RedisClusterReactiveCommands<String, String> redisClusterCommands;
  private final boolean clusterEnabled;

  private static final Logger LOG = LoggerFactory.getLogger(Writer.class);

  /**
   * Set up writer to interact with standalone Redis.
   *
   * @param redisStandaloneCommands Standalone Redis to write to.
   */
  public Writer(RedisReactiveCommands<String, String> redisStandaloneCommands) {
    this.redisStandaloneCommands = redisStandaloneCommands;
    this.redisClusterCommands = null;
    this.clusterEnabled = false;
  }

  /**
   * Set up writer to interact with Redis cluster.
   *
   * @param redisClusterCommands Redis cluster to write to.
   */
  public Writer(RedisClusterReactiveCommands<String, String> redisClusterCommands) {
    this.redisStandaloneCommands = null;
    this.redisClusterCommands = redisClusterCommands;
    this.clusterEnabled = true;
  }

  /**
   * Apply write-type command to Redis.
   *
   * @param redisCommand Command to apply.
   * @return Mono used to indicate the write has completed.
   */
  public Mono<Void> write(RedisCommand redisCommand) {
    LOG.debug("writing {}", redisCommand);

    final Mono<Void> response;

    switch (redisCommand.getCommand()) {
      case SET:
        response = set((RedisSetCommand) redisCommand);
        break;
      case EXPIRE:
        response = expire((RedisExpireCommand) redisCommand);
        break;
      case EXPIREAT:
        response = expireat((RedisExpireatCommand) redisCommand);
        break;
      case PEXPIRE:
        response = pexpire((RedisPexpireCommand) redisCommand);
        break;
      case SADD:
        response = sadd((RedisSaddCommand) redisCommand);
        break;
      case GEOADD:
        response = geoadd((RedisGeoaddCommand) redisCommand);
        break;
      default:
        response = Mono.error(new ConnectException("redis command " + redisCommand + " is not supported"));
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

          if (expiration.getType() == RedisSetCommand.Payload.Expiration.Type.EX) {
            setArgs.ex(expiration.getTime());
          }
          else if (expiration.getType() == RedisSetCommand.Payload.Expiration.Type.PX) {
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

  private Mono<Void> expire(RedisExpireCommand expireCommand) {
    return Mono
      .just(expireCommand.getPayload())
      .flatMap(payload ->
        (this.clusterEnabled)
          ? this.redisClusterCommands.expire(payload.getKey(), payload.getSeconds())
          : this.redisStandaloneCommands.expire(payload.getKey(), payload.getSeconds()))
      .then();
  }

  private Mono<Void> expireat(RedisExpireatCommand expireAtCommand) {
    return Mono
      .just(expireAtCommand.getPayload())
      .flatMap(payload ->
        (this.clusterEnabled)
          ? this.redisClusterCommands.expireat(payload.getKey(), payload.getTimestamp())
          : this.redisStandaloneCommands.expireat(payload.getKey(), payload.getTimestamp()))
      .then();
  }

  private Mono<Void> pexpire(RedisPexpireCommand pexpireCommand) {
    return Mono
      .just(pexpireCommand.getPayload())
      .flatMap(payload ->
        (this.clusterEnabled)
          ? this.redisClusterCommands.pexpire(payload.getKey(), payload.getMilliseconds())
          : this.redisStandaloneCommands.pexpire(payload.getKey(), payload.getMilliseconds()))
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
      .flatMapIterable(geoLocation -> {
        if (geoLocation.getMember() == null) {
          throw new ConnectException("geoadd command does not contain member");
        }
        return Arrays.asList(geoLocation.getLongitude(), geoLocation.getLatitude(), geoLocation.getMember());
      });

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
