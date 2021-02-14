package io.github.jaredpetersen.kafkaconnectredis.sink.writer;

import io.github.jaredpetersen.kafkaconnectredis.sink.writer.record.RedisArbitraryCommand;
import io.github.jaredpetersen.kafkaconnectredis.sink.writer.record.RedisCommand;
import io.github.jaredpetersen.kafkaconnectredis.sink.writer.record.RedisExpireCommand;
import io.github.jaredpetersen.kafkaconnectredis.sink.writer.record.RedisExpireatCommand;
import io.github.jaredpetersen.kafkaconnectredis.sink.writer.record.RedisGeoaddCommand;
import io.github.jaredpetersen.kafkaconnectredis.sink.writer.record.RedisPexpireCommand;
import io.github.jaredpetersen.kafkaconnectredis.sink.writer.record.RedisSaddCommand;
import io.github.jaredpetersen.kafkaconnectredis.sink.writer.record.RedisSetCommand;
import io.lettuce.core.SetArgs;
import io.lettuce.core.api.reactive.RedisReactiveCommands;
import io.lettuce.core.cluster.api.reactive.RedisClusterReactiveCommands;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.output.CommandOutput;
import io.lettuce.core.output.VoidOutput;
import io.lettuce.core.protocol.CommandArgs;
import io.lettuce.core.protocol.ProtocolKeyword;
import java.nio.charset.StandardCharsets;
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
      case ARBITRARY:
        response = arbitrary((RedisArbitraryCommand) redisCommand);
        break;
      default:
        response = Mono.error(new ConnectException("redis command " + redisCommand + " is not supported"));
    }

    return response;
  }

  private Mono<Void> set(RedisSetCommand setCommand) {
    final RedisSetCommand.Payload payload = setCommand.getPayload();
    final Mono<SetArgs> setArgsMono = Mono
      .fromCallable(() -> {
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
      });
    final Mono<String> setResult = setArgsMono
      .flatMap(setArgs ->
        (this.clusterEnabled)
          ? this.redisClusterCommands.set(payload.getKey(), payload.getValue(), setArgs)
          : this.redisStandaloneCommands.set(payload.getKey(), payload.getValue(), setArgs));

    return setResult.then();
  }

  private Mono<Void> expire(RedisExpireCommand expireCommand) {
    final RedisExpireCommand.Payload payload = expireCommand.getPayload();
    final Mono<Boolean> expirationResult = (this.clusterEnabled)
      ? this.redisClusterCommands.expire(payload.getKey(), payload.getSeconds())
      : this.redisStandaloneCommands.expire(payload.getKey(), payload.getSeconds());

    return expirationResult.then();
  }

  private Mono<Void> expireat(RedisExpireatCommand expireAtCommand) {
    final RedisExpireatCommand.Payload payload = expireAtCommand.getPayload();
    final Mono<Boolean> expirationResult = (this.clusterEnabled)
      ? this.redisClusterCommands.expireat(payload.getKey(), payload.getTimestamp())
      : this.redisStandaloneCommands.expireat(payload.getKey(), payload.getTimestamp());

    return expirationResult.then();
  }

  private Mono<Void> pexpire(RedisPexpireCommand pexpireCommand) {
    final RedisPexpireCommand.Payload payload = pexpireCommand.getPayload();
    final Mono<Boolean> expirationResult = (this.clusterEnabled)
      ? this.redisClusterCommands.pexpire(payload.getKey(), payload.getMilliseconds())
      : this.redisStandaloneCommands.pexpire(payload.getKey(), payload.getMilliseconds());

    return expirationResult.then();
  }

  private Mono<Void> sadd(RedisSaddCommand saddCommand) {
    final RedisSaddCommand.Payload payload = saddCommand.getPayload();
    final String[] members = payload.getValues().toArray(new String[0]);
    final Mono<Long> saddResult = (this.clusterEnabled)
      ? this.redisClusterCommands.sadd(payload.getKey(), members)
      : this.redisStandaloneCommands.sadd(payload.getKey(), members);

    return saddResult.then();
  }

  private Mono<Void> geoadd(RedisGeoaddCommand geoaddCommand) {
    final RedisGeoaddCommand.Payload payload = geoaddCommand.getPayload();
    final Flux<Object> geoLocationFlux = Flux
      .fromIterable(payload.getValues())
      .flatMapIterable(geoLocation -> {
        if (geoLocation.getMember() == null) {
          throw new ConnectException("geoadd command does not contain member");
        }
        return Arrays.asList(geoLocation.getLongitude(), geoLocation.getLatitude(), geoLocation.getMember());
      });
    final Mono<Long> geoaddResult = geoLocationFlux
      .collectList()
      .flatMap(geoLocationList -> {
        final String key = payload.getKey();
        final Object[] longitudeLatitudeMembers = geoLocationList.toArray(new Object[0]);
        return (this.clusterEnabled)
          ? this.redisClusterCommands.geoadd(key, longitudeLatitudeMembers)
          : this.redisStandaloneCommands.geoadd(key, longitudeLatitudeMembers);
      });

    return geoaddResult.then();
  }

  private Mono<Void> arbitrary(RedisArbitraryCommand arbitraryCommand) {
    // Set up arbitrary command
    final ProtocolKeyword protocolKeyword = new ProtocolKeyword() {
      public final byte[] bytes = name().getBytes(StandardCharsets.US_ASCII);

      @Override
      public byte[] getBytes() {
        return this.bytes;
      }

      @Override
      public String name() {
        return arbitraryCommand.getPayload().getCommand();
      }
    };
    final CommandOutput<String, String, Void> commandOutput = new VoidOutput<>();
    final CommandArgs<String, String> commandArgs = new CommandArgs<>(StringCodec.UTF8)
      .addValues(arbitraryCommand.getPayload().getArguments());

    return (this.clusterEnabled)
      ? this.redisClusterCommands.dispatch(protocolKeyword, commandOutput, commandArgs).then()
      : this.redisStandaloneCommands.dispatch(protocolKeyword, commandOutput, commandArgs).then();
  }
}
