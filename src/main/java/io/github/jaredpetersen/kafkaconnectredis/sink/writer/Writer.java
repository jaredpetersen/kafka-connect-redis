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
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.cluster.api.sync.RedisClusterCommands;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.output.CommandOutput;
import io.lettuce.core.output.VoidOutput;
import io.lettuce.core.protocol.CommandArgs;
import io.lettuce.core.protocol.ProtocolKeyword;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Writer {
  private static final Logger LOG = LoggerFactory.getLogger(Writer.class);

  private final RedisCommands<String, String> redisStandaloneCommands;
  private final RedisClusterCommands<String, String> redisClusterCommands;
  private final boolean clusterEnabled;

  /**
   * Set up writer to interact with standalone Redis.
   *
   * @param redisStandaloneCommands Standalone Redis to write to
   */
  public Writer(RedisCommands<String, String> redisStandaloneCommands) {
    this.redisStandaloneCommands = redisStandaloneCommands;
    this.redisClusterCommands = null;
    this.clusterEnabled = false;
  }

  /**
   * Set up writer to interact with Redis cluster.
   *
   * @param redisClusterCommands Redis cluster to write to
   */
  public Writer(RedisClusterCommands<String, String> redisClusterCommands) {
    this.redisStandaloneCommands = null;
    this.redisClusterCommands = redisClusterCommands;
    this.clusterEnabled = true;
  }

  /**
   * Apply write-type command to Redis.
   *
   * @param redisCommand Command to apply
   */
  public void write(RedisCommand redisCommand) {
    LOG.debug("writing {}", redisCommand);

    switch (redisCommand.getCommand()) {
      case SET:
        set((RedisSetCommand) redisCommand);
        break;
      case EXPIRE:
        expire((RedisExpireCommand) redisCommand);
        break;
      case EXPIREAT:
        expireat((RedisExpireatCommand) redisCommand);
        break;
      case PEXPIRE:
        pexpire((RedisPexpireCommand) redisCommand);
        break;
      case SADD:
        sadd((RedisSaddCommand) redisCommand);
        break;
      case GEOADD:
        geoadd((RedisGeoaddCommand) redisCommand);
        break;
      case ARBITRARY:
        arbitrary((RedisArbitraryCommand) redisCommand);
        break;
      default:
        throw new ConnectException("redis command " + redisCommand + " is not supported");
    }
  }

  private void set(RedisSetCommand setCommand) {
    final RedisSetCommand.Payload payload = setCommand.getPayload();
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

    if (clusterEnabled) {
      redisClusterCommands.set(payload.getKey(), payload.getValue(), setArgs);
    }
    else {
      redisStandaloneCommands.set(payload.getKey(), payload.getValue(), setArgs);
    }
  }

  private void expire(RedisExpireCommand expireCommand) {
    final RedisExpireCommand.Payload payload = expireCommand.getPayload();

    if (clusterEnabled) {
      redisClusterCommands.expire(payload.getKey(), payload.getSeconds());
    }
    else {
      redisStandaloneCommands.expire(payload.getKey(), payload.getSeconds());
    }
  }

  private void expireat(RedisExpireatCommand expireAtCommand) {
    final RedisExpireatCommand.Payload payload = expireAtCommand.getPayload();

    if (clusterEnabled) {
      redisClusterCommands.expireat(payload.getKey(), payload.getTimestamp());
    }
    else {
      redisStandaloneCommands.expireat(payload.getKey(), payload.getTimestamp());
    }
  }

  private void pexpire(RedisPexpireCommand pexpireCommand) {
    final RedisPexpireCommand.Payload payload = pexpireCommand.getPayload();

    if (clusterEnabled) {
      redisClusterCommands.pexpire(payload.getKey(), payload.getMilliseconds());
    }
    else {
      redisStandaloneCommands.pexpire(payload.getKey(), payload.getMilliseconds());
    }
  }

  private void sadd(RedisSaddCommand saddCommand) {
    final RedisSaddCommand.Payload payload = saddCommand.getPayload();
    final String[] members = payload.getValues().toArray(new String[0]);

    if (clusterEnabled) {
      redisClusterCommands.sadd(payload.getKey(), members);
    }
    else {
      redisStandaloneCommands.sadd(payload.getKey(), members);
    }
  }

  private void geoadd(RedisGeoaddCommand geoaddCommand) {
    final RedisGeoaddCommand.Payload payload = geoaddCommand.getPayload();
    final String key = payload.getKey();
    final List<Object> geoLocationList = new ArrayList<>();

    for (RedisGeoaddCommand.Payload.GeoLocation geoLocation : payload.getValues()) {
      if (geoLocation.getMember() == null) {
        throw new ConnectException("geoadd command does not contain member");
      }

      geoLocationList.add(geoLocation.getLongitude());
      geoLocationList.add(geoLocation.getLatitude());
      geoLocationList.add(geoLocation.getMember());
    }

    final Object[] longitudeLatitudeMembers = geoLocationList.toArray(new Object[0]);

    if (clusterEnabled) {
      redisClusterCommands.geoadd(key, longitudeLatitudeMembers);
    }
    else {
      redisStandaloneCommands.geoadd(key, longitudeLatitudeMembers);
    }
  }

  private void arbitrary(RedisArbitraryCommand arbitraryCommand) {
    // Generate arbitrary command
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

    // Ignore output except for errors
    final CommandOutput<String, String, Void> commandOutput = new VoidOutput<>();

    // Provide arguments to the command
    final CommandArgs<String, String> commandArgs = new CommandArgs<>(StringCodec.UTF8)
      .addValues(arbitraryCommand.getPayload().getArguments());

    if (clusterEnabled) {
      redisClusterCommands.dispatch(protocolKeyword, commandOutput, commandArgs);
    }
    else {
      redisStandaloneCommands.dispatch(protocolKeyword, commandOutput, commandArgs);
    }
  }
}
