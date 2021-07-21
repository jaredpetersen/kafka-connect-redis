package io.github.jaredpetersen.kafkaconnectredis.sink.writer;

import io.github.jaredpetersen.kafkaconnectredis.sink.writer.record.RedisArbitraryCommand;
import io.github.jaredpetersen.kafkaconnectredis.sink.writer.record.RedisCommand;
import io.github.jaredpetersen.kafkaconnectredis.sink.writer.record.RedisExpireCommand;
import io.github.jaredpetersen.kafkaconnectredis.sink.writer.record.RedisExpireatCommand;
import io.github.jaredpetersen.kafkaconnectredis.sink.writer.record.RedisGeoaddCommand;
import io.github.jaredpetersen.kafkaconnectredis.sink.writer.record.RedisPexpireCommand;
import io.github.jaredpetersen.kafkaconnectredis.sink.writer.record.RedisSaddCommand;
import io.github.jaredpetersen.kafkaconnectredis.sink.writer.record.RedisSetCommand;
import java.util.ArrayList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;

@Slf4j
public class RecordConverter {
  /**
   * Convert sink record to Redis command.
   *
   * @param sinkRecord Record to convert.
   * @return Redis command.
   */
  public RedisCommand convert(SinkRecord sinkRecord) {
    LOG.debug("Converting record {}", sinkRecord);

    final Struct recordValue = (Struct) sinkRecord.value();
    final String recordValueSchemaName = recordValue.schema().name();

    final RedisCommand redisCommand;

    switch (recordValueSchemaName) {
      case "io.github.jaredpetersen.kafkaconnectredis.RedisSetCommand":
        redisCommand = convertSet(recordValue);
        break;
      case "io.github.jaredpetersen.kafkaconnectredis.RedisExpireCommand":
        redisCommand = convertExpire(recordValue);
        break;
      case "io.github.jaredpetersen.kafkaconnectredis.RedisExpireatCommand":
        redisCommand = convertExpireat(recordValue);
        break;
      case "io.github.jaredpetersen.kafkaconnectredis.RedisPexpireCommand":
        redisCommand = convertPexpire(recordValue);
        break;
      case "io.github.jaredpetersen.kafkaconnectredis.RedisSaddCommand":
        redisCommand = convertSadd(recordValue);
        break;
      case "io.github.jaredpetersen.kafkaconnectredis.RedisGeoaddCommand":
        redisCommand = convertGeoadd(recordValue);
        break;
      case "io.github.jaredpetersen.kafkaconnectredis.RedisArbitraryCommand":
        redisCommand = convertArbitrary(recordValue);
        break;
      default:
        throw new ConnectException("unsupported command schema " + recordValueSchemaName);
    }

    return redisCommand;
  }

  private RedisCommand convertSet(Struct value) {
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
  }

  private RedisCommand convertExpire(Struct value) {
    final RedisExpireCommand.Payload payload = RedisExpireCommand.Payload.builder()
      .key(value.getString("key"))
      .seconds(value.getInt64("seconds"))
      .build();

    return RedisExpireCommand.builder()
      .payload(payload)
      .build();
  }

  private RedisCommand convertExpireat(Struct value) {
    final RedisExpireatCommand.Payload payload = RedisExpireatCommand.Payload.builder()
      .key(value.getString("key"))
      .timestamp(value.getInt64("timestamp"))
      .build();

    return RedisExpireatCommand.builder()
      .payload(payload)
      .build();
  }

  private RedisCommand convertPexpire(Struct value) {
    final RedisPexpireCommand.Payload payload = RedisPexpireCommand.Payload.builder()
      .key(value.getString("key"))
      .milliseconds(value.getInt64("milliseconds"))
      .build();

    return RedisPexpireCommand.builder()
      .payload(payload)
      .build();
  }

  private RedisCommand convertSadd(Struct value) {
    final RedisSaddCommand.Payload payload = RedisSaddCommand.Payload.builder()
      .key(value.getString("key"))
      .values(value.getArray("values"))
      .build();

    return RedisSaddCommand.builder()
      .payload(payload)
      .build();
  }

  private RedisCommand convertGeoadd(Struct value) {
    final List<RedisGeoaddCommand.Payload.GeoLocation> geoLocations = new ArrayList<>();

    for (Object rawGeoLocation : value.getArray("values")) {
      final Struct rawGeolocationStruct = (Struct) rawGeoLocation;

      final RedisGeoaddCommand.Payload.GeoLocation geoLocation = RedisGeoaddCommand.Payload.GeoLocation.builder()
        .latitude(rawGeolocationStruct.getFloat64("latitude"))
        .longitude(rawGeolocationStruct.getFloat64("longitude"))
        .member(rawGeolocationStruct.getString("member"))
        .build();

      geoLocations.add(geoLocation);
    }

    final RedisGeoaddCommand.Payload payload = RedisGeoaddCommand.Payload.builder()
      .key(value.getString("key"))
      .values(geoLocations)
      .build();

    return RedisGeoaddCommand.builder()
      .payload(payload)
      .build();
  }

  private RedisCommand convertArbitrary(Struct value) {
    final RedisArbitraryCommand.Payload payload = RedisArbitraryCommand.Payload.builder()
      .command(value.getString("command"))
      .arguments(value.getArray("arguments"))
      .build();

    return RedisArbitraryCommand.builder()
      .payload(payload)
      .build();
  }
}
