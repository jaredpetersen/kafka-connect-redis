package io.github.jaredpetersen.kafkaconnectredis.source.listener.record;

import lombok.Builder;
import lombok.Value;

import java.util.List;

@Value
@Builder(builderClassName = "Builder")
public class RedisSaddCommand implements RedisCommand {
  Command command = Command.SADD;
  Payload payload;

  @Value
  @lombok.Builder(builderClassName = "Builder")
  public static class Payload {
    String key;
    List<String> values;
  }
}
