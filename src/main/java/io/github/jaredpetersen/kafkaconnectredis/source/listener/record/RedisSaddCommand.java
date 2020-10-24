package io.github.jaredpetersen.kafkaconnectredis.source.listener.record;

import java.util.List;
import lombok.Builder;
import lombok.Value;

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
