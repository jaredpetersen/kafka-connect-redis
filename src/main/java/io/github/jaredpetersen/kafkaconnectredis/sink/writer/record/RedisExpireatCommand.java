package io.github.jaredpetersen.kafkaconnectredis.sink.writer.record;

import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class RedisExpireatCommand implements RedisCommand {
  Command command = Command.EXPIREAT;
  Payload payload;

  @Value
  @lombok.Builder
  public static class Payload {
    String key;
    long timestamp;
  }
}
