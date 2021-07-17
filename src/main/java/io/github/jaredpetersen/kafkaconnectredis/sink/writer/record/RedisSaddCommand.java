package io.github.jaredpetersen.kafkaconnectredis.sink.writer.record;

import java.util.List;
import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class RedisSaddCommand implements RedisCommand {
  Command command = Command.SADD;
  Payload payload;

  @Value
  @lombok.Builder
  public static class Payload {
    String key;
    List<String> values;
  }
}
