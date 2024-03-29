package io.github.jaredpetersen.kafkaconnectredis.sink.writer.record;

import java.util.List;
import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class RedisArbitraryCommand implements RedisCommand {
  Command command = Command.ARBITRARY;
  Payload payload;

  @Value
  @lombok.Builder
  public static class Payload {
    String command;
    List<String> arguments;
  }
}
