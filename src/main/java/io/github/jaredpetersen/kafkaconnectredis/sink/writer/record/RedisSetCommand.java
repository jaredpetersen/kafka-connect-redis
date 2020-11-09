package io.github.jaredpetersen.kafkaconnectredis.sink.writer.record;

import lombok.Builder;
import lombok.Value;

@Value
@Builder(builderClassName = "Builder")
public class RedisSetCommand implements RedisCommand {
  Command command = Command.SET;
  Payload payload;

  @Value
  @lombok.Builder(builderClassName = "Builder")
  public static class Payload {
    public enum Condition { NX, XX }

    String key;
    String value;
    Expiration expiration;
    Condition condition;

    @Value
    @lombok.Builder(builderClassName = "Builder")
    public static class Expiration {
      public enum Type { EX, PX, KEEPTTL }

      Type type;
      long time;
    }
  }
}
