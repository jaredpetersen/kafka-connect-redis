package io.github.jaredpetersen.kafkaconnectredis.source.listener.record;

public interface RedisCommand {
  enum Command {
    SADD,
    SET,
    GEOADD
  }

  Command getCommand();
  Object getPayload();
}
