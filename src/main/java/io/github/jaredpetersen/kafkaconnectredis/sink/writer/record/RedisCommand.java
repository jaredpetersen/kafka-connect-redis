package io.github.jaredpetersen.kafkaconnectredis.sink.writer.record;

public interface RedisCommand {
  enum Command {
    SADD,
    SET,
    GEOADD
  }

  Command getCommand();

  Object getPayload();
}
