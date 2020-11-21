package io.github.jaredpetersen.kafkaconnectredis.sink.writer.record;

public interface RedisCommand {
  enum Command {
    SET,
    EXPIRE,
    EXPIREAT,
    PEXPIRE,
    SADD,
    GEOADD
  }

  Command getCommand();

  Object getPayload();
}
