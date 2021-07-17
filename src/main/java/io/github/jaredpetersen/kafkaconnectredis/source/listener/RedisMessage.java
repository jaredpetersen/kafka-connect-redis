package io.github.jaredpetersen.kafkaconnectredis.source.listener;

import lombok.Builder;
import lombok.Value;

@Value
@Builder(builderClassName = "Builder")
public class RedisMessage {
  String pattern;
  String channel;
  String message;
}
