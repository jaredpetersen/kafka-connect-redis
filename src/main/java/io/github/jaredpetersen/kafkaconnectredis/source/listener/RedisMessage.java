package io.github.jaredpetersen.kafkaconnectredis.source.listener;

import lombok.Builder;
import lombok.Value;

@Value
@Builder(builderClassName = "Builder")
public class RedisMessage {
  String channel;
  String pattern;
  String message;
}