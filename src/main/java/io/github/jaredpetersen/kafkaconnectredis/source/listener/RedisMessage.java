package io.github.jaredpetersen.kafkaconnectredis.source.listener;

import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class RedisMessage {
  String nodeId;
  String pattern;
  String channel;
  String message;
}
