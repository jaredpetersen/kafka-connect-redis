package io.github.jaredpetersen.kafkaconnectredis.source.listener;

import lombok.Builder;
import lombok.Value;

@Value
@Builder(builderClassName = "Builder")
public class RedisSubscriptionMessage {
  String channel;
  String pattern;
  String message;
}
