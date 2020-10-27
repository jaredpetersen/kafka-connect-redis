package io.github.jaredpetersen.kafkaconnectredis.source.listener.subscriber;

import lombok.Builder;
import lombok.Value;

@Value
@Builder(builderClassName = "Builder")
public class SubscriptionEvent<K, V> {
  K channel;
  V message;
}
