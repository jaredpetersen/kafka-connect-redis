package io.github.jaredpetersen.kafkaconnectredis.source.listener;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class RedisClusterPatternSubscriber implements RedisSubscriber {
  @Override
  public Mono<Void> subscribe() {
    return null;
  }

  @Override
  public Mono<Void> unsubscribe() {
    return null;
  }

  @Override
  public Flux<RedisSubscriptionMessage> observe() {
    return null;
  }
}
