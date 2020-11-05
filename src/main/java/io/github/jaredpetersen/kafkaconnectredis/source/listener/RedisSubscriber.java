package io.github.jaredpetersen.kafkaconnectredis.source.listener;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface RedisSubscriber {
  /**
   * Subscribe to Redis Pub/Sub channels.
   * @return Mono to indicate subscription is complete.
   */
  Mono<Void> subscribe();

  /**
   * Unsubscribe from Redis Pub/Sub channels.
   * @return Mono to indicate unsubscription is complete.
   */
  Mono<Void> unsubscribe();

  /**
   * Listen to subscribed Redis Pub/Sub channels and emit messages reactively.
   * @return Flux of emitted Redis subscription messages.
   */
  Flux<RedisSubscriptionMessage> observe();
}
