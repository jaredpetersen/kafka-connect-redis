package io.github.jaredpetersen.kafkaconnectredis.source.listener.subscriber;

import io.github.jaredpetersen.kafkaconnectredis.source.listener.RedisMessage;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import java.util.List;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class RedisPatternSubscriber implements RedisSubscriber {
  private final StatefulRedisPubSubConnection<String, String> redisPubSubConnection;
  private final List<String> patterns;

  public RedisPatternSubscriber(
      StatefulRedisPubSubConnection<String, String> redisPubSubConnection,
      List<String> patterns) {
    this.redisPubSubConnection = redisPubSubConnection;
    this.patterns = patterns;
  }

  @Override
  public Mono<Void> subscribe() {
    return redisPubSubConnection.reactive()
      .psubscribe(patterns.toArray(new String[0]));
  }

  @Override
  public Mono<Void> unsubscribe() {
    return redisPubSubConnection.reactive()
      .punsubscribe(patterns.toArray(new String[0]));
  }

  @Override
  public Flux<RedisMessage> observe() {
    return redisPubSubConnection.reactive().observePatterns()
      .map(channelMessage -> RedisMessage.builder()
        .channel(channelMessage.getChannel())
        .pattern(channelMessage.getPattern())
        .message(channelMessage.getMessage())
        .build());
  }
}
