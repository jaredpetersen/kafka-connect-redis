package io.github.jaredpetersen.kafkaconnectredis.source.listener.subscriber;

import io.github.jaredpetersen.kafkaconnectredis.source.listener.RedisMessage;
import io.lettuce.core.cluster.pubsub.StatefulRedisClusterPubSubConnection;
import java.util.List;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class RedisClusterPatternSubscriber implements RedisSubscriber {
  private final StatefulRedisClusterPubSubConnection<String, String> redisClusterPubSubConnection;
  private final List<String> patterns;

  public RedisClusterPatternSubscriber(
      StatefulRedisClusterPubSubConnection<String, String> redisClusterPubSubConnection,
      List<String> patterns) {
    this.redisClusterPubSubConnection = redisClusterPubSubConnection;
    this.patterns = patterns;
  }

  @Override
  public Mono<Void> subscribe() {
    return redisClusterPubSubConnection.reactive()
      .psubscribe(patterns.toArray(new String[0]));
  }

  @Override
  public Mono<Void> unsubscribe() {
    return redisClusterPubSubConnection.reactive()
      .punsubscribe(patterns.toArray(new String[0]));
  }

  @Override
  public Flux<RedisMessage> observe() {
    return redisClusterPubSubConnection.reactive().observePatterns()
      .map(channelMessage -> RedisMessage.builder()
        .channel(channelMessage.getChannel())
        .pattern(channelMessage.getPattern())
        .message(channelMessage.getMessage())
        .build());
  }
}
