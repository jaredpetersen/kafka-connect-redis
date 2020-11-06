package io.github.jaredpetersen.kafkaconnectredis.source.listener.subscriber;

import io.github.jaredpetersen.kafkaconnectredis.source.listener.RedisSubscriptionMessage;
import io.lettuce.core.cluster.pubsub.StatefulRedisClusterPubSubConnection;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class RedisClusterPatternSubscriber implements RedisSubscriber {
  private final StatefulRedisClusterPubSubConnection<String, String> redisClusterPubSubConnection;
  private final List<String> patterns;

  private static final Logger LOG = LoggerFactory.getLogger(RedisChannelSubscriber.class);

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
  public Flux<RedisSubscriptionMessage> observe() {
    return redisClusterPubSubConnection.reactive().observePatterns()
      .map(channelMessage -> RedisSubscriptionMessage.builder()
        .channel(channelMessage.getChannel())
        .pattern(channelMessage.getPattern())
        .message(channelMessage.getMessage())
        .build())
      .doOnNext(redisMessage -> LOG.info("received message {}", redisMessage));
  }
}
