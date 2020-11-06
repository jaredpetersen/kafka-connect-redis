package io.github.jaredpetersen.kafkaconnectredis.source.listener.subscriber;

import io.github.jaredpetersen.kafkaconnectredis.source.listener.RedisSubscriptionMessage;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class RedisPatternSubscriber implements RedisSubscriber {
  private final StatefulRedisPubSubConnection<String, String> redisPubSubConnection;
  private final List<String> patterns;

  private static final Logger LOG = LoggerFactory.getLogger(RedisChannelSubscriber.class);

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
  public Flux<RedisSubscriptionMessage> observe() {
    return redisPubSubConnection.reactive().observePatterns()
      .map(channelMessage -> RedisSubscriptionMessage.builder()
        .channel(channelMessage.getChannel())
        .pattern(channelMessage.getPattern())
        .message(channelMessage.getMessage())
        .build())
      .doOnNext(redisMessage -> LOG.info("received message {}", redisMessage));
  }
}
