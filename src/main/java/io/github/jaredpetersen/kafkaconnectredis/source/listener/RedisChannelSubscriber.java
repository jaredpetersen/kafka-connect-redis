package io.github.jaredpetersen.kafkaconnectredis.source.listener;

import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

public class RedisChannelSubscriber implements RedisSubscriber {
  private final StatefulRedisPubSubConnection<String, String> redisPubSubConnection;
  private final List<String> channels;

  private static final Logger LOG = LoggerFactory.getLogger(RedisChannelSubscriber.class);

  public RedisChannelSubscriber(
      StatefulRedisPubSubConnection<String, String> redisPubSubConnection,
      List<String> channels) {
    this.redisPubSubConnection = redisPubSubConnection;
    this.channels = channels;
  }

  @Override
  public Mono<Void> subscribe() {
    return redisPubSubConnection.reactive()
      .subscribe(channels.toArray(new String[0]));
  }

  @Override
  public Mono<Void> unsubscribe() {
    return redisPubSubConnection.reactive()
      .unsubscribe(channels.toArray(new String[0]));
  }

  @Override
  public Flux<RedisSubscriptionMessage> observe() {
    return redisPubSubConnection.reactive().observePatterns()
      .map(channelMessage -> RedisSubscriptionMessage.builder()
        .channel(channelMessage.getChannel())
        .message(channelMessage.getMessage())
        .build())
      .doOnNext(redisMessage -> LOG.info("received message {}", redisMessage));
  }
}
